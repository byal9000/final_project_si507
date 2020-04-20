#################################
# Name: Chris Plotts
##### Uniqname: cplotts
#################################

import requests
import secrets
import sqlite3
import datetime
import json
import sys

NYT_COVID19_BASE = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/'
NYT_INT_LIST = ['cases', 'deaths']
CENSUS_POP_BASE = 'https://api.census.gov/data/2019/pep/population'
CENSUS_INT_LIST = 'pop'
CENSUS_FLOAT_LIST = 'density'
PROJECT_DATABASE_NAME = 'project_data.sqlite'


def get_table_timings(connection, cursor):
    timings_dict = dict()
    final_timings_dict = dict()
    cmd = "SELECT name FROM sqlite_master WHERE [Type]='table';"
    tables = [x[0] for x in cursor.execute(cmd).fetchall()]

    if 'timings' not in tables:
        cmd = ("CREATE TABLE timings" +
               "(tablename TEXT PRIMARY KEY, timestamp TEXT NOT NULL);")
        cursor.execute(cmd)
        connection.commit()
        tables.append('timings')
        # We're appending an empty timings at the end of this block.
        # This will have the effect of clearing out all other tables
        # but it shouldn't really be possible to have no timings
        # here unless someone manually goes and deletes it between
        # runs of the program.

    cmd = "SELECT * FROM timings;"
    timings = cursor.execute(cmd).fetchall()
    for row in timings:
        timings_dict[row[0]] = row[1]

    for table in tables:  # Clean out tables that aren't in timings.
        if (table not in timings_dict.keys()
                and table not in {'timings', 'sqlite_sequence'}):
            cmd = "DROP TABLE " + table + ";"
            cursor.execute(cmd)
            connection.commit()

    # Fetch updated tables list.
    cmd = "SELECT name FROM sqlite_master WHERE [Type]='table';"
    tables = [x[0] for x in cursor.execute(cmd).fetchall()]

    for row in timings:  # Clean out timings that don't have tables.
        if row[0] not in tables:
            cmd = ("DELETE FROM timings WHERE " +
                   "tablename=? and timestamp=?;")
            cursor.execute(cmd, tuple(row))
            connection.commit()
        else:
            final_timings_dict[row[0]
                               ] = datetime.datetime.fromisoformat(row[1])

    return final_timings_dict


def get_covid_data(connection, cursor, timings, table, covid_url, parameters=None):
    refresh_time = datetime.datetime.now() - datetime.timedelta(hours=3)

    if table not in timings.keys():
        load_covid_data(connection, cursor, table, covid_url)
        timings = get_table_timings(connection, cursor)

    elif timings[table] < refresh_time:  # Clear out table if 3+ hours old.
        cmd = "DROP TABLE " + table + ";"
        cursor.execute(cmd)
        connection.commit()
        load_covid_data(connection, cursor, table, covid_url)
        timings = get_table_timings(connection, cursor)

    cmd = "SELECT * FROM " + table + ";"
    data = cursor.execute(cmd).fetchall()

    return timings, data


def load_covid_data(connection, cursor, table, covid_url):
    try:
        returned_request = requests.get(covid_url)
    except:
        print('Exception in load_csv_data, trying again...')
        returned_request = requests.get(covid_url)

    data_lines = returned_request.text.splitlines()
    data_header = data_lines.pop(0).split(',')

    cmd = ("CREATE TABLE " + table +
           " ('ID' INTEGER PRIMARY KEY AUTOINCREMENT, ")
    cmd_list = list()
    for field in data_header:
        if field in NYT_INT_LIST:
            cmd_list.append(field + ' INTEGER NOT NULL')
        else:
            cmd_list.append(field + ' TEXT NOT NULL')
    cmd += ','.join(cmd_list) + ");"

    cursor.execute(cmd)
    connection.commit()

    table_data = list()
    for line in data_lines:
        table_data.append(tuple(line.split(',')))
    cmd = ("INSERT INTO " + table +
           " VALUES (NULL," + ','.join('?'*len(data_header)) + ");")
    cursor.executemany(cmd, table_data)
    connection.commit()

    cmd = "INSERT INTO timings VALUES (?,?);"
    cursor.execute(cmd, (table, datetime.datetime.now().isoformat()))
    connection.commit()

    return


def make_census_call(base, get_params, for_params):
    request_params = {'get': get_params,
                      'for': for_params,
                      'key': secrets.API_KEY}
    return requests.get(base, request_params).text


def load_census_data(connection, cursor, table, return_data):
    data = json.loads(return_data)
    data_header = [x.lower() for x in data.pop(0)]

    cmd = ("CREATE TABLE " + table +
           " ('ID' INTEGER PRIMARY KEY AUTOINCREMENT, ")
    cmd_list = list()
    for field in data_header:
        if field in CENSUS_INT_LIST:
            cmd_list.append(field + ' INTEGER NOT NULL')
        elif field in CENSUS_FLOAT_LIST:
            cmd_list.append(field + ' REAL') #These are sometimes null.
        else:
            cmd_list.append(field + ' TEXT NOT NULL')
    cmd += ','.join(cmd_list) + ");"

    # print(data_header)

    cursor.execute(cmd)
    connection.commit()

    table_data = list()
    for row in data:
        table_data.append(tuple(row))
    cmd = ("INSERT INTO " + table +
           " VALUES (NULL," + ','.join('?'*len(data_header)) + ");")
    cursor.executemany(cmd, table_data)
    connection.commit()

    cmd = "INSERT INTO timings VALUES (?,?);"
    # test = tuple([table, 'now'])
    # print(test)
    cursor.execute(cmd, (table, datetime.datetime.now().isoformat()))
    connection.commit()

    return


def get_census_data(connection, cursor, timings, table, census_url, get_params, for_params):
    if table not in timings.keys():
        return_text = make_census_call(CENSUS_POP_BASE, get_params, for_params)
        load_census_data(connection, cursor, table, return_text)
        timings = get_table_timings(connection, cursor)

    cmd = "SELECT * FROM " + table + ";"
    data = cursor.execute(cmd).fetchall()

    return timings, data


def main_loop():
    conn = sqlite3.connect(PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    table_timings = get_table_timings(conn, cur)
    table_timings, covid_us = get_covid_data(conn, cur, table_timings, 'us',
                                             NYT_COVID19_BASE + 'us.csv')
    table_timings, covid_state = get_covid_data(conn, cur, table_timings, 'state',
                                                NYT_COVID19_BASE + 'us-states.csv')
    table_timings, covid_county = get_covid_data(conn, cur, table_timings, 'county',
                                                 NYT_COVID19_BASE + 'us-counties.csv')

    table_timings, census_us = get_census_data(conn, cur, table_timings, 'us_census',
                                               CENSUS_POP_BASE, 'POP,DENSITY', 'us:*')
    table_timings, census_state = get_census_data(conn, cur, table_timings, 'state_census',
                                                  CENSUS_POP_BASE, 'POP,DENSITY,NAME', 'state:*')
    table_timings, census_county = get_census_data(conn, cur, table_timings, 'county_census',
                                                   CENSUS_POP_BASE, 'POP,DENSITY,NAME', 'county:*')

    conn.close()                                                   
    return


if __name__ == "__main__":
    main_loop()

    print('Exiting...')
