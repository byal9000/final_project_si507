import sqlite3
import plotly.graph_objs as go
import data_setup
from flask import Flask, render_template, request, redirect, url_for


TABLE_ORDER = ['date', 'cases', 'deaths', 'pop', 'density']
TABLE_NAMES = ['Date', 'Positive Tests',
               'Deaths', 'Population', 'Population Density']


app = Flask(__name__)


@app.route('/')
@app.route('/index')
@app.route('/index.html')
def index():
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = 'SELECT DISTINCT fips FROM state ORDER BY fips;'
    states = cur.execute(query).fetchall()
    conn.close()

    return render_template('index.html',
                           states=[x['fips'] for x in states],
                           state_dict=state_dict)


# US SECTION
@app.route('/us', methods=['GET'])
@app.route('/us.html', methods=['GET'])
def us():
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = 'SELECT DISTINCT date FROM us ORDER BY date;'
    dates = cur.execute(query).fetchall()
    conn.close()
    return render_template('us.html', dates=[x['date'] for x in dates])


@app.route('/us/date_results', methods=['POST'])
def us_date_results():
    return redirect('/us/us_date_'+request.form['date'])


@app.route('/us/us_date_<date>', methods=['GET'])
def us_date(date):
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = 'SELECT * FROM us INNER JOIN us_census WHERE date=?;'
    date_data = cur.execute(query, tuple([date])).fetchall()
    conn.close()

    return render_template('us_date.html',
                           order=TABLE_ORDER,
                           names=TABLE_NAMES,
                           date_data=date_data)


@app.route('/us/graph_results', methods=['POST'])
def us_graph_results():
    return redirect('/us/us_graph_'+request.form['graph'])


@app.route('/us/us_graph_<graph>', methods=['GET'])
def us_graph(graph):
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    fig = go.Figure()
    if graph == 'both':
        query = 'SELECT date,cases,deaths FROM us;'
        graph_data = cur.execute(query).fetchall()
        x_data = [x['date'] for x in graph_data]
        y1_data = [x['cases'] for x in graph_data]
        y2_data = [x['deaths'] for x in graph_data]
        fig.add_trace(go.Scatter(x=x_data, y=y1_data, name='Cases'))
        fig.add_trace(go.Scatter(x=x_data, y=y2_data, name='Deaths'))
        fig.update_layout(yaxis_title='Number of Cases/Deaths')
    elif graph == 'cases':
        query = 'SELECT date,cases FROM us;'
        graph_data = cur.execute(query).fetchall()
        x_data = [x['date'] for x in graph_data]
        y_data = [x['cases'] for x in graph_data]
        fig.add_trace(go.Scatter(x=x_data, y=y_data, name='Cases'))
        fig.update_layout(yaxis_title='Number of Cases')
    elif graph == 'deaths':
        query = 'SELECT date,deaths FROM us;'
        graph_data = cur.execute(query).fetchall()
        x_data = [x['date'] for x in graph_data]
        y_data = [x['deaths'] for x in graph_data]
        fig.add_trace(go.Scatter(x=x_data, y=y_data, name='Deaths'))
        fig.update_layout(yaxis_title='Number of Deaths')
    else:
        return render_template('us_graph.html',
                               graph_div='')
    conn.close()

    fig.update_layout(xaxis_title='Date')

    return render_template('us_graph.html',
                           graph_div=fig.to_html(full_html=False))


# STATE SECTION
@app.route('/state', methods=['GET'])
@app.route('/state.html', methods=['GET'])
def state():
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = 'SELECT DISTINCT fips FROM state ORDER BY fips;'
    states = cur.execute(query).fetchall()

    query = 'SELECT DISTINCT date FROM state ORDER BY date;'
    dates = cur.execute(query).fetchall()
    conn.close()

    return render_template('state.html',
                           states=[x['fips'] for x in states],
                           state_dict=state_dict,
                           dates=[x['date'] for x in dates])


@app.route('/state/date_results', methods=['POST'])
def state_date_results():
    return redirect('/state/state_date/' + request.form['state']
                    + '/' + request.form['date'])


@app.route('/state/state_date/<state>/<date>', methods=['GET'])
def state_date(state, date):
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = ('SELECT * FROM state INNER JOIN state_census'
             + ' as Cen ON fips=Cen.state WHERE fips=? ORDER BY date;')
    query_data = cur.execute(query, tuple([state])).fetchall()
    dates = [x['date'] for x in query_data]
    if date not in dates:
        if date < dates[0]:
            return redirect('/state/state_date/' + state
                            + '/' + dates[0])
        else:
            return redirect('/state/state_date/' + state
                            + '/' + dates[-1])
    conn.close()

    fig = go.Figure()
    y1_data = [x['cases'] for x in query_data]
    y2_data = [x['deaths'] for x in query_data]
    fig.add_trace(go.Scatter(x=dates, y=y1_data, name='Cases'))
    fig.add_trace(go.Scatter(x=dates, y=y2_data, name='Deaths'))
    fig.update_layout(xaxis_title='Date')
    fig.update_layout(yaxis_title='Number of Cases/Deaths')
    fig.update_layout(annotations=[
        dict(x=date, y=0, xref='x', yref='y', text='Selected Date',
             showarrow=True, arrowhead=7, ax=0, ay=-10)
    ])

    date_data = None
    for row in query_data:
        if row['date'] == date:
            date_data = row

    return render_template('state_detail.html',
                           graph_div=fig.to_html(full_html=False),
                           order=TABLE_ORDER,
                           names=TABLE_NAMES,
                           state=state_dict[state],
                           date_data=date_data)


@app.route('/state/graph_results', methods=['POST'])
def state_graph_results():
    compare = request.form['comparison'].replace(' ', '_')
    return redirect('/state/state_compare/' + compare
                    + '/' + request.form['yaxis'])


@app.route('/state/state_compare/<comparison>/<yaxis>', methods=['GET'])
def state_graph(comparison, yaxis):
    compare = comparison.replace('_', ' ')
    compare_var = compare.split(' ')[0]
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    fig = go.Figure()
    query = ''
    states = list()
    if compare_var in ['cases', 'deaths']:
        query = 'SELECT DISTINCT date FROM state ORDER BY date desc LIMIT 1;'
        date = cur.execute(query).fetchall()[0]['date']
        query = ('SELECT fips,' + compare_var + " FROM state WHERE date='"
                 + date + "' and fips in ('" + "','".join(state_dict.keys())
                 + "') ORDER BY " + compare + ' LIMIT 5;')
        states = [x['fips'] for x in cur.execute(query).fetchall()]
    else:
        query = ('SELECT state,' + compare_var + ' FROM state_census ORDER BY '
                 + compare + ' LIMIT 5;')
        states = [x['state'] for x in cur.execute(query).fetchall()]

    for state in states:
        query = 'SELECT date,' + yaxis + ' FROM state WHERE fips=?;'
        results = cur.execute(query, tuple([state])).fetchall()
        x_data = [x['date'] for x in results]
        y_data = [x[yaxis] for x in results]
        fig.add_trace(go.Scatter(x=x_data, y=y_data,
                                 name=state_dict[state]))

    conn.close()

    fig.update_layout(xaxis_title='Date',
                      yaxis_title=yaxis[0].upper() + yaxis[1:])

    metric = None
    if compare_var == 'pop':
        metric = 'Population'
    else:
        metric = compare_var[0].upper() + compare_var[1:]

    if compare.split(' ')[1] == 'asc':
        metric = ' by States with Lowest ' + metric
    else:
        metric = ' by States with Highest ' + metric

    metric = yaxis[0].upper() + yaxis[1:] + metric

    return render_template('state_graph.html',
                           metric=metric,
                           graph_div=fig.to_html(full_html=False))


# COUNTY SECTION
@app.route('/county/state_selector', methods=['POST'])
def county_state_results():
    return redirect('/county/' + request.form['state'])


@app.route('/county/<state>', methods=['GET'])
@app.route('/county/<state>.html', methods=['GET'])
def county(state):
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = ("SELECT DISTINCT fips FROM county WHERE substr(fips,1,2)='"
             + state + "' ORDER BY fips;")
    counties = cur.execute(query).fetchall()

    query = 'SELECT DISTINCT date FROM county ORDER BY date;'
    dates = cur.execute(query).fetchall()
    conn.close()

    return render_template('county.html',
                           counties=[x['fips'] for x in counties],
                           county_dict=county_dict,
                           state=state,
                           state_dict=state_dict,
                           dates=[x['date'] for x in dates])


@app.route('/county/date_results/<state>', methods=['POST'])
def county_date_results(state):
    return redirect('/county/county_details/' + state + '/' +
                    request.form['county'] + '/' + request.form['date'])


@app.route('/county/county_details/<state>/<county>/<date>', methods=['GET'])
def county_date(state, county, date):
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = ('SELECT * FROM county INNER JOIN county_census'
             + ' as Cen ON fips=Cen.state||Cen.county WHERE'
             + ' fips=? ORDER BY date;')
    query_data = cur.execute(query, tuple([county])).fetchall()
    dates = [x['date'] for x in query_data]
    if date not in dates:
        if date < dates[0]:
            return redirect('/county/county_details/' + state + '/'
                            + county + '/' + dates[0])
        else:
            return redirect('/county/county_details/' + state + '/'
                            + county + '/' + dates[-1])
    conn.close()

    fig = go.Figure()
    y1_data = [x['cases'] for x in query_data]
    y2_data = [x['deaths'] for x in query_data]
    fig.add_trace(go.Scatter(x=dates, y=y1_data, name='Cases'))
    fig.add_trace(go.Scatter(x=dates, y=y2_data, name='Deaths'))
    fig.update_layout(xaxis_title='Date')
    fig.update_layout(yaxis_title='Number of Cases/Deaths')
    fig.update_layout(annotations=[
        dict(x=date, y=0, xref='x', yref='y', text='Selected Date',
             showarrow=True, arrowhead=7, ax=0, ay=-10)
    ])

    date_data = None
    for row in query_data:
        if row['date'] == date:
            date_data = row

    return render_template('county_detail.html',
                           graph_div=fig.to_html(full_html=False),
                           order=TABLE_ORDER,
                           names=TABLE_NAMES,
                           state=state_dict[state],
                           county=county_dict[county],
                           date_data=date_data)


@app.route('/county/county_compare/<state>', methods=['POST'])
def county_graph_results(state):
    compare = request.form['comparison'].replace(' ', '_')
    return redirect('/county/county_compare/' + state + '/' + compare
                    + '/' + request.form['yaxis'])


@app.route('/county/county_compare/<state>/<comparison>/<yaxis>', methods=['GET'])
def county_graph(state, comparison, yaxis):
    compare = comparison.replace('_', ' ')
    compare_var = compare.split(' ')[0]
    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    fig = go.Figure()
    query = ''
    counties = list()
    if compare_var in ['cases', 'deaths']:
        query = 'SELECT DISTINCT date FROM county ORDER BY date desc LIMIT 1;'
        date = cur.execute(query).fetchall()[0]['date']
        query = ('SELECT fips,' + compare_var + " FROM county WHERE date='"
                 + date + "' and fips in (SELECT state||county FROM "
                 + "county_census WHERE state='" + state + "') ORDER BY "
                 + compare + ' LIMIT 5;')
        counties = [x['fips'] for x in cur.execute(query).fetchall()]
    else:
        query = ('SELECT state||county as county_fips,' + compare_var +
                 " FROM county_census WHERE state='" + state + "' ORDER BY "
                 + compare + ' LIMIT 5;')
        counties = [x['county_fips'] for x in cur.execute(query).fetchall()]

    for county in counties:
        query = 'SELECT date,' + yaxis + ' FROM county WHERE fips=?;'
        results = cur.execute(query, tuple([county])).fetchall()
        x_data = [x['date'] for x in results]
        y_data = [x[yaxis] for x in results]
        fig.add_trace(go.Scatter(x=x_data, y=y_data,
                                 name=county_dict[county]))

    conn.close()

    fig.update_layout(xaxis_title='Date',
                      yaxis_title=yaxis[0].upper() + yaxis[1:])

    metric = None
    if compare_var == 'pop':
        metric = 'Population'
    else:
        metric = compare_var[0].upper() + compare_var[1:]

    if compare.split(' ')[1] == 'asc':
        metric = ' by Counties with Lowest ' + metric
    else:
        metric = ' by Counties with Highest ' + metric

    metric = yaxis[0].upper() + yaxis[1:] + metric

    return render_template('county_graph.html',
                           state=state_dict[state],
                           metric=metric,
                           graph_div=fig.to_html(full_html=False))


if __name__ == "__main__":
    data_setup.main_data_setup()

    conn = sqlite3.connect(data_setup.PROJECT_DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    state_dict = dict()
    query = 'SELECT state, name FROM state_census;'
    for row in cur.execute(query).fetchall():
        state_dict[row['state']] = row['name']

    county_dict = dict()
    query = ("select [state]||[county] as fips, " +
             "substr([name],0,instr([name],',')) " +
             "as name from county_census;")
    for row in cur.execute(query).fetchall():
        county_dict[row['fips']] = row['name']

    conn.close()
    app.run(debug=True)
