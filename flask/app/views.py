from app import app
from flask import render_template, jsonify
import happybase

project = {'name':'Nile',
    'desc': 'Sales Analytics' 
}

@app.route('/')
@app.route('/index')
def batch():
    return render_template('batch.html',
        title='Insight Data Engineering Project',
        project = project)

@app.route('/realtime')
def realtime():
    return render_template('realtime.html',
        title='Insight Data Engineering Project',
        project = project)

@app.route('/presentation')
def presentation():
    return render_template('presentation.html',
        title='Insight Data Engineering Project',
        project = project)

@app.route("/hbase/")
def hbase_test():
    connection = happybase.Connection('54.183.25.144')
    my_table = connection.table('cat_pcount')
    key6 = my_table.row('Romance')
    return key6['f:c1']

@app.route("/api/getProductsPerCategory")
def api_ProCat():
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('cat_pcount')
    data = hbase_table.scan()
    datalist = []
    for key, value in data:
        datalist.append([key,int(value['f:c1'])])
    
    dataChart1 = {
        'series' : [{"type": 'pie',"name": 'categories', "data": datalist}],
        'title' : {"text": 'Product Distribution by Category'},
    }
    return jsonify(dataChart1)

@app.route("/api/getSalesPerCategory_daily/<day>")
def api_scd(day = '2014-12-28'):
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('cat_day_vol')
    data = hbase_table.scan(row_prefix=day)
    datalist = []
    xAxislist = []
    for key, value in data:
        datalist.append(int(value['f:c1']))
        xAxislist.append(key[11:])
    
    dataChart = {
        'series' : [{"name": day , "data": datalist}],
        'title' : {"text": 'Day Sales'},
        'xAxis' : {"categories": xAxislist},
        'yAxis' : {"min" : 1000, "title": {"text": 'Product Count'}},
    }

    return jsonify(dataChart)

@app.route("/api/getSalesPerCategory_monthly/<month>")
def api_scm(month = '2014-12'):
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('cat_mth_vol')
    data = hbase_table.scan(row_prefix=month)
    datalist = []
    xAxislist = []
    for key, value in data:
        datalist.append(int(value['f:c1']))
        xAxislist.append(key[8:])
    
    dataChart = {
        'series' : [{"name": month , "data": datalist}],
        'title' : {"text": 'Monthly Sales'},
        'xAxis' : {"categories": xAxislist},
        'yAxis' : {"min" : 30000 , "title": {"text": 'Product Count'}},
    }

    return jsonify(dataChart)

@app.route("/api/getProductsPerRegion_monthly/<value>")
def api_prm(value = None):
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('salesPerZipcode')
    data = hbase_table.scan()
    datalist = []
    xAxislist = []
    for key, value in data:
        datalist.append(int(value['f:c1']))
        xAxislist.append(key[8:])
    
    dataChart = {
        'series' : [{"name": month , "data": datalist}],
        'title' : {"text": 'Monthly Sales'},
        'xAxis' : {"categories": xAxislist},
        'yAxis' : {"min" : 30000 , "title": {"text": 'Product Count'}},
    }

    return jsonify(dataChart)

@app.route("/api/getDateTotalRevenue")
def api_dtr(value = None):
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('day_vol_revenue')
    data1 = hbase_table.scan(row_prefix='2014-11')
    datalist1 = []
    xAxislist = []
    for key, value in data1:
        datalist1.append(int(value['f:c1']))
        xAxislist.append(key[7:])
    
    data2 = hbase_table.scan(row_prefix='2014-12')
    datalist2 = []
    for key, value in data2:
        datalist2.append(int(value['f:c1']))
        
    
    dataChart = {
        'series' : [{"name": "2014 Nov" , "data": datalist1},
                    {"name": "2014 Dec" , "data": datalist2}],
        'title' : {"text": 'Monthly Revenue Comparison'},
        'xAxis' : {"categories": xAxislist,
        'labels': {
                'rotation': 45
            }},
        'yAxis' : {"min" : 20000 , "title": {"text": 'Product Count'}},
    }

    return jsonify(dataChart)

@app.route("/api/getRealTime")
def api_realtime(value = None):
    connection = happybase.Connection('54.183.25.144')
    hbase_table = connection.table('day_vol_revenue')
    data = hbase_table.scan(row_prefix='2014-11')
    datalist = []
    xAxislist = []
    for key, value in data:
        datalist.append(int(value['f:c1']))
        xAxislist.append(key[7:])



    dataChart = {
        'series' : [{"name": "2015 Jan" , "data": datalist}],
        'title' : {"text": 'Daily Revenue (Product)'},
        'xAxis' : {"categories": xAxislist,
        'labels': {
                'rotation': 45
            }},
        'yAxis' : {"min" : 10000 , "title": {"text": 'Product'}},
    }

    return jsonify(dataChart)

