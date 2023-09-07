from flask import Flask, render_template, request, jsonify, flash, redirect,session
import pandas as pd
import json
import mysql.connector as sql
import numpy as np
from datetime import datetime
import warnings
import pandas as pd
import mysql.connector
warnings.filterwarnings("ignore")
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
app = Flask(__name__)
app.secret_key="inique"
db_connection = mysql.connector.connect(
    host='database-2.cbjabnlglbz6.ap-south-1.rds.amazonaws.com',
    database='vehicledb',
    user='admin',
    password='aparna123',
    auth_plugin='mysql_native_password'
)
@app.route("/", methods=["GET", "POST"])
def get_odo():
    cursor = db_connection.cursor()
    cursor.execute("SHOW TABLES")
    table_names = [table[0] for table in cursor.fetchall()]
    return render_template("index.html", table_names=table_names, c1result=None)
@app.route("/range", methods=["POST", "GET"])
def range():
    if request.method == "POST":
        start_date = request.form['start']
        end_date = request.form['end']
        selected_table = request.form.get('table_name')
        cur = db_connection.cursor()
        cursor = db_connection.cursor()
        cursor.execute("SHOW TABLES")
        table_names = [table[0] for table in cursor.fetchall()]
        alltables = table_names
        print(alltables)
        results = []
        for total in alltables:
            query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(total)
            cur.execute(query, (start_date, end_date))
            table_data = cur.fetchall()
            dfu = pd.DataFrame(table_data)
            dfu.columns = ['index', 'Latitude', 'Longitude', 'IGN',"PWR","GPS","Other Ports","Speed","Odometer","date_time","Data Received Time","Difference Duration","Heart Beat Data","History Data","Data String","Voltage(v)"]
            dfu= dfu[(dfu['IGN'] == 'On') & (dfu['GPS'] == 'On')]
            dfuu = dfu.drop_duplicates(subset=['Odometer'])
            dfuuu = dfuu[dfuu['Odometer'] != 0]
            dfuuu["Odometer"] = dfuuu["Odometer"].diff()
            dfv = dfuuu.groupby(dfuuu.date_time.dt.date)['Odometer'].sum()
            result = dfv.sum() / len(dfv) * 0.001
            formatted_num = "{:.2f}".format(result)
            results.append((result, total))
            results.sort(reverse=True)
            top_3_results = results[:3]
            print(top_3_results)
            data = [
                {"data": item[0], "label": item[1]} for item in top_3_results
            ]
            json_data = json.dumps(data)
        if selected_table == "all":
            cursor = db_connection.cursor()
            cursor.execute("SHOW TABLES")
            selected_tables = [table[0] for table in cursor.fetchall()]
        else:
            selected_tables = [selected_table]
        total_sum = 0
        id_count = 0
        counts = []
        individual_results = []
        for table in selected_tables:
            query = "SELECT * FROM {} WHERE date_time BETWEEN %s AND %s".format(table)
            cur.execute(query, (start_date, end_date))
            table_data = cur.fetchall()
            df = pd.DataFrame(table_data)
            dfk = pd.DataFrame(table_data)
            dfo = pd.DataFrame(table_data)
            dfh = pd.DataFrame(table_data)
            df.columns = ['index', 'Latitude', 'Longitude', 'IGN',"PWR","GPS","Other Ports","Speed","Odometer","date_time","Data Received Time","Difference Duration","Heart Beat Data","History Data","Data String","Voltage(v)"]
            dfk.columns = ['index', 'Latitude', 'Longitude', 'IGN',"PWR","GPS","Other Ports","Speed","Odometer","date_time","Data Received Time","Difference Duration","Heart Beat Data","History Data","Data String","Voltage(v)"]
            dfo.columns = ['index', 'Latitude', 'Longitude', 'IGN',"PWR","GPS","Other Ports","Speed","Odometer","date_time","Data Received Time","Difference Duration","Heart Beat Data","History Data","Data String","Voltage(v)"]
            dfh.columns = ['index', 'Latitude', 'Longitude', 'IGN',"PWR","GPS","Other Ports","Speed","Odometer","date_time","Data Received Time","Difference Duration","Heart Beat Data","History Data","Data String","Voltage(v)"]

            df.rename(columns={'Data Actual Time': 'date_time'}, inplace=True)
            print(df)
            df = df[(df['IGN'] == 'On') & (df['GPS'] == 'On')]
            df2 = df.drop_duplicates(subset=['Odometer'])
            df1 = df2[df2['Odometer'] != 0]
            df1["Odometer"] = df1["Odometer"].diff()
            df3 = df1.groupby(df1.date_time.dt.date)['Odometer'].sum()
            result = df3.sum() / len(df3) * 0.001
            formatted_num = "{:.2f}".format(result)
            total_sum += result
            individual_results.append(result)
            counts.append((id_count, table))
            cycle_count = 0
            previous_state = None
            for i, row in dfk.iterrows():
                current_state = row["IGN"]
                if previous_state == "On" and current_state == "Off":
                    cycle_count += 1
                previous_state = current_state
            cycle_count1 = 0
            previous_state1 = None
            for i, row in dfk.iterrows():
                current_state1 = row["IGN"]
                if previous_state1 == "Off" and current_state1 == "On":
                    cycle_count1 += 1
                previous_state1 = current_state1
            dfm = df[df['Speed']!=0]
            grouped = dfm.groupby(dfm.date_time.dt.date)
            dfn = df[df['Speed'] <= 35]
            grouped1 = dfn.groupby(dfn.date_time.dt.date)
            top_speed = grouped1["Speed"].max()
            avg_speed = grouped["Speed"].mean()
            df_json = avg_speed.to_json(date_format='iso')
            df1_json = top_speed.to_json(date_format="iso")
            df22 = dfo.drop_duplicates(subset=['Odometer'])
            df11 = df22[df22['Odometer'] != 0]
            df11["Odometer"] = df11["Odometer"].diff()
            df11['Time'] = pd.to_datetime(df11['date_time'], format='%d-%m-%Y %H:%M:%S')
            df11['Date'] = df11['Time'].dt.date
            df11['Time'] = df11['Time'].dt.time
            df11['Time'] = pd.to_datetime(df11['date_time'], format='%d-%m-%Y %H:%M:%S')
            df11['Weekday'] = df11['Time'].dt.strftime('%A')  # Convert date to weekday
            df11['Date'] = df11['Time'].dt.strftime('%Y-%m-%d')  # Convert date to YYYY-MM-DD format
            df11['Time'] = pd.to_datetime(df11['Time'], format='%d-%m-%Y %H:%M:%S')
            df11['Time'] = df11['Time'].dt.strftime('%H:00')
            df11["Odometer"] = df11["Odometer"] * 0.001
            df11['Date_Weekday'] = df11['Date'].astype(str) + ' (' + df11['Weekday'] + ')'
            pivot_table = df11.pivot_table(values='Odometer', index='Date_Weekday', columns='Time', aggfunc='sum')
            pivot_table_json = pivot_table.to_json(orient='columns')
            print(pivot_table_json)
            op_col = []
            for i in dfh['Speed']:
                op_col.append(i)
            np.set_printoptions(threshold=np.inf)
            lower_limit = int(request.form.get('lower_limit', 0))
            upper_limit1 = int(request.form.get('upper_limit1', 0))
            upper_limit2 = int(request.form.get('upper_limit2', 0))
            x = np.array(op_col)
            x1 = x.astype('int32')
            sub_lists = np.split(x1, np.where(np.diff(x1) < 0)[0] + 1)
            id_count = 0
            for unit in sub_lists:
                if min(unit) <= lower_limit and max(unit) > upper_limit1 and max(unit) < upper_limit2 and len(
                        set(unit)) > 1:
                    id_count += 1
        average_result = sum(individual_results) / len(individual_results)
        formatted_average_result = "{:.2f}".format(average_result)
        return jsonify({'htmlresponse': render_template('odo.html',average_result=average_result, c1result=formatted_num,data=df_json,data1=df1_json,count_result=id_count,cycle_count=cycle_count,cycle_count1=cycle_count1,pivot_table_json=pivot_table_json,id_count=id_count,json_data=json_data)})
if __name__ == '__main__':
 app.run(debug=True, port="3216")
