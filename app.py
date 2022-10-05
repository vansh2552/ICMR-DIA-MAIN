##################################### IMPORTING LIBRARIES ##############################

import base64
from calendar import c
import re
import getpass
import json
import math
import pymysql
import os
import sys
import subprocess
import hashlib
import pathlib
import datetime
import numpy as np
import pandas as pd
import requests as rq
from io import StringIO
from flask import Flask, render_template, flash, url_for, request, redirect, session, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta
from os import listdir
from urllib.parse import quote
from os.path import isfile, join

from constants.constants import db_credentials, webpage

##################################### HELPING FUNCTIONS ################################


def get_sqlalchemy_db_url():
    # return 'mysql+pymysql://jasleen:%s@127.0.0.1/jas_amr' % quote("jasleen123")
    # return 'mysql+pymysql://Nikhil:%s@127.0.0.1/amr2021' % quote('Nikhil_123')
    return 'mysql+pymysql://' + str(db_credentials.USER.value) + ':' + str(quote(str(db_credentials.PASSWORD.value))) + '@' + str(db_credentials.HOST.value) + '/' + str(db_credentials.DB.value)


def db_connect():
    conn = pymysql.connect(host=db_credentials.HOST.value, db=db_credentials.DB.value,
                           user=db_credentials.USER.value, password=db_credentials.PASSWORD.value)
    return conn


def get_table_columns(table_name):
    cur.execute('SELECT * from ' + table_name + ' LIMIT 0;')
    col = []
    for col_val in cur.description:
        col.append(col_val[0])
    return pd.Series(col).values


def handle_date(date_arr):
    for i in date_arr:
        if "-" in i:
            try:
                datetime.strptime(i, '%Y-%m-%d')
                continue
            except ValueError:
                # if i[4] == "-":
                #     date_arr[date_arr.index(i)] = datetime.strptime(i, '%Y-%m-%d').strftime('%Y-%m-%d')
                if i[2] == "-":
                    date_arr[date_arr.index(i)] = datetime.strptime(i, '%d-%m-%Y').strftime('%Y-%m-%d')
        elif "/" in i:
            if i[4] == "/":
                date_arr[date_arr.index(i)] = datetime.strptime(i, '%Y/%m/%d').strftime('%Y-%m-%d')
            elif i[2] == "/":
                date_arr[date_arr.index(i)] = datetime.strptime(i, '%d/%m/%Y').strftime('%Y-%m-%d')

    return date_arr

########################################################################################


app = Flask(__name__)
app.config['DEBUG'] = True
# app.config['SQLALCHEMY_DATABASE_URI'] = get_sqlalchemy_db_url()
app.config['SQLALCHEMY_DATABASE_URI'] = get_sqlalchemy_db_url()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_POOL_PRE_PING"] = True

db = SQLAlchemy(app)
db.init_app(app)
SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}
app.secret_key = db_credentials.PASSWORD.value

###################################### MAIN FUNCTIONS ##################################

conn = db_connect()
cur = conn.cursor()


@app.route('/', methods=['GET', 'POST'])
def index():
    print(get_sqlalchemy_db_url())
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/', methods=['GET', 'POST'])
def login():
    conn = db_connect()
    cur = conn.cursor()

    msg = ""
    if request.method == 'POST' and 'user_name' in request.form and 'password' in request.form:
        username = (request.form['user_name'])
        password = request.form['password']
        pas = hashlib.md5(password.encode()).hexdigest()
        try:
            msg = ""
            cur.execute(
                'SELECT * FROM users WHERE username = %s AND password = %s', (username, pas))

            account = cur.fetchone()
            session['loggedin'] = True
            session['user_id'] = account[0]
            session['username'] = account[3]
            session['role'] = account[6]
            session["user_names"] = account[1]
            session['laboratory'] = (account[5])
            conn.close()
            if(session['role'] != "DEO"):
                msg = "Unauthorized User"
                return render_template(webpage.LOGIN_PAGE.value, msg=msg)

            return render_template(webpage.INDEX_PAGE.value, username=session['username'])

        except:
            msg = "Invalid Password/Username, Enter valid Details"
            return render_template(webpage.LOGIN_PAGE.value, msg=msg)
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/logout')
def logout():
    conn = db_connect()
    cur = conn.cursor()

    session.pop('loggedin', None)
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('role', None)
    session.pop('laboratory', None)
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/upload', methods=['GET', 'POST'])
def upload():
    conn = db_connect()
    cur = conn.cursor()

    msg = ""
    if 'loggedin' in session:
        msg = ""
        userid = session['user_id']
        if request.method == 'POST':
            if request.form['action'] == 'Registered hospital':
                header_filename = "dataconfig_file_"+str(userid)+"/"
                isDir = os.path.isdir(header_filename)
                if(isDir):
                    files = []
                    file = [f for f in listdir(header_filename) if isfile(
                        join(header_filename, f))]
                    for i in file:
                        if(i == "antibiotic_config_file.json" or i == "optionset_config_file.json" or i == 'header_config_file.json'):
                            files.append(i)
                    files = sorted(files, key=lambda s: s.casefold())
                    conn.close()
                    return render_template(webpage.IMPORT_PAGE.value, files=files, username=session['username'])
                else:
                    conn.close()
                    msg = "FILE NOT EXISTS PLEASE CREATE CONFIGURATION FILE FIRST"
                    return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            elif request.form['action'] == 'Instructions':
                conn.close()
                return render_template(webpage.INSTRUCTIONS_PAGE.value, username=session['username'])

            elif request.form['action'] == 'demovideo':
                conn.close()
                return redirect('/static/video.mp4')

            elif request.form['action'] == 'upload CSV':

                try:
                    header_filename = "dataconfig_file_"+str(userid)+"/"
                    os.mkdir(header_filename)
                except OSError as error:
                    print(error)
                    
                f = request.files['fl']
                f.save(header_filename+'hosp.csv')
                try:    
                    dset = pd.read_csv(header_filename+'hosp.csv', encoding='unicode_escape', skipinitialspace=True)
                    # Removed starting and Ending spaces from the header 
                    dset.columns = dset.columns.str.strip()
                    headers = dset.columns.values

                    dbheaders2 = get_table_columns('hospital_patient_relation')
                    dbheaders3 = get_table_columns('patient_information')
                    dbheaders4 = get_table_columns('sample_information')
                    dbheaders5 = get_table_columns('susceptibility_testing')
                    
                    conn.close()
                    return render_template("mapping.html", fields=headers, dbfields2=dbheaders2, dbfields3=dbheaders3, dbfields4=dbheaders4, dbfields5=dbheaders5, username=session['username'])

                except Exception as e:
                    conn.close()
                    msg = "Import the file"
                    return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])
            elif request.form['action'] == 'edit options':
                try:
                    header_filename = "dataconfig_file_"+str(userid)+"/"
                    os.mkdir(header_filename)
                except OSError as error:
                    print(error)
                f = request.files['fl2']
                f.save(header_filename+'hosp.csv')
              
                # f2 = request.files['fljson']
                # f2.save(header_filename+'optionset_config_file.json')
              
                try:
                    dset = pd.read_csv(header_filename+'hosp.csv', encoding='unicode_escape', skipinitialspace=True)
                    dset.columns = dset.columns.str.strip()

                    with open(header_filename+'combine_config_file.json', "r") as q:
                        dict_combine_new = q.read()
                    dict_new = json.loads(dict_combine_new)
                    dset.rename(columns=dict_new, inplace=True)
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())

                    # organism
                    df_organism_name = pd.read_sql_query( 'select * from organisms', con=conn)
                    orgval = df_organism_name['organism_name'].values
                    dset['organism_id'] = dset['organism_id'].str.capitalize()
                    u7 = dset.organism_id.unique()
                    

                    org_int_id = df_organism_name['organism_id'].values
                    org_id = list(map(str, org_int_id))
                    dict_orid = {}
                    for i in range(len(org_id)):
                        dict_orid[i]=org_id[i]
                    
                    print(dict_orid)

                    with open(header_filename+'optionset_config_file.json', "r") as r:
                        data_option = r.read()
                        dictopt = json.loads(data_option)
                    # print(dictopt)
                    di=dictopt[9]
                    print(di)

                    # sample
                    df_sample_type = pd.read_sql_query('select * from sample_type_master', con=conn)
                    sampleval = df_sample_type['sample_type'].values
                    dset['sample_type'] = dset['sample_type'].str.capitalize()
                    u5 = dset.sample_type.unique()
                    sv_int_id = df_sample_type['sample_type_id'].values
                    sv_id = list(map(str, sv_int_id))
                    dict_sampleid = {}
                    for i in range(len(sv_id)):
                        dict_sampleid[i] = sv_id[i]
                    # dsample=dictopt[7]
                    dsampleid=dictopt[8]
                    print(dsampleid)
                    print(dict_sampleid)

                    # antibiotic
                    df_antibiotic_name = pd.read_sql_query( 'select * from antibiotics', con=conn)
                    antival1 = df_antibiotic_name['antibiotic_name'].values
                    antival2 = df_antibiotic_name['susceptibility_test_type'].values
                    antival = [i + j for i, j in zip(antival1, antival2)]
                    u_anti_val=dset.antibiotic_id.unique()

                    anti_int_id=df_antibiotic_name['antibiotic_id'].values
                    anti_id = list(map(str, anti_int_id))
                    dict_antiid = {}
                    for i in range(len(anti_id)):
                        dict_antiid[i]=anti_id[i]

                    with open(header_filename+'antibiotic_config_file.json', "r") as a:
                        data_option_1 = a.read()
                        dictant = json.loads(data_option_1)
                    
                    dianti=dictant
                    

                    #department
                    df_department_name = pd.read_sql_query( 'select * from hospital_dept_master', con=conn)
                    departmentval = df_department_name['department_name'].values
                    u_department_val=dset.hospital_department.unique()
                    dept_int_id=df_department_name['hospital_dept_id'].values
                    dept_id = list(map(str, dept_int_id))
                    dict_deptid = {}
                    for i in range(len(dept_id)):
                        dict_deptid[i]=dept_id[i]

                    didept=dictopt[5]

                    conn.close()
                    return render_template('editset.html',optdept=u_department_val,deptmap=departmentval,didept=didept,dideptnew=dict_deptid,
                    optanti=u_anti_val,antimap=antival,dianti=dianti,diantinew=dict_antiid,
                    opts=u5,optio=u7,samplemap=sampleval, orgmap=orgval,dism=dsampleid,diog=di,dismnew=dict_sampleid,dinew=dict_orid,
                    username=session['username'])

                except Exception as e:
                    print(e)
                    conn.close()
                    msg = "Import the file"
                    return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/editing_mapping', methods=['GET', 'POST'])
def editing_mapping():
    conn = db_connect()
    cur = conn.cursor()
    
    msg = ""
    userid = session["user_id"]
    if 'loggedin' in session:

        msg =""
        header_filename = "dataconfig_file_"+str(userid)+"/"

        df_organism_id = pd.read_sql_query('select * from organisms', con=conn)
        org_int_id = df_organism_id['organism_id'].values
            
        org_id = list(map(str, org_int_id))
            
        dict_orgmap = {}
        for i in range(len(org_id)):
            t = request.form.get('om' + str(i))
            print(t)
            dict_orgmap[t] = org_id[i]
        if 'select option set from the list' in dict_orgmap.keys():
            del dict_orgmap['select option set from the list']


        
        df_antibiotic_id = pd.read_sql_query('select * from antibiotics', con=conn)
        anti_int_id = df_antibiotic_id['antibiotic_id'].values

        anti_id = list(map(str, anti_int_id))

        dict_antimap = {}
        for i in range(len(anti_id)):
            t = request.form.get('anti' + str(i))
            dict_antimap[t] = anti_id[i]
        if 'select option set from the list' in dict_antimap.keys():
            del dict_antimap['select option set from the list']

        df_dept_id = pd.read_sql_query('select * from hospital_dept_master', con=conn)
        dv= df_dept_id['department_name'].values
        dept_int_id = df_dept_id['hospital_dept_id'].values
            
        dept_id = list(map(str, dept_int_id))
            
        dict_deptmap = {}
        for i in range(len(dv)):
            t = request.form.get('dept' + str(i))
            dict_deptmap[t] = dv[i]
        if 'select option set from the list' in dict_deptmap.keys():
            del dict_deptmap['select option set from the list']
        
        dict_deptmap_id = {}
        for i in range(len(dept_id)):
            t = request.form.get('dept' + str(i))
            dict_deptmap_id[t] = dept_id[i]
        if 'select option set from the list' in dict_deptmap_id.keys():
            del dict_deptmap_id['select option set from the list']


        # fetching sample type name from table
        df_sample_type = pd.read_sql_query('select * from sample_type_master', con=conn)
        sv = df_sample_type['sample_type'].values
        # fetching sample type id from table
        df_sample_type_id = pd.read_sql_query('select * from sample_type_master', con=conn)
        sv_int_id = df_sample_type_id['sample_type_id'].values
        # converting integer into string
        sv_id = list(map(str, sv_int_id))

        # get value for option set mapping of sample type column
        dict_samplemap = {}
        for i in range(len(sv)):
            t = request.form.get('sm' + str(i))
            dict_samplemap[t] = sv[i]
        if 'select option set from the list' in dict_samplemap.keys():
            del dict_samplemap['select option set from the list']

        dict_sampletype_id = {}
        for i in range(len(sv_id)):
            t = request.form.get('sm' + str(i))
            dict_sampletype_id[t] = sv_id[i]
        if 'select option set from the list' in dict_sampletype_id.keys():
            del dict_sampletype_id['select option set from the list']
            


        with open(header_filename+'optionset_config_file.json', "r") as r:
                        data_option = r.read()
                        dictopt = json.loads(data_option)
        
        # updated the optioset_config_file.json
        dictopt[7] = dict_samplemap
        dictopt[8] = dict_sampletype_id
        dictopt[9] = dict_orgmap
        dictopt[4]= dict_deptmap
        dictopt[5]= dict_deptmap_id
        with open(header_filename+'optionset_config_file.json', 'w') as outfile1:
                json.dump(dictopt, outfile1, indent=4)
        
        #  updated the combinet_config_optionset.json
        mergedopt = {}
        for i in range(12):
            mergedopt.update(dictopt[i])
        with open(header_filename+'combine_config_optionset_file.json', 'w') as outfile2:
            json.dump(mergedopt, outfile2, indent=4)
        
        with open(header_filename+'antibiotic_config_file.json', "r") as r1:
                        data_option1 = r1.read()
                        dictanti = json.loads(data_option1)

        dictanti=dict_antimap
        with open(header_filename+'antibiotic_config_file.json', 'w') as outfile3:
                json.dump(dictanti, outfile3, indent=4)
        

        conn.close()
        return render_template('download1.html', username=session['username'])

        


    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/mapping', methods=['GET', 'POST'])
def mapping_file():
    conn = db_connect()
    cur = conn.cursor()

    msg = ""
    userid = session["user_id"]
    if 'loggedin' in session:
        msg = ""
        header_filename = "dataconfig_file_"+str(userid)+"/"
        if request.method == 'POST':
            dset = pd.read_csv(header_filename+'hosp.csv',encoding='unicode_escape', skipinitialspace=True)
            dset.columns = dset.columns.str.strip()
            if(dset.empty):
                conn.close()
                msg = "Please upload file"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            headers = dset.columns.values
            headers = sorted(headers, key=lambda s: s.casefold())
            t2c2 = request.form.get('patient_regnum')
            t2c4 = request.form.get('hosp_adm_date')
            t2c6 = request.form.get('hosp_dept')
            t2c8 = request.form.get('hosp_prior_admission')
            t2c9 = request.form.get('hosp_loc_type')
            t2c10 = request.form.get('hosp_infec_type')
            t2c11 = request.form.get('hosp_clinical_diag')
            t2c12 = request.form.get('hosp_comorbidity')
            t2c13 = request.form.get('hosp_devices_used')

            t3c1 = request.form.get('patient_regnum')
            t3c4 = request.form.get('patient_age_year')
            t3c5 = request.form.get('patient_gender')
            t3c6 = request.form.get('patient_dob')
            t3c7 = request.form.get('patient_city')
            t3c8 = request.form.get('patient_district')
            t3c9 = request.form.get('patient_state')
            t3c10 = request.form.get('patient_country')
            t3c11 = request.form.get('patient_loctype')
            t3c12 = request.form.get("unit_of_age")

            t4c1 = request.form.get('sample_lab_id')
            t4c3 = request.form.get('sample_collection_date')
            t4c4 = request.form.get('sample_type')
            t4c8 = request.form.get('sample_prescribed_antibiotic')

            t5c3 = request.form.get('susceptibility_antibiotic_id')
            t5c5 = request.form.get('susceptibility_organism_id')
            t5c8 = request.form.get('susceptibility_identification_method')
            t5c11 = request.form.get('susceptibility_value')
            t5c16 = request.form.get('susceptibility_validation_status')
            t5c17 = request.form.get('susceptibility_remarks')

            # sending value of antibiotic_id header into database
            dbh2 = get_table_columns('hospital_patient_relation')
            dbh3 = get_table_columns('patient_information')
            dbh4 = get_table_columns('sample_information')
            dbh5 = get_table_columns('susceptibility_testing')

            dict_table2 = {t2c2: dbh2[2], t2c4: dbh2[4], t2c6: dbh2[6], t2c8: dbh2[14],
                           t2c9: dbh2[9], t2c10: dbh2[10], t2c11: dbh2[11], t2c12: dbh2[12], t2c13: dbh2[13]}
            dict_table3 = {t3c1: dbh3[1], t3c4: dbh3[4], t3c5: dbh3[5], t3c6: dbh3[6], t3c7: dbh3[7],
                           t3c8: dbh3[8], t3c9: dbh3[9], t3c10: dbh3[10], t3c11: dbh3[11]+"1", t3c12: "Unit"}
            dict_table4 = {t4c1: dbh4[1], t4c3: dbh4[3],
                           t4c4: dbh4[4], t4c8: dbh4[8]}
            dict_table5 = {t5c3: dbh5[3], t5c5: dbh5[5], t5c8: dbh5[8],
                           t5c11: dbh5[11], t5c16: dbh5[16], t5c17: dbh5[17]}

            # Remove Mapping if any column is not select corresponding to it
            if 'select header from the list' in dict_table2.keys():
                del dict_table2['select header from the list']
            if 'select header from the list' in dict_table3.keys():
                del dict_table3['select header from the list']
            if 'select header from the list' in dict_table4.keys():
                del dict_table4['select header from the list']
            if 'select header from the list' in dict_table5.keys():
                del dict_table5['select header from the list']
            dict_combine = [dict_table2, dict_table3, dict_table4, dict_table5]
            merged = {}
            for x in dict_combine:
                merged.update(x)
            

            # Save two json file, 
            # 1.header_config_file: Array of Mapping of each 4 section as individual (patient info, hospital pateint relation, sample info, susceptibilty testing)
            # 2. combine_config_file: A single object in JSON containing mapping of all above header together
            with open(header_filename+"header_config_file.json", 'w') as outfile1:
                json.dump([dict_table2, dict_table3, dict_table4, dict_table5], outfile1, indent=4)

            with open(header_filename+'combine_config_file.json', 'w') as outfile2:
                json.dump(merged, outfile2, indent=4)

            # Renamed the dset(only variable dset not the HOSP.csv) header with mapped header
            dset = pd.read_csv(header_filename+'hosp.csv', encoding='unicode_escape', skipinitialspace=True)
            dset.columns = dset.columns.str.strip()
            dset.rename(columns=merged, inplace=True)

            
            
            # set unique values for option set
            df_antibiotic = pd.read_sql_query('select * from antibiotics', con=conn)
            antival = df_antibiotic['antibiotic_name'].values
            guide = df_antibiotic['guidlines'].values
            sustest = df_antibiotic['susceptibility_test_type'].values
            
            # if user does NOT mapped antibiotic columns 
            # means antibiotics are in column wise  (each column dedicated for an antibiotic)
            if 'antibiotic_id' not in dict_table5.values():
                conn.close()
                return render_template('antibiotic_mapping.html', fields=headers, antimap=antival, guimap=guide, testmap=sustest, username=session["username"])
            else:
                uniquevalues = dset.antibiotic_id.unique()
                conn.close()
                return render_template('antibiotic_mapping.html', fields=uniquevalues, antimap=antival, guimap=guide, testmap=sustest, username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/mapping_antibiotics', methods=['GET', 'POST'])
def mapping_antibiotics():
    conn = db_connect()
    cur = conn.cursor()

    if 'loggedin' in session:
        userid = session["user_id"]
        header_filename = "dataconfig_file_"+str(userid)+"/"
        if request.method == 'POST':
            dset = pd.read_csv(header_filename+'hosp.csv', encoding='unicode_escape', skipinitialspace=True)
            dset.columns = dset.columns.str.strip()
            with open(header_filename+'combine_config_file.json', "r") as p:
                data = p.read()
                
            # loading combine JSON Mapping as dictionary
            dict_config_file = json.loads(data)
            dictall = (dict_config_file)
            a = (dictall.keys())

            # Fetching dataframe with only columns mentioned in 'combine config file.json'
            df_combine = pd.DataFrame(dset)
            df_combine = df_combine[a]

            # Replacing hospital specific headers with ICMR specific headers 
            df_combine.rename(columns=dictall, inplace=True)

    # Antibiotic
            # fetching antibiotic id from table
            df_antibiotic = pd.read_sql_query('select * from antibiotics', con=conn)
            anti_int_id = df_antibiotic['antibiotic_id'].values
            
            # converting integer into string
            anti_id = list(map(str, anti_int_id))

            # get value for option set mapping of antibiotic column
            dict_antibiotic = {}
            for i in range(len(anti_id)):
                t = request.form.get('anti' + str(i))
                dict_antibiotic[t] = anti_id[i]
            del dict_antibiotic['select antibiotic from the list']

            # save configuration file for antibiotic
            with open(header_filename+'antibiotic_config_file.json', 'w') as outfile:
                json.dump(dict_antibiotic, outfile, indent=4)

            # getting All filled mapping(response) corresponding to each Antibiotics
            list_second = []
            for i in range(len(anti_id)):
                t = request.form.get('anti' + str(i))
                list_second.append(t)
            
            # counting the starting index where antibiotics's header starts
            headers = list(dset.columns)
            count = 0
            for e1 in headers:
                if e1 not in list_second:
                    count = count + 1
                else:
                    break
                
            b = count-1
            antibiotic_start = e1
            header_finish = headers[b]

            # if each antibiotic has its own dedicated column
            if "antibiotic_id" not in dict_config_file.values():
                def isNan(val):
                    if isinstance(df2.loc[ind, col], float) and math.isnan(val):
                        return True
                    return False
                df1 = dset.loc[:, :header_finish]
                df2 = dset.loc[:, antibiotic_start:].T
                # df1.to_csv('df1.csv', index=False)
                # df2.to_csv('df2.csv', index=False)
                req_df_col = df1.columns.values.tolist()
                req_df_col.extend(["antibiotic_id", "value"])
                req_df = pd.DataFrame(columns=req_df_col)
                req_df["antibiotic_id"] = req_df["antibiotic_id"].astype(str)
                req_df["value"] = req_df["value"].astype(str)
                for col in df2.columns.values:
                    col_val_count = 0
                    for ind in df2.index:
                        if not isNan(df2.loc[ind, col]):
                            req_df.loc[len(req_df.index)] = df1.loc[col]
                            req_df.loc[len(req_df.index)-1, "antibiotic_id"] = ind
                            req_df.at[len(req_df.index)-1, "value"] = df2.loc[ind, col]
                            col_val_count += 1
                    if col_val_count == 0:
                        req_df.loc[len(req_df.index)] = df1.loc[col]
                        req_df.at[len(req_df.index)-1, "antibiotic_id"] = None
                        req_df.at[len(req_df.index)-1, "value"] = None
                # req_df.to_csv('req_df.csv', index=False)
                req_df.to_csv(header_filename + "app_transpose_file.csv", index=False)

            with open(header_filename+'combine_config_file.json', "r") as q:
                dict_combine_new = q.read()
            dict_new = json.loads(dict_combine_new)
            dset.rename(columns=dict_new, inplace=True)
            conn.close()
            
            conn = db_connect()
            cur = conn.cursor()

            engine = create_engine(get_sqlalchemy_db_url())

            df_hospdept = pd.read_sql_query('select * from hospital_dept_master', con=conn)
            deptval = df_hospdept['department_name'].values
            df_sample_type = pd.read_sql_query('select * from sample_type_master', con=conn)
            sampleval = df_sample_type['sample_type'].values
            df_organism_name = pd.read_sql_query( 'select * from organisms', con=conn)
            orgval = df_organism_name['organism_name'].values

            df_state_district = pd.read_sql_query("select * from state_district_master", con=conn)
            state_dict = {}
            for i in df_state_district.state.unique():
                dis_df = df_state_district.loc[(df_state_district['state'] == i)]
                state_dict[i] = list(set(dis_df["district"].tolist()))
            
            # set unique values for option set
            dset['gender'] = dset['gender'].str.capitalize()
            dset['hospital_department'] = dset['hospital_department'].str.capitalize()
            dset['sample_type'] = dset['sample_type'].str.capitalize()
            dset['identification_method'] = dset['identification_method'].str.capitalize()
            dset['organism_id'] = dset['organism_id'].str.capitalize()
            if 'location_type' in dset.columns.values:
                dset['location_type'] = dset['location_type'].str.capitalize()
            dset['country'] = dset['country'].str.capitalize()
            # dset['state'] = dset['state'].str.capitalize()
            # dset['district'] = dset['district'].str.capitalize()
            
            ustate = dset.state.unique()
            udistrict = dset.district.unique()
            u1 = dset.gender.unique()
            if "location_type1" in dset.columns:
                if not dset['location_type1'].isnull().all():
                    dset['location_type1'] = dset['location_type1'].str.capitalize()
                    u2 = dset.location_type1.unique()
                else:
                    u2 = []
            else:
                u2 = []
                
            u3 = dset.country.unique()
            u4 = dset.hospital_department.unique()
            u5 = dset.sample_type.unique()
            u6 = dset.identification_method.unique()
            u7 = dset.organism_id.unique()
            if "prior_admission" in dset.columns:
                if not dset['prior_admission'].isnull().all():
                    dset['prior_admission'] = dset['prior_admission'].str.capitalize()
                    u8 = dset.prior_admission.unique()
                else:
                    u8 = []
            else:
                u8 = []
            u9 = dset.location_type.unique()
            if "infection_type" in dset.columns:
                if not dset['infection_type'].isnull().all():
                    dset['infection_type'] = dset['infection_type'].str.capitalize()
                    u10 = dset.infection_type.unique()
                else:
                    u10 = []
            else:
                u10 = []
            if "comorbidity" in dset.columns:
                if not dset['comorbidity'].isnull().all():
                    dset['comorbidity'] = dset['comorbidity'].str.capitalize()
                    u11 = dset.comorbidity.unique()
                else:
                    u11 = []
            else:
                u11 = []
            if "devices_used" in dset.columns:
                if not dset['devices_used'].isnull().all():
                    dset['devices_used'] = dset['devices_used'].str.capitalize()
                    u12 = dset.devices_used.unique()
                else:
                    u12 = []
            else:
                u12 = []
            if "Unit" in dset.columns:
                if not dset['Unit'].isnull().all():
                    dset['Unit'] = dset['Unit'].str.capitalize()
                    uage = dset.Unit.unique()
                else:
                    uage = []
            else:
                uage = []

            # with open(header_filename+'headers_checkpoint.json', 'w') as checkpoint:
            #     json.dump([ustate,udistrict, state_dict, u10, u11, u12, u9, u8, u1, u2, u3, u4, u5, u6, u7, deptval, sampleval, orgval, uage], checkpoint, indent=4)
            
            conn.close()
            return render_template('optionset.html', optstate=ustate, optdistrict=udistrict, state_district_dict=state_dict, inf=u10, com=u11, dev=u12, optk=u9, optp=u8, optg=u1, optl=u2, optc=u3, opth=u4, opts=u5, opti=u6, optio=u7, deptmap=deptval, samplemap=sampleval, orgmap=orgval, optage=uage, username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/mapping_optionset', methods=['GET', 'POST'])
def mapping_option():
    conn = db_connect()
    cur = conn.cursor()

    if 'loggedin' in session:   
        userid = session["user_id"]
        header_filename = "dataconfig_file_"+str(userid)+"/"
        if request.method == 'POST':
            
            def alert_multiple_mapping(lists):
                used = {}
                for list in lists:
                    for i in range(1, len(list)):
                        dist_value = list[i]
                        if dist_value in used.keys():
                            return True, dist_value
                        used[dist_value] = True
                return False, ''
                

            # get value for option set mapping of gender column
            s1 = request.form.getlist('mal')
            s2 = request.form.getlist('fem')
            s3 = request.form.getlist('trans')
            # just to distinguish for key values
            s1.insert(0, "Male")
            s2.insert(0, "Female")
            s3.insert(0, "Transgender")
            alert, dup = alert_multiple_mapping([s1, s2, s3])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in GENDER section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])
               
            # get value for pateint location column
            s4 = request.form.getlist('urb')
            s5 = request.form.getlist('rur')
            s4.insert(0, "Urban")
            s5.insert(0, "Rural")
            alert, dup = alert_multiple_mapping([s4, s5])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in PATEINT LOCATION section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            # get value for option set mapping of country column
            s6 = request.form.getlist('in')
            s7 = request.form.getlist('out_in')
            s6.insert(0, "India")
            s7.insert(0, "Outside_India")
            alert, dup = alert_multiple_mapping([s6, s7])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in COUNTRY section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            # get value for option set mapping of unit of age
            sd = request.form.getlist('days')
            sm = request.form.getlist('months')
            sy = request.form.getlist('years')
            sd.insert(0, "Days")
            sm.insert(0, "Months")
            sy.insert(0, "Year")
            alert, dup = alert_multiple_mapping([sd, sm, sy])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in UNIT OF AGE section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            # get value for option set mapping of identification method column
            s8 = request.form.getlist('biochem_test')
            s9 = request.form.getlist('proteomics')
            s10 = request.form.getlist('vitek')
            s11 = request.form.getlist('maldi')
            s12 = request.form.getlist('rna_sequencing')
            s13 = request.form.getlist('gnome_sequencing')
            s14 = request.form.getlist('microscopy')
            s8.insert(0, "Biochem_test")
            s9.insert(0, "Proteomics")
            s10.insert(0, "Vitek")
            s11.insert(0, "Maldi")
            s12.insert(0, "Rna_seq")
            s13.insert(0, "Gnome_seq")
            s14.insert(0, "Microscopy")
            alert, dup = alert_multiple_mapping([s8, s9, s10, s11, s12, s13, s14])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in IDENTIFICATION METHOD section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])

            # get value for option set mapping of prior admission column
            s15 = request.form.get('icu')
            s16 = request.form.get('ward')

            # get value for option set mapping of hospital location column
            s17 = request.form.getlist('loc_icu')
            s18 = request.form.getlist('loc_ward')
            s19 = request.form.getlist('loc_opd')
            s17.insert(0, "Loc_icu")
            s18.insert(0, "Loc_ward")
            s19.insert(0, "Loc_opd")
            alert, dup = alert_multiple_mapping([s17, s18, s19])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in HOSPITAL LOCATION section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])
            
            # comorbidity
            s20 = request.form.get('malig')
            s21 = request.form.get('diab_mell')
            s22 = request.form.get('transplant')
            s23 = request.form.get('covid')
            
            # infection_type
            s24 = request.form.getlist('com_acq_inf')
            s25 = request.form.getlist('heal_care_asso_inf')
            s24.insert(0, "Com_acq_inf")
            s25.insert(0, "Heal_care_asso_inf")
            alert, dup = alert_multiple_mapping([s24, s25])
            if alert == True:
                conn.close()
                msg = "There are Duplicate mapping of " + str(dup) + " keyword in INFECTION TYPE section"
                return render_template(webpage.INDEX_PAGE.value, msg=msg, username=session['username'])
            
            # patient with devices
            s26 = request.form.get('cen_lin_cat')
            s27 = request.form.get('endo_tube')
            s28 = request.form.get('mech_vent')
            s29 = request.form.get('urin_cath')
            s30 = request.form.get('oth_vas_acc_lin')
            
                   
            # Hospital Department
            # fetching hospital department names from table
            df_hospdept = pd.read_sql_query('select * from hospital_dept_master', con=conn)
            dv = df_hospdept['department_name'].values
            # fetching hospital department id from table
            df_hospdept_id = pd.read_sql_query('select * from hospital_dept_master', con=conn)
            dv_int_id = df_hospdept_id['hospital_dept_id'].values
            # converting integer into string
            dv_id = list(map(str, dv_int_id))

            # get value for option set mapping of hospital department column
            dict_hospmap = {}
            for i in range(len(dv)):
                t = request.form.get('hm' + str(i))
                dict_hospmap[t] = dv[i]
            if 'select option set from the list' in dict_hospmap.keys():
                del dict_hospmap['select option set from the list']

            dict_hospdept_id = {}
            for i in range(len(dv_id)):
                t = request.form.get('hm' + str(i))
                dict_hospdept_id[t] = dv_id[i]
            if 'select option set from the list' in dict_hospdept_id.keys():
                del dict_hospdept_id['select option set from the list']

    # Sample Type

            # fetching sample type name from table
            df_sample_type = pd.read_sql_query('select * from sample_type_master', con=conn)
            sv = df_sample_type['sample_type'].values
            # fetching sample type id from table
            df_sample_type_id = pd.read_sql_query('select * from sample_type_master', con=conn)
            sv_int_id = df_sample_type_id['sample_type_id'].values
            # converting integer into string
            sv_id = list(map(str, sv_int_id))

            # get value for option set mapping of sample type column
            dict_samplemap = {}
            for i in range(len(sv)):
                t = request.form.get('sm' + str(i))
                dict_samplemap[t] = sv[i]
            if 'select option set from the list' in dict_samplemap.keys():
                del dict_samplemap['select option set from the list']

            dict_sampletype_id = {}
            for i in range(len(sv_id)):
                t = request.form.get('sm' + str(i))
                dict_sampletype_id[t] = sv_id[i]
            if 'select option set from the list' in dict_sampletype_id.keys():
                del dict_sampletype_id['select option set from the list']


    #State & District original  with state district
            df_state = pd.read_sql_query('select * from state_district_master', con=conn)
            state_dict2 = {}
            for i in df_state.state.unique():
                state_dict2[i]={}
                dis_df=df_state.loc[(df_state['state'] == i)]
                # print("dis_df",dis_df)
                for j in dis_df.district.unique():
                    t2=request.form.get('dis'+i+'_'+j)
                    state_dict2[i][t2]=j
                    
            for key, val in state_dict2.items():
                if isinstance(val, dict):
                    if 'select option set from the list' in val.keys():
                        del val['select option set from the list'] 
                        
            state_dict2 = {key : val for key, val in state_dict2.items() if val}

    #State & District original ONLY STATE 
            df_state2 = pd.read_sql_query('select * from state_district_master', con=conn)
            state_dict3 = {}
            for i in df_state2.state.unique():
                t3 = request.form.get('state'+i)
                state_dict3[t3] = i
                
            if 'select option set from the list' in state_dict3.keys():
                del state_dict3['select option set from the list']


    # Organism
            # fetching organism id from table
            df_organism_id = pd.read_sql_query('select * from organisms', con=conn)
            org_int_id = df_organism_id['organism_id'].values
            # print("org int id",org_int_id)
            # converting integer into string
            org_id = list(map(str, org_int_id))
            # print("org id",org_id)
            # get value for option set mapping of organism name column
            dict_orgmap = {}
            for i in range(len(org_id)):
                # print("org i",i) 
                t = request.form.get('om' + str(i))
                # print("organisom")
                # print(t)
                dict_orgmap[t] = org_id[i]
                # print(dict_orgmap)
            if 'select option set from the list' in dict_orgmap.keys():
                del dict_orgmap['select option set from the list']

            s1 = ','.join(s1)
            s2 = ','.join(s2)
            s3 = ','.join(s3)
            s4 = ','.join(s4)
            s5 = ','.join(s5)
            s6 = ','.join(s6)
            s7 = ','.join(s7)
            sd = ','.join(sd)
            sm = ','.join(sm)
            sy = ','.join(sy)
            s8 = ','.join(s8)
            s9 = ','.join(s9)
            s10 = ','.join(s10)
            s11 = ','.join(s11)
            s12 = ','.join(s12)
            s13 = ','.join(s13)
            s14 = ','.join(s14)
            
            s17 = ','.join(s17)
            s18 = ','.join(s18)
            s19 = ','.join(s19)

            s24 = ','.join(s24)
            s25 = ','.join(s25)

            dict_o1 = {s1: 'Male', s2: 'Female', s3: 'Transgender'}
            dict_o2 = {s4: 'Urban', s5: 'Rural'}
            dict_o3 = {s6: 'India', s7: 'Outside India'}
            dict_o5 = {s15: 'ICU', s16: 'Ward'}
            dict_o6 = {s17: 'ICU', s18: 'Ward', s19: 'OPD'}
            dict_o4 = {s8: 'Biochemical test', s9: 'Proteomics', s10: 'Vitek-2', s11: 'MaldiToff', s12: '16s rRNA Sequencing',
                       s13: 'Whole Genome Sequencing', s14: 'Conventional identification by microscopy/phenotype'}
            dict_panel_id = {org_id[0]: '1', org_id[1]: '1', org_id[2]: '1', org_id[3]: '1', org_id[4]: '1', org_id[5]: '1', org_id[6]: '1', org_id[7]: '1', org_id[8]: '1', org_id[9]: '1', org_id[10]: '1', org_id[11]: '1', org_id[12]: '1', org_id[13]: '1', org_id[14]: '1', org_id[15]: '1', org_id[16]: '1', org_id[17]: '3', org_id[18]: '3', org_id[19]: '3', org_id[20]: '4', org_id[21]: '5', org_id[22]: '5', org_id[23]: '5', org_id[24]: '5', org_id[25]: '7', org_id[26]: '6', org_id[27]: '13', org_id[28]: '17', org_id[29]: '14', org_id[30]: '14', org_id[31]: '14', org_id[32]: '14', org_id[33]: '15', org_id[34]: '15', org_id[35]: '15', org_id[36]: '12', org_id[37]: '9', org_id[38]: '10', org_id[39]: '10', org_id[40]: '10', org_id[41]: '8', org_id[42]: '11', org_id[43]: 'Null', org_id[44]: '8', org_id[45]: '8', org_id[46]: '8', org_id[47]: '8', org_id[48]: '8', org_id[49]: '9', org_id[50]: '9', org_id[51]: '9', org_id[52]: '9', org_id[53]: '9', org_id[54]: '12', org_id[55]: 'Null',
                             org_id[56]: '14', org_id[57]: '18', org_id[58]: '25', org_id[59]: '25', org_id[60]: '26', org_id[61]: '25', org_id[62]: '27', org_id[63]: '25', org_id[64]: '20', org_id[65]: '20', org_id[66]: '23', org_id[67]: '20', org_id[68]: '20', org_id[69]: '20', org_id[70]: '20', org_id[71]: '20', org_id[72]: '20', org_id[73]: '20', org_id[74]: '20', org_id[75]: '20', org_id[76]: '20', org_id[77]: '20', org_id[78]: '20', org_id[79]: '20', org_id[80]: '20', org_id[81]: '20', org_id[82]: '24', org_id[83]: '21', org_id[84]: '22', org_id[85]: '20', org_id[86]: '20', org_id[87]: '20', org_id[88]: '20', org_id[89]: '20', org_id[90]: '20', org_id[91]: '20', org_id[92]: '20', org_id[93]: '20', org_id[94]: '20', org_id[95]: '20', org_id[96]: 'Null', org_id[97]: 'Null', org_id[98]: 'Null', org_id[99]: '20', org_id[100]: '20', org_id[101]: '20', org_id[102]: '20', org_id[103]: '20', org_id[104]: 'Null', org_id[105]: '5', org_id[106]: '6', org_id[107]: 'Null', org_id[108]: '20'}
            dict_optcombine = [dict_o1, dict_o2, dict_o3, dict_o4, dict_hospmap, dict_hospdept_id,
                               dict_o5, dict_samplemap, dict_sampletype_id, dict_orgmap, dict_panel_id, dict_o6]
            dict_o7 = {s24: 'Community Acquired Infection',
                       s25: 'Health Care Associated Infection'}
            dict_o8 = {s20: 'Malignancy', s21: 'Diabetes mellitus',
                       s22: 'Transplantation', s23: 'COVID19'}
            dict_o9 = {s26: 'Central line catheter', s27: 'Endotracheal tube',
                       s28: 'Mechanical ventilator', s29: 'Urinary catheter', s30: 'Other vascular access lines'}
            dict_age = {sd: 'd', sm: 'm', sy: 'y'}
            mergedopt = {}
            for x in dict_optcombine:
                mergedopt.update(x)

            with open(header_filename+'optionset_config_file.json', 'w') as outfile1:
                json.dump([dict_o1, dict_o2, dict_o3, dict_o4, dict_hospmap, dict_hospdept_id, dict_o5, dict_samplemap,
                          dict_sampletype_id, dict_orgmap, dict_panel_id, dict_o6, dict_o7, dict_o8, dict_o9,
                           dict_age], outfile1, indent=4)

            with open(header_filename+'combine_config_optionset_file.json', 'w') as outfile2:
                json.dump(mergedopt, outfile2, indent=4)
            with open(header_filename+'combine_config_file.json', "r") as p:
                data = p.read()
            dict_config_file1 = json.loads(data)
            
            with open(header_filename+'state_district.json', 'w') as outfile3:
                json.dump([state_dict3,state_dict2], outfile3, indent=4)
            
            if "antibiotic_id" not in dict_config_file1.values():
                conn.close()
                return render_template('download2.html', username=session['username'])
            else:
                conn.close()
                return render_template('download1.html', username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/download_one', methods=['GET', 'POST'])
def downloadFile_one():
    conn = db_connect()
    cur = conn.cursor()

    if 'loggedin' in session:
        userid = session["user_id"]
        header_filename = "dataconfig_file_"+str(userid)+"/"
        files = []
        file = [f for f in listdir(header_filename) if isfile(join(header_filename, f))]
        for i in file:
            if(i == "header_config_file.json" or i == "optionset_config_file.json" or i == 'antibiotic_config_file.json'):
                files.append(i)
        files = sorted(files, key=lambda s: s.casefold())

        if request.method == 'POST':
            if request.form['action'] == 'headers_map':
                path = header_filename+"header_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'antibiotic_map':
                path = header_filename+"antibiotic_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'optionset_map':
                path = header_filename+"optionset_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'Registered hospital':
                conn.close()
                return render_template(webpage.IMPORT_PAGE.value, files=files, username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/download_two', methods=['GET', 'POST'])
def downloadFile_two():
    conn = db_connect()
    cur = conn.cursor()

    if 'loggedin' in session:
        userid = session["user_id"]
        header_filename = "dataconfig_file_"+str(userid)+"/"
        files = []
        file = [f for f in listdir(header_filename) if isfile(join(header_filename, f))]
        for i in file:
            if(i == "header_config_file.json" or i == "optionset_config_file.json" or i == 'antibiotic_config_file.json'):
                files.append(i)
        files = sorted(files, key=lambda s: s.casefold())
        if request.method == 'POST':
            if request.form['action'] == 'headers_map':
                path = header_filename+"header_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'antibiotic_map':
                path = header_filename+"antibiotic_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'optionset_map':
                path = header_filename+"optionset_config_file.json"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'csv_transform':
                path = header_filename+"app_transpose_file.csv"
                return send_file(path, as_attachment=True)

            elif request.form['action'] == 'Registered hospital':
                conn.close()
                return render_template(webpage.IMPORT_PAGE.value, files=files, username=session['username'])
    conn.close()
    return render_template(webpage.LOGIN_PAGE.value)


@app.route('/login/import', methods=['GET', 'POST'])
def import_file():
    conn = db_connect()
    cur = conn.cursor()
    engine = create_engine(get_sqlalchemy_db_url())
    count_check = 0
    msg = ""
    if 'loggedin' in session:
        userid = session["user_id"]
        username = session["username"]
        header_filename = "dataconfig_file_"+str(userid)+"/"
        if request.method == 'POST':

            # try:
            f = request.files['csvf']
            f.save(header_filename+f.filename)
            a = f.filename
            dset = pd.read_csv(header_filename+a, encoding='unicode_escape', skipinitialspace=True)

            dset.columns = dset.columns.str.strip()
            b = request.form.get('header')
            with open(header_filename+b, "r") as p:
                data_header = p.read()
            dict_config_file = json.loads(data_header)
            d = request.form.get('optionset')
            with open(header_filename+d, "r") as r:
                data_option = r.read()
            dictopt = json.loads(data_option)

            # read configuration file of mapping of antibiotics

            dictt5 = (dict_config_file[3])
            e = request.form.get('antibiotic')
            with open(header_filename+e, "r") as s:
                data_anti = s.read()
            anti_dict = json.loads(data_anti)
            
            
            with open(header_filename+'combine_config_file.json', "r") as s:
                comb_dict = s.read()
            combine_dict = json.loads(comb_dict)
            
            
            # Handling the NUll values in mandatory fields
            mandatory_field = ['patient_reg_num', 'gender', 'country', 'state', 'district', 'admission_date', 'hospital_department', 'lab_sample_id', 'sample_type', 'collection_date', 'organism_id', 'identification_method']
            dset.reset_index()
            df_missing = pd.DataFrame(columns=dset.columns.values)
            df_passed = pd.DataFrame(columns=dset.columns.values)
            for index, row in dset.iterrows():
                ismiss = False
                
                for col in dset.columns.values:
                    if col in combine_dict and  combine_dict[col] in mandatory_field:
                        if str(row[col]).strip() == "nan":
                            ismiss = True
                            break
                
                if ismiss:
                    df_missing.loc[len(df_missing.index)] = row
                else:
                    df_passed.loc[len(df_passed.index)] = row
                    
            # df_missing.to_csv("missing_data.csv", index=False)
            df_missing.to_csv(f"static/missing_{username}.csv", index=False)
            df_passed.to_csv(header_filename+a, index=False)
            dset = pd.read_csv(header_filename+a, encoding='unicode_escape', skipinitialspace=True)
            dset.columns = dset.columns.str.strip()
            
            # handling the dates
            dates_to_change = ['date_of_birth', 'entry_date', 'collection_date', 'admission_date']
            for col in dset.columns.values:
                if col in combine_dict.keys() and combine_dict[col] in dates_to_change:
                    dset[col]  = handle_date(dset[col].tolist())
            dset.to_csv(header_filename+a, index=False)
            
            
            dset = pd.read_csv(header_filename+a, encoding='unicode_escape', skipinitialspace=True)
            dset.columns = dset.columns.str.strip()
            dset = dset.sort_values(by=dset.columns.values[0])
            
            # checking type of file either row based or column based
            if "antibiotic_id" not in dict_config_file[3].values():
                if "antibiotic_id" in dset.columns:
                    dictt5.update(antibiotic_id='antibiotic_id')
                    dictt5.update(value="value")
                else:
                    
                    list_second = []
                    for i in anti_dict.keys():
                        list_second.append(i)

                    count = 0
                    for e1 in dset.columns:
                        if e1 not in list_second:
                            count = count + 1
                        else:
                            break
                    b = count-1
                    antibiotic_start = e1
                    header_finish = dset.columns[b]

                    def isNan(val):
                        if isinstance(df2.loc[ind, col], float) and math.isnan(val):
                            return True
                        return False
                    df1 = dset.loc[:, :header_finish]
                    df2 = dset.loc[:, antibiotic_start:].T
                    req_df_col = df1.columns.values.tolist()
                    req_df_col.extend(["antibiotic_id", "value"])
                    req_df = pd.DataFrame(columns=req_df_col)
                    req_df["antibiotic_id"] = req_df["antibiotic_id"].astype(str)
                    req_df["value"] = req_df["value"].astype(str)
                    
                    org_index = 0
                    for col in df2.columns.values:
                        col_val_count = 0
                        for ind in df2.index:
                            if not isNan(df2.loc[ind, col]):
                                req_df.loc[len(req_df.index)] = df1.loc[col]
                                req_df.loc[len(req_df.index)-1, "antibiotic_id"] = ind
                                req_df.at[len(req_df.index)-1, "value"] = df2.loc[ind, col]
                                col_val_count += 1
                        if col_val_count == 0:
                            req_df.loc[len(req_df.index)] = df1.loc[col]
                            req_df.at[len(req_df.index)-1, "antibiotic_id"] = None
                            req_df.at[len(req_df.index)-1, "value"] = None

                    req_df.to_csv(header_filename + "app_transpose_file.csv", index=False)
                    dset = req_df
                    dictt5.update(antibiotic_id='antibiotic_id')
                    dictt5.update(value="value")
                    
            dictt2 = (dict_config_file[0])
            d2 = list((dictt2.keys()))

            # dictionary for table: hospital patient relation
            dictt3 = (dict_config_file[1])
            d3 = list((dictt3.keys()))

            # dictionary for table sample information
            dictt4 = (dict_config_file[2])
            d4 = list((dictt4.keys()))

            # dictionary for table susceptibility testing

            d5 = list((dictt5.keys()))

            dict_combine = {**dictt2, **dictt3, **dictt4, **dictt5}
            dc = list((dict_combine.keys()))
            # print(dict_combine)
            # print(dc)

            df2 = pd.DataFrame(dset)
            df2 = df2[d2]
            df3 = pd.DataFrame(dset)
            df3 = df3[d3]

            df4 = pd.DataFrame(dset)
            df4 = df4[d4]
            # df4["lab_sample_id"]=df4["lab_sample_id"].astype(str)

            df5 = pd.DataFrame(dset)
            df5 = df5[d5]

            df_combine = pd.DataFrame(dset)
            df_combine = df_combine[dc]
            df2.rename(columns=dictt2, inplace=True)
            df2["patient_reg_num"] = df2["patient_reg_num"].astype(str)
            for row in (df2.index):
                sample_p = df2["patient_reg_num"][row].encode("ascii")
                base64_bytes = base64.b64encode(sample_p)
                df2["patient_reg_num"][row] = base64_bytes.decode("ascii")
            df3.rename(columns=dictt3, inplace=True)
            df3["patient_reg_num"] = df3["patient_reg_num"].astype(str)
            if "location_type1" in df3.columns:
                df3.rename(columns={'location_type1': 'location_type'}, inplace=True)
            for row in (df3.index):
                sample_p = df3["patient_reg_num"][row].encode("ascii")
                base64_bytes = base64.b64encode(sample_p)
                df3["patient_reg_num"][row] = base64_bytes.decode("ascii")
            df4.rename(columns=dictt4, inplace=True)
            df4["lab_sample_id"] = df4["lab_sample_id"].astype(str)
            for row in (df4.index):
                sample_p = df4["lab_sample_id"][row].encode("ascii")
                base64_bytes = base64.b64encode(sample_p)
                df4["lab_sample_id"][row] = base64_bytes.decode("ascii")

            df5.rename(columns=dictt5, inplace=True)
            df_combine.rename(columns=dict_combine, inplace=True)
            df_combine["lab_sample_id"] = df_combine["lab_sample_id"].astype(str)
            df_combine["patient_reg_num"] = df_combine["patient_reg_num"].astype(str)
            for row in (df_combine.index):
                sample_p = df_combine["patient_reg_num"][row].encode("ascii")
                base64_bytes = base64.b64encode(sample_p)
                df_combine["patient_reg_num"][row] = base64_bytes.decode("ascii")
                sample_p = df_combine["lab_sample_id"][row].encode("ascii")
                base64_bytes = base64.b64encode(sample_p)
                df_combine["lab_sample_id"][row] = base64_bytes.decode("ascii")

            df2['location_type'] = df2['location_type'].str.capitalize()
            
            o1 = list(dictopt[0].keys())[0]
            o1 = list(str(o1).split(","))
            o1.pop(0)
            
            o2 = list(dictopt[0].keys())[1]
            o2 = list(str(o2).split(","))
            o2.pop(0)
            
            o3 = list(dictopt[0].keys())[2]
            o3 = list(str(o3).split(","))
            o3.pop(0)

            o4 = list(dictopt[1].keys())[0]
            o4 = list(str(o4).split(","))
            o4.pop(0)
            
            o5 = list(dictopt[1].keys())[1]
            o5 = list(str(o5).split(","))
            o5.pop(0)
            
            o6 = list(dictopt[2].keys())[0]
            o6 = list(str(o6).split(","))
            o6.pop(0)
            
            o7 = list(dictopt[2].keys())[1]
            o7 = list(str(o7).split(","))
            o7.pop(0)

            dictt_ident = dictopt[3]
            dictt_hospdept = dictopt[4]
            dictt_hospdept_id = dictopt[5]
            dictt_sampletype = dictopt[7]
            dictt_sampletype_id = dictopt[8]
            dictt_organism_id = dictopt[9]
            dictt_panel_id = dictopt[10]
            o11 = dictopt[11]
            o21 = dictopt[12]
            o31 = dictopt[13]
            o41 = dictopt[14]
            oage = dictopt[15]
            oprior = dictopt[6]
            
            # is prior_admission takes multiple entires as string? opd icu icu
            if "prior_admission" in df3.columns and not df3['prior_admission'].isnull().all():
                df3['prior_admission'] = df3['prior_admission'].str.capitalize()
                #df2['prior_admission'] = df2.prior_admission.replace(oprior)
                df2["prior_admission"] = df2["prior_admission"].astype(str)
                for i in df2.index:
                    li = df2['prior_admission'][i].split(",")
                    li = [each_string.capitalize() for each_string in li]
                    for ij in li:
                        if ij in oprior:
                            index = li.index(ij)
                            li[index] = oprior[ij]
                    listToStr = ",".join(li)
                    df2["prior_admission"][i] = listToStr
                    
            ################### FUNCTIONS TO MAP VAlUES ##################
                    
            def remove_unmapped_value(column, map):
                used = set()
                for key, value in map.items():
                    o_curr = key
                    o_curr = list(str(o_curr).split(","))
                    o_curr.pop(0)
                    for i in o_curr:
                        used.add(i)
                
                for i in range(len(column)):
                    val = (str(column[i])).strip()
                    if val not in used:
                        column[i] = ""
                return column
            
            def handle_replace_values(column, list, value):
                for i in range(len(column)):
                    val = column[i].strip()
                    if val in list:
                        column[i] = value
                return column
                    
            def handle_replace_mapping(column, map, remove=True):
                column = remove_unmapped_value(column, map)
                for key, value in map.items():
                    o_curr = key
                    o_curr = list(str(o_curr).split(","))
                    if remove:
                        o_curr.pop(0)
                    column = handle_replace_values(column, o_curr, value=value)
                return column
            
            ################### STATE AND DISTRICT MAPPING #################
            
            with open(header_filename+"state_district.json", "r") as s:
                state_dist = s.read()
            state_dist_map = json.loads(state_dist)
            
            # Mapping state values
            def replace_state_values(column, map):
                for i in range(len(column)):
                    dist = (str(column[i])).strip()
                    if dist in map.keys():
                        column[i] = map[dist]
                    else:
                        column[i] = ""
                return column
                
            df3['state'] = replace_state_values(df3['state'], state_dist_map[0])
            df_combine['state'] = replace_state_values(df_combine['state'], state_dist_map[0])
            
            # Mapping district values
            def replace_district_values(df, multi_map, state_map):
                for i in range(len(df['district'])):
                    final_dict = ""
                    curr_state = df['state'][i]
                    curr_dist = (df['district'][i]).strip()
                    if curr_state in state_map.values() and curr_state in multi_map.keys():
                        dist_map = multi_map[curr_state]
                        if curr_dist in dist_map.keys():
                            final_dict = dist_map[curr_dist]
                    df['district'][i] = final_dict
                return df
            
            df3 = replace_district_values(df3, state_dist_map[1], state_dist_map[0])  
            df_combine =  replace_district_values(df_combine, state_dist_map[1], state_dist_map[0])
            
            ################################################################
                    
            df2['location_type'] = df2['location_type'].str.capitalize()
            #df2["location_type"]= df2["location_type"].replace(regex= [o11], value = "ICU")
            #df2["location_type"]= df2["location_type"].replace(regex= [o21], value = "Ward")
            #df2["location_type"]= df2["location_type"].replace(regex= [o31], value = "OPD")
            df5['organism_id'] = df5['organism_id'].str.capitalize()
            df4['sample_type'] = df4['sample_type'].str.capitalize()
            df5['identification_method'] = df5['identification_method'].str.capitalize()
            df3['gender'] = df3['gender'].str.capitalize()
            
            # df3["gender"] = df3["gender"].replace(regex=o1, value="Male")
            # df3["gender"] = df3["gender"].replace(regex=o2, value="Female")
            # df3["gender"] = df3["gender"].replace(regex=o3, value="Transgender")
            df3["gender"] = remove_unmapped_value(df3["gender"], dictopt[0])
            df3["gender"] = handle_replace_values(df3["gender"], o1, value="Male")
            df3["gender"] = handle_replace_values(df3["gender"], o2, value="Female")
            df3["gender"] = handle_replace_values(df3["gender"], o3, value="Transgender")
            
            if "location_type" in df3.columns and not df3['location_type'].isnull().all():
                df3['location_type'] = df3['location_type'].str.capitalize()
                df3["location_type"] = remove_unmapped_value(df3["location_type"], dictopt[1])
                df3["location_type"] = handle_replace_values(df3["location_type"], o4, value="Urban")
                df3["location_type"] = handle_replace_values(df3["location_type"], o5, value="Rural")
                
                    
            df3['country'] = df3['country'].str.capitalize()
            df3["country"] = remove_unmapped_value(df3["country"], dictopt[2])
            df3["country"] = handle_replace_values(df3["country"], o6, value="India")
            df3["country"] = handle_replace_values(df3["country"], o7, value="Outside India")

            # Option Set Mapping for table : Susceptibility Testing
            # df2_rep = df2['location_type'].map(o11)
            df2_rep = handle_replace_mapping(df2['location_type'], o11)
            del df2['location_type']
            df2 = pd.concat([df2, df2_rep], axis=1)
            
            if "infection_type" in df2.columns and not df2['infection_type'].isnull().all():
                df2['infection_type'] = df2['infection_type'].str.capitalize()
                # df2_rep = df2['infection_type'].map(o21)
                df2_rep = handle_replace_mapping(df2['infection_type'], o21)
                del df2['infection_type']
                df2 = pd.concat([df2, df2_rep], axis=1)
                
            if "Unit" in df3.columns and not df3['Unit'].isnull().all():
                df3['Unit'] = df3["Unit"].str.capitalize()
                # dff_rep = df3['Unit'].map(oage)
                dff_rep = handle_replace_mapping(df3['Unit'], oage)
                del df3['Unit']
                df3 = pd.concat([df3, dff_rep], axis=1)

            if "comorbidity" in df2.columns and not df2['comorbidity'].isnull().all():
                df2['comorbidity'] = df2['comorbidity'].str.capitalize()
                #df2['comorbidity'] = df2.comorbidity.replace(o31)
                df2["comorbidity"] = df2["comorbidity"].astype(str)
                for i in df2.index:
                    li = df2['comorbidity'][i].split(",")
                    li = [each_string.capitalize() for each_string in li]
                    for ij in li:
                        if ij in o31:
                            index = li.index(ij)
                            li[index] = o31[ij]
                    listToStr = ",".join(li)
                    df2["comorbidity"][i] = listToStr
                #df2_rep = df2['comorbidity'].map(o31)
                #del df2['comorbidity']
                #df2 = pd.concat([df2, df2_rep], axis = 1)
            if "devices_used" in df2.columns and not df2['devices_used'].isnull().all():
                df2['devices_used'] = df2['devices_used'].str.capitalize()
                #df2_rep = df2['devices_used'].map(o41)
                #del df2['devices_used']
                #df2['devices_used'] = df2.devices_used.replace(o41)
                #df2 = pd.concat([df2, df2_rep], axis = 1)
                df2["devices_used"] = df2["devices_used"].astype(str)
                for i in df2.index:
                    li = df2['devices_used'][i].split(",")
                    li = [each_string.capitalize() for each_string in li]
                    for ij in li:
                        if ij in o41:
                            index = li.index(ij)
                            li[index] = o41[ij]
                    listToStr = ",".join(li)
                    df2["devices_used"][i] = listToStr
            # replace antibiotic names with antibiotic id
            df5_replace = df5['antibiotic_id'].map(anti_dict)
            del df5['antibiotic_id']
            df5_replace_antibiotic = pd.concat([df5, df5_replace], axis=1)

            # replace identification method column data based upon option set mapping
            # df5_rep = df5_replace_antibiotic['identification_method'].map(dictt_ident)
            
            
                
            df5_rep = handle_replace_mapping(df5_replace_antibiotic['identification_method'], dictt_ident)
            del df5_replace_antibiotic['identification_method']
            
            
            df5_replace_ident = pd.concat([df5_replace_antibiotic, df5_rep], axis=1)

            # replace organism id column data based upon option set mapping
            df5_reporg = df5_replace_ident['organism_id'].map(dictt_organism_id)
            del df5_replace_ident['organism_id']
            df5_replace_final = pd.concat([df5_replace_ident, df5_reporg], axis=1)
            # creation of panel_id based upon organism_id
            df5_replace_final['panel_id'] = df5_replace_final['organism_id'].map(dictt_panel_id)

            # Option Set Mapping for table : Sample Information

            # creation of sample type id based upon sample type
            df4['sample_type_id'] = df4['sample_type'].map(dictt_sampletype_id)
            
            # replace sample type column data based upon option set mapping
            df4_replace = df4['sample_type'].map(dictt_sampletype)
            
            del df4['sample_type']
            df4_replace_sampletype = pd.concat([df4, df4_replace], axis=1)
            df5_replace_final['sample_type'] = df4_replace_sampletype["sample_type"]
            # Option Set Mapping for table : Hospital Patient Relation

            # creation of hospital department id based upon hospital department name
            df2['hospital_department_id'] = df2['hospital_department'].map(dictt_hospdept_id)
            # df2['hospital_department_id'] = handle_replace_mapping(df2['hospital_department'], dictt_hospdept_id, remove=False)

            # replace sample type column data based upon option set mapping
            df2['hospital_department'] = df2['hospital_department'].str.capitalize()
            df2_replace = df2['hospital_department'].map(dictt_hospdept)
            # df2_replace = handle_replace_mapping(df2['hospital_department'], dictt_hospdept, remove=False)
            
            del df2['hospital_department']
            df2_replace_hospdept = pd.concat([df2, df2_replace], axis=1)
            hosp_id = str(session["laboratory"])
            conn.close()
            conn = db_connect()
            cur = conn.cursor()
            # engine = create_engine(get_sqlalchemy_db_url())
            range0 = cur.execute('SELECT hospital_id from diagnostic_laboratory_information where lab_id= %s' % "'"+hosp_id+"'")

            range0 = str(cur.fetchall())
            temp = re.findall(r'\d+', range0)
            res = list(map(int, temp))
            res1 = ''.join(map(str, res))
            query = "select hospital_patient_relation.hospital_id, hospital_patient_relation.patient_reg_num,sample_information.lab_sample_id,susceptibility_testing.antibiotic_id,susceptibility_testing.organism_id from hospital_patient_relation inner join sample_information on hospital_patient_relation.hospital_patient_rel_id=sample_information.hospital_patient_rel_id inner join susceptibility_testing on sample_information.sample_id=susceptibility_testing.sample_id where hospital_patient_relation.hospital_id=%s and hospital_patient_relation.patient_reg_num=%s and sample_information.lab_sample_id=%s and susceptibility_testing.antibiotic_id=%s and susceptibility_testing.organism_id=%s"
            df_combine["antibiotic_id"] = df5_replace_final["antibiotic_id"]
            equalitydata1 = pd.DataFrame(columns=["hospital_id", "patient_reg_num", "lab_sample_id", "antibiotic_id", "organism_id"])
            equalitydata1["antibiotic_id"] = df5_replace_final["antibiotic_id"]
            equalitydata1["organism_id"] = df5_replace_final["organism_id"]
            equalitydata1["patient_reg_num"] = df_combine["patient_reg_num"]
            equalitydata1["lab_sample_id"] = df_combine["lab_sample_id"]
            equalitydata1["hospital_id"].fillna(res1, inplace=True)
            equalitydata1['hospital_department'] = df2_replace_hospdept['hospital_department']
            equalitydata1['sample_type'] = df4_replace_sampletype['sample_type']
            equalitydata1["district"] = df3["district"]
            equalitydata1["location_type"] = df2_replace_hospdept["location_type"]
            equalitydata1["country"] = df3["country"]
            equalitydata1["identification_method"] = df5_replace_final["identification_method"]
            equalitydata1["gender"] = df3["gender"]
            equalitydata1["state"] = df3["state"]
            equalitydata1["admission_date"] = df2_replace_hospdept["admission_date"]
            equalitydata1["collection_date"] = df4_replace_sampletype["collection_date"] 
            inv1 = (equalitydata1[equalitydata1.isna().any(axis=1)])
            
            inv_index = set()
            for row in (inv1.index):
                sample_p = inv1["patient_reg_num"][row].encode("ascii")
                base64_bytes = base64.b64decode(sample_p)
                inv1["patient_reg_num"][row] = base64_bytes.decode("ascii")
                sample_p = inv1["lab_sample_id"][row].encode("ascii")
                base64_bytes = base64.b64decode(sample_p)
                inv1["lab_sample_id"][row] = base64_bytes.decode("ascii")
            
            # OLD INVALID CSV
            inv1.to_csv(f"static/invalid_{username}.csv", index=False)            
            
            invalid = 0
            index_to_drop = []
            insert = 0
            duplicate = 0
            datalength = len(equalitydata1)
            rows_with_nan = [
                index for index, row in equalitydata1.iterrows() if row.isnull().any()]
            equalitydata1 = equalitydata1.dropna()
            equalitydata1 = equalitydata1.reset_index(drop=True)
            if((len(equalitydata1)) == 0):
                invalid = datalength
                column1 = dset.columns
                export1 = pd.DataFrame(columns=column1)
                export1.to_csv(f"static/valid_{username}.csv", index=False)

            else:
                index_to_drop = []
                invalid = datalength - len(equalitydata1)
                for i in equalitydata1.index:
                    list1 = (equalitydata1.iloc[i])
                    a = (str(list1[0]), str(list1[1]), str(list1[2]), str(list1[3]), str(list1[4]))
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    equalitycheck = cur.execute(query, a)
                    equalitydata = cur.fetchone()
                    if (equalitycheck != 0):
                        duplicate = duplicate+1
                        index_to_drop.append(i)
                    else:
                        insert = insert+1
                if(duplicate != len(equalitydata1)):

                    df2 = df2.drop(rows_with_nan)
                    df2 = df2.reset_index(drop=True)
                    df3 = df3.drop(rows_with_nan)
                    df3 = df3.reset_index(drop=True)
                    df4 = df4.drop(rows_with_nan)
                    df4 = df4.reset_index(drop=True)
                    df5 = df5.drop(rows_with_nan)
                    df5 = df5.reset_index(drop=True)
                    df_combine = df_combine.drop(rows_with_nan)
                    df_combine = df_combine.reset_index(drop=True)
                    df5_replace_final = df5_replace_final.drop(rows_with_nan)
                    df5_replace_final = df5_replace_final.reset_index(drop=True)
                    df2_replace_hospdept = df2_replace_hospdept.drop(rows_with_nan)
                    df2_replace_hospdept = df2_replace_hospdept.reset_index(drop=True)
                    df4_replace_sampletype = df4_replace_sampletype.drop(rows_with_nan)
                    df4_replace_sampletype = df4_replace_sampletype.reset_index(drop=True)

                    df2 = df2.drop(index_to_drop)
                    df2 = df2.reset_index(drop=True)
                    df3 = df3.drop(index_to_drop)
                    df3 = df3.reset_index(drop=True)
                    df4 = df4.drop(index_to_drop)
                    df4 = df4.reset_index(drop=True)
                    df5 = df5.drop(index_to_drop)
                    df5 = df5.reset_index(drop=True)
                    df_combine = df_combine.drop(index_to_drop)
                    df_combine = df_combine.reset_index(drop=True)
                    df5_replace_final = df5_replace_final.drop(index_to_drop)
                    df5_replace_final = df5_replace_final.reset_index(drop=True)
                    df2_replace_hospdept = df2_replace_hospdept.drop(index_to_drop)
                    df2_replace_hospdept = df2_replace_hospdept.reset_index(drop=True)
                    df4_replace_sampletype = df4_replace_sampletype.drop(index_to_drop)
                    df4_replace_sampletype = df4_replace_sampletype.reset_index(drop=True)

                    newdata = 0
                    olddata = 0
                    follow_updata = pd.DataFrame(columns=['patient_id', 'patient_reg_num', 'hospital_patient_rel_id', 'sample_id', 'lab_sample_id'])
                    for i, row in equalitydata1.iterrows():
                        query1 = "SELECT patient_information.patient_id,patient_information.patient_reg_num,hospital_patient_relation.hospital_patient_rel_id,sample_information.sample_id,sample_information.lab_sample_id from patient_information INNER JOIN hospital_patient_relation on patient_information.patient_id=hospital_patient_relation.patient_id inner join sample_information on sample_information.hospital_patient_rel_id=hospital_patient_relation.hospital_patient_rel_id WHERE hospital_patient_relation.patient_reg_num=%s and hospital_patient_relation.hospital_id=%s and sample_information.lab_sample_id=%s"
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        data_check = cur.execute(query1, (row['patient_reg_num'], row['hospital_id'], row['lab_sample_id']))
                        data_check1 = cur.fetchall()
                        listing = []
                        if not data_check1:
                            newdata = newdata + 1
                        else:
                            listing.append(data_check1[0])
                            follow_updata.append(listing)
                            olddata = olddata+1
                            listing = []
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    m1 = cur.execute('SELECT max(patient_id) from patient_information')
                    m1 = cur.fetchone()
                    maxt1_before = int(0 if m1[0] is None else m1[0])+1

                    m3 = cur.execute('SELECT max(hospital_patient_rel_id) from hospital_patient_relation')
                    m3 = cur.fetchone()
                    maxt2_before = int(0 if m3[0] is None else m3[0])+1

                    m5 = cur.execute('SELECT max(sample_id) from sample_information')
                    m5 = cur.fetchone()
                    maxt3_before = int(0 if m5[0] is None else m5[0])+1

                    m5a = cur.execute('SELECT max(amr_id) from susceptibility_testing')
                    m5a = cur.fetchone()
                    maxt3a_before = int(0 if m5a[0] is None else m5a[0])+1

                    # for entry date
                    today = date.today()
                    d1 = today.strftime("%Y-%m-%d")
                    # sending data to table 1
                    # drop rows with duplicate patient_reg_number
                    df_patientinfo = df3.drop_duplicates(subset=['patient_reg_num'], keep='first')

                    row_count_patientinfo = df_patientinfo.shape[0]

                    df_patientinfo.reset_index(inplace=True, drop=True)
                    df_patientinfo_final = df_patientinfo
                    df_hpr_drop = df2_replace_hospdept.drop_duplicates(subset=['patient_reg_num'], keep='first')
                    df_hpr_drop.reset_index(inplace=True, drop=True)
                    df_hpr_drop['admission_date'] = pd.to_datetime(df_hpr_drop['admission_date'], dayfirst=True)
                    if "date_of_birth" not in dict_config_file[1].values() and "Unit" in dict_config_file[1].values():
                        df_patientinfo_final['date_of_birth'] = np.nan
                        df_patientinfo_final['Unit'].str.capitalize()
                        df_hpr_drop['admission_date'] = pd.to_datetime(df_hpr_drop['admission_date'], dayfirst=True)
                        df_patientinfo_final["admission_date"] = df_hpr_drop["admission_date"]


                        def myfunc(age, pclass, start_date1, ay):
                            if pd.isnull(age) and (pclass == "y"):
                                age = start_date1 - timedelta(days=int(ay)*365)

                            elif pd.isnull(age) and (pclass == "m"):
                                age = start_date1 - \
                                    timedelta(days=int(ay)*365/12)

                            elif pd.isnull(age) and (pclass == "d"):
                                age = start_date1 - timedelta(days=int(ay))
                            else:
                                print("Hllll")
                            return age
                        df_patientinfo_final['date_of_birth'] = df_patientinfo_final.apply(lambda x: myfunc(
                            x['date_of_birth'], x['Unit'], x['admission_date'], x["age_year"]), axis=1)
                        df_patientinfo_final["age_year"] = np.nan
                        df_patientinfo_final = df_patientinfo_final.drop(['admission_date'], axis=1)

                    elif "date_of_birth" not in dict_config_file[1].values() and "Unit" not in dict_config_file[1].values():
                        df_hpr_drop['year'] = df_hpr_drop['admission_date'].dt.year
                        df_hpr_drop['month'] = df_hpr_drop['admission_date'].dt.month
                        df_hpr_drop['day'] = df_hpr_drop['admission_date'].dt.day

                        df_hpr_drop['year'] = df_hpr_drop['year'] - \
                            df_patientinfo_final['age_year']

                        df_patientinfo_final['date_of_birth'] = pd.to_datetime(
                            df_hpr_drop[['year', 'month', 'day']], dayfirst=True).dt.date
                    else:
                        df_patientinfo_final['date_of_birth'] = pd.to_datetime(
                            df_patientinfo_final['date_of_birth'], dayfirst=True).dt.date
                    if "age_year" in dict_config_file[1].values():
                        df_patientinfo_final["age_year"] = np.nan
                    df_patientinfo_final['entry_date'] = d1
                    # sending data to table 2
                    df_patientinfo_final["entry_date"] = pd.to_datetime('today')
                    df_patientinfo_final['patient_id'] = 0
                    duplicate_row = []
                    df_patientinfo_final["sent_through"] = "DIA"
                    for i, row in df_patientinfo_final.iterrows():
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT * FROM hospital_patient_relation WHERE patient_reg_num=%s and hospital_id=%s", (row['patient_reg_num'], res1))
                        data = cur.fetchone()
                        if not data:
                            count_check = 0
                        else:
                            duplicate_row.append(i)
                            count_check = 1
                    df_patientinfo_final = df_patientinfo_final.drop(duplicate_row)
                    df_patientinfo_final = df_patientinfo_final.reset_index(drop=True)
                    for i in range(len(df_patientinfo_final)):
                        df_patientinfo_final['patient_id'][i] = maxt1_before + i
                    if 'Unit' in df_patientinfo_final:
                        df_patientinfo_final = df_patientinfo_final.drop(['Unit'], axis=1)
                    if(df_patientinfo_final.empty == False):
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()

                        engine = create_engine(get_sqlalchemy_db_url())
                        df_patientinfo_final.to_sql(
                            name='patient_information', con=engine, if_exists='append', index=False)
                        conn.commit()
                # fetching data for foreign key patient_id and hospital_id
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    m2 = cur.execute('SELECT max(patient_id) from patient_information')
                    m2 = cur.fetchone()
                    maxt1_after = m2[0]

                # for repetition of hospital id
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    hosp_id = str(session["laboratory"])
                    range0 = cur.execute('SELECT hospital_id from diagnostic_laboratory_information where lab_id= %s' % "'"+hosp_id+"'")
                    range0 = cur.fetchall()
                    col_range0 = []
                    for elt0 in cur.description:
                        col_range0.append(elt0[0])

                    df_range0 = pd.DataFrame(range0, columns=col_range0)

                    rowcount = df2.shape[0]
                    df_rep = pd.concat([df_range0]*rowcount, ignore_index=True)
                    df_complete = pd.concat([df_rep, df2, df3, df4, df5], axis=1)
                    df_complete = df_complete.T.drop_duplicates().T

                    # generate unique amr id
                    username1 = session['username']
                    export1 = dset
                    export1 = export1.drop(rows_with_nan)
                    export1 = export1.reset_index(drop=True)
                    export1 = export1.drop(index_to_drop)
                    export1 = export1.reset_index(drop=True)
                    df_complete = df_complete.assign(id=df_complete.groupby(['patient_reg_num', 'hospital_id', 'lab_sample_id', 'organism_id']).ngroup())
                    df_complete['amr_id'] = df_complete['id']+maxt3a_before
                    export1.insert(loc=0, column='amr_id',value=df_complete["amr_id"])
                    export1.to_csv(f"static/valid_{username1}.csv", index=False)
                    a = df_complete[df_complete.index.duplicated()]
                    df5_replace_final['amr_id'] = df_complete['amr_id']
                    df5_replace_final['lab_sample_id'] = df_complete['lab_sample_id']
                    reqList = []
                    for val in (df_combine.patient_reg_num.unique()):
                        reqList.append(df_combine.loc[df_combine['patient_reg_num'] == val, 'patient_reg_num'].tolist()[0])
                    reqListnew = []
                    reqList = df_combine.patient_reg_num.unique()
                    for vale in reqList:
                        vale = str(vale)
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        myquery = cur.execute('select patient_id,patient_reg_num from patient_information where patient_reg_num = %s' % "'"+vale+"'")
                        myquery = cur.fetchall()
                        reqListnew.append(myquery[0])
                    df_range1 = pd.DataFrame(reqListnew, columns=['patient_id', 'patient_reg_num'])
                    row_count = df_range1.shape[0]

                    df_hpr_drop = df2_replace_hospdept.drop_duplicates(subset=['patient_reg_num'], keep='first')
                    df_hpr_drop['hospital_id'] = res1
                    df_hpr_drop.reset_index(inplace=True, drop=True)
                    df_hpr = df_range1
                    df_hpr["patient_reg_num"] = df_hpr["patient_reg_num"].astype(str)
                    df_hpr_drop["patient_reg_num"] = df_hpr_drop["patient_reg_num"].astype(str)
                    df_hpr_combine = pd.merge(df_hpr, df_hpr_drop, on="patient_reg_num")
                    # sending data to table 3
                    df_hpr_combine['admission_date'] = pd.to_datetime(df_hpr_combine['admission_date'], dayfirst=True).dt.date
                    df_hpr_combine['hospital_patient_rel_id'] = 0
                    df_hpr_combine['entry_date'] = pd.to_datetime("today")
                    if "infection_type" not in dict_config_file[0]:
                        df_hpr_combine["infection_type"] = "Not Known"
                    else:
                        df_hpr_combine["infection_type"] = df_hpr_combine['infection_type'].fillna(
                            "Not Known")
                    duplicate_row = []
                    df_hpr_combine["sent_through"] = "DIA"
                    for i, row in df_hpr_combine.iterrows():
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        cur.execute("SELECT * FROM hospital_patient_relation WHERE patient_reg_num=%s and hospital_id=%s and hospital_department=%s",
                                    (row['patient_reg_num'], row['hospital_id'], row['hospital_department']))
                        data = cur.fetchone()
                        if not data:
                            count_check = 0
                        else:
                            duplicate_row.append(i)
                            count_check = 1
                    df_hpr_combine = df_hpr_combine.drop(duplicate_row)
                    df_hpr_combine = df_hpr_combine.reset_index(drop=True)
                    for i in range(len(df_hpr_combine)):
                        df_hpr_combine['hospital_patient_rel_id'][i] = maxt2_before+i
                    if(df_hpr_combine.empty == False):
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        df_hpr_combine.to_sql(name='hospital_patient_relation', con=engine, if_exists='append', index=False)
                        conn.commit()

                    # fetching data for foreign key hospital_patient_rel_id
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    m4 = cur.execute(
                        'SELECT max(hospital_patient_rel_id) from hospital_patient_relation')
                    m4 = cur.fetchone()
                    maxt2_after = m4[0]

                    range2 = cur.execute(
                        'SELECT hospital_patient_rel_id from hospital_patient_relation where hospital_patient_rel_id BETWEEN %s and %s', (maxt2_before, maxt2_after))
                    range2 = cur.fetchall()
                    col_range2 = []
                    for elt2 in cur.description:
                        col_range2.append(elt2[0])

                    df_range2 = pd.DataFrame(range2, columns=col_range2)
                    reqList = []
                    for val in df_combine.lab_sample_id.unique():
                        reqList.append(
                            df_combine.loc[df_combine['lab_sample_id'] == val, 'patient_reg_num'].tolist()[0])
                    reqListnew = []
                    for vale in reqList:
                        vale = str(vale)
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        myquery = cur.execute(
                            'select max(hospital_patient_rel_id) from hospital_patient_relation where patient_reg_num = %s' % "'"+vale+"'")
                        myquery = cur.fetchall()
                        reqListnew.append(myquery)
                    reqList_hpr_id = []
                    for countt in range(0, len(reqListnew)):
                        a = reqListnew[countt][0][0]
                        reqList_hpr_id.append(a)

                    df_hpr_id = pd.DataFrame(reqList_hpr_id, columns=['hospital_patient_rel_id'])
                    df_sample_drop = df4_replace_sampletype.drop_duplicates(subset=['lab_sample_id'], keep='first')
                    df_sample_drop.reset_index(inplace=True, drop=True)
                    df_sampinfo = pd.concat([df_hpr_id, df_sample_drop], axis=1)
                    row_count_sampinfo = df_sampinfo.shape[0]

                    df_sampinfo_final = df_sampinfo

                    # sending data to table4
                    df_sampinfo_final['collection_date'] = pd.to_datetime(df_sampinfo_final['collection_date'], dayfirst=True).dt.date
                    df_sampinfo_final["sample_id"] = 0
                    df_sampinfo_final['entry_date'] = pd.to_datetime('today')
                    df_sampinfo_final['sample_type_id'] = df_sampinfo_final['sample_type_id'].fillna(0)
                    df_sampinfo_final['lab_id'] = session['laboratory']
                    df_sampinfo_final["sent_through"] = "DIA"
                    duplicate_row = []
                    for i, row in df_sampinfo_final.iterrows():
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        cur.execute("SELECT * FROM sample_information WHERE hospital_patient_rel_id=%s and lab_sample_id=%s",
                                    (row['hospital_patient_rel_id'], row['lab_sample_id']))
                        data = cur.fetchone()
                        if not data:
                            count_check = 0
                        else:
                            duplicate_row.append(i)
                            count_check = 1
                    df_sampinfo_final = df_sampinfo_final.drop(duplicate_row)
                    df_sampinfo_final = df_sampinfo_final.reset_index(drop=True)
                    for i in range(len(df_sampinfo_final)):
                        df_sampinfo_final['sample_id'][i] = maxt3_before + i
                    if df_sampinfo_final.empty == False:
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        # count_check=0
                        engine = create_engine(get_sqlalchemy_db_url())
                        df_sampinfo_final.to_sql(name='sample_information', con=engine, if_exists='append', index=False)
                        conn.commit()

                # fetching data for foreign key sample_id
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    m6 = cur.execute('SELECT max(sample_id) from sample_information')
                    m6 = cur.fetchone()
                    maxt3_after = m6[0]
                    df_combine["sample_type"] = df4_replace_sampletype["sample_type"]
                    reqList = df_combine.drop_duplicates(subset=["lab_sample_id", "sample_type"])
                    reqList = reqList.reset_index(drop=True)
                    hosp_id = str(res1)
                    reqListnew = []
                    for vale in reqList.index:
                        conn.close()
                        conn = db_connect()
                        cur = conn.cursor()
                        engine = create_engine(get_sqlalchemy_db_url())
                        myquery = cur.execute("SELECT * FROM sample_information inner JOIN hospital_patient_relation ON sample_information.hospital_patient_rel_id=hospital_patient_relation.hospital_patient_rel_id WHERE sample_information.sample_type=%s AND  hospital_patient_relation.patient_reg_num=%s AND sample_information.lab_sample_id=%s AND hospital_patient_relation.hospital_id=%s",
                                              (reqList["sample_type"][vale], reqList["patient_reg_num"][vale], reqList["lab_sample_id"][vale], hosp_id))
                        myquery = cur.fetchall()
                        myquery = list(myquery)
                        if(len(myquery) != 0):
                            wer = (pd.DataFrame(myquery))
                        else:
                            continue

                        reqListnew.append([wer.iloc[-1][1], wer.iloc[-1][0]])

                    df_range3 = pd.DataFrame(reqListnew, columns=['lab_sample_id', 'sample_id'])
                    row_count = df_range3.shape[0]

                # creating dataframe of repeated sample_id
                    df_repeated_sample_id = df_range3

                    df_sustesting = pd.merge(df_repeated_sample_id, df5_replace_final, on='lab_sample_id')
                    df_sustesting1 = df_sustesting.drop_duplicates()
                    df_sustestingf = df_sustesting1.drop(["lab_sample_id"], axis=1)
                    row_count_sustesting = df_sustestingf.shape[0]

                    df_sustesting_final = df_sustestingf
                    df_sustesting_mod = df_sustesting_final.dropna(how='all', subset=['organism_id', 'antibiotic_id', 'value'])
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    m5as = cur.execute('SELECT max(susceptibility_id) from susceptibility_testing')
                    m5as = cur.fetchone()
                    maxt5as_before = int(0 if m5as[0] is None else m5as[0])+1
                    df_sustesting_mod['entry_date'] = pd.to_datetime('today')
                    df_sustesting_mod['value'] = df_sustesting_mod['value'].fillna(0)
                    df_sustesting_mod['susceptibility_id'] = 0
                    df_sustesting_mod["sent_through"] = "DIA"
                    df_sustesting_mod["submitted_by"] = session["user_names"]
                    df_sustesting_mod["submitted_by_id"] = session["user_id"]
                    for i in range(len(df_sustesting_mod)):
                        df_sustesting_mod["susceptibility_id"][i] = maxt5as_before + i
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    cur.execute('SELECT * from antibiotic_panel;')
                    data_panel = cur.fetchall()
                    col2 = []
                    for ab2 in cur.description:
                        col2.append(ab2[0])
                    df_panel = pd.DataFrame(data_panel, columns=col2)
                    df_sustesting_mod = df_sustesting_mod.drop(
                        ['panel_id'], axis=1)
                    df_new = df_sustesting_mod[['organism_id', 'sample_type']]
                    df_new = df_new.drop_duplicates(subset=["sample_type", "organism_id"], keep='first')
                    df_new['panel_id'] = np.nan
                    for i in df_new.index:
                        for j in df_panel.index:
                            anti = df_panel['antibiotic_ids'][j].split(",")
                            org = df_panel['organism_ids'][j].split(",")
                            try:
                                org.remove('nan')
                            except:
                                print("hiiii")
                            try:
                                anti.remove('nan')
                            except:
                                print("hrllo")
                            anti = [int(i) for i in anti]
                            org = [int(i) for i in org]
                            if int(float(df_new["organism_id"][i])) in org:
                                if(df_panel['panel_id'][j] == 1 or df_panel['panel_id'][j] == '1' or df_panel['panel_id'][j] == 2 or df_panel['panel_id'][j] == '2'):
                                    if(df_new['sample_type'][i] == 'Urine'):
                                        df_new['panel_id'][i] = 1
                                    else:
                                        df_new['panel_id'][i] = 2
                                elif(df_panel['panel_id'][j] == 15 or df_panel['panel_id'][j] == '15' or df_panel['panel_id'][j] == 19 or df_panel['panel_id'][j] == '19'):
                                    if(df_new['sample_type'][i] == 'Urine'):
                                        df_new['panel_id'][i] = 15
                                    else:
                                        df_new['panel_id'][i] = 19
                                else:
                                    df_new['panel_id'][i] = df_panel['panel_id'][j]
                    df_sustesting_mod = pd.merge(df_sustesting_mod, df_new, on=[
                                                 'organism_id', 'sample_type'], how='left', suffixes=(False, False))
                    df_sustesting_mod = df_sustesting_mod.drop(
                        ['sample_type'], axis=1)
                    conn.close()
                    conn = db_connect()
                    cur = conn.cursor()
                    engine = create_engine(get_sqlalchemy_db_url())
                    df_sustesting_mod.to_sql(
                        name='susceptibility_testing', con=engine, if_exists='append', index=False)
                    conn.commit()
            dup_data = equalitydata1.iloc[index_to_drop]
            for row in (dup_data.index):
                sample_p = dup_data["patient_reg_num"][row].encode("ascii")
                base64_bytes = base64.b64decode(sample_p)
                dup_data["patient_reg_num"][row] = base64_bytes.decode("ascii")
                sample_p = dup_data["lab_sample_id"][row].encode("ascii")
                base64_bytes = base64.b64decode(sample_p)
                dup_data["lab_sample_id"][row] = base64_bytes.decode("ascii")
            dup_data.to_csv(f"static/duplicate_{username}.csv", index=False)
            conn.close()
            return render_template('last.html', username=session['username'], insert=insert, duplicate=duplicate, invalid=invalid)


if __name__ == "__main__":
    #Timer(1, open_browser).start();
    app.run(host='0.0.0.0', port=8081, debug=True)
    # app.run(host='0.0.0.0')
