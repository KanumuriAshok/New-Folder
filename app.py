import os
from flask import Flask, flash, request, redirect, url_for
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine
import geopandas as gpd
import zipfile
import subprocess
from flask_cors import CORS
from geo.Geoserver import Geoserver
import pandas as pd
import psycopg2
import time
user = "postgres"
password = "postgres"
host = "localhost"
port = 5432
database = "UAT"

#connections = "psql -U postgres -c "SELECT pid, (SELECT pg_terminate_backend(pid)) as killed from pg_stat_activity WHERE datname = 'my_database_to_alter';"
# Initialize the library
#postgres to shp - [pgsql2shp -f "C:\Users\jyothy\Desktop\New folder\geoflask\data output\cluster_output" -h localhost -u postgres -P postgres UAT "SELECT * FROM sample_flask_cluster_output"]
#reference - https://gis.stackexchange.com/questions/55206/how-can-i-get-a-shapefile-from-a-postgis-query


os.environ['PATH'] += r';C:\Program Files\PostgreSQL\14\bin'
# http://www.postgresql.org/docs/current/static/libpq-envars.html
os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGUSER'] = 'postgres'
os.environ['PGPASSWORD'] = 'postgres'
os.environ['PGDATABASE'] = 'UAT'

UPLOAD_FOLDER = r'C:\Users\jyothy\Desktop\New folder\geoflask\data_input'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif','shp','zip','cpg','dbf','qmd','shx','prj'}

app = Flask(__name__)
CORS(app, resource={r"*" : {"origins":"*"}})
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
GLOBAL_WORKSPACENAME = ''

def terminate_connections():
    try:
        #'shp2pgsql "' + shapefile_list[0] + '" node_boundary | psql '
        #sql = "SELECT pid, (SELECT pg_terminate_backend(pid)) as killed from pg_stat_activity WHERE datname = 'UAT';"
        subprocess.run(f'python test_postgres.py')
        return True
    except:
        return False

def generate_random_workspace():
    curr_time = int(time.time())
    workspaceName = "NODE_" + str(curr_time)
    set_workspace(workspaceName)
    return workspaceName

def set_workspace(data):
    GLOBAL_WORKSPACENAME = data

def get_workspace():
    return GLOBAL_WORKSPACENAME

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
def remove_table(table_name):
    try:
        conn = psycopg2.connect(
           database=database, user=user, password=password, host=host, port= port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = f'DROP TABLE IF EXISTS {table_name}' #keeping constant for now
        cursor.execute(sql)
        conn.commit()
        conn.close()
        terminate_connections()
        return {
            "status":"PASS"
        }
        
    except Exception as msg:
        try:
        
            conn.close()
            terminate_connections()
        except:
            pass
        return  {
            "status":"PASS",
            "error":msg
        }
def generate_outliers():
    try:
        conn = psycopg2.connect(
           database=database, user=user, password=password, host=host, port= port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = f'DROP TABLE IF EXISTS sample_flask_outlier_output' #keeping constant for now
        cursor.execute(sql)
        conn.commit()
        
        sql = 'select cluster_id,sum(pon_homes) as tot_pon into sample_flask_outlier_output from sample_flask_cluster_output group by cluster_id having sum(pon_homes) > 24 or sum(pon_homes) < 3'
        cursor.execute(sql)
        conn.commit()

        sql = f'DROP TABLE IF EXISTS sample_flask_joined_cluster_id'  # keeping constant for now
        cursor.execute(sql)
        conn.commit()

        sql = '''
        SELECT  s.gid ,
            s.identifier ,
            s.pon_homes,
            s.p2p_homes,
            s.loc_issue,
            s.loc_desc,
            s.qc_check,
            s.include,
            s.exc_reason,
            s.connection,
            s.cfh_type,
            s.prop_type ,
            s.forced_cbl ,
            s.uprn ,
            s.category,
            s.cat_type ,
            s.org_name,
            s.bld_name,
            s.bld_name2 ,
            s.bld_num ,
            s.streetname ,
            s.postcode,
            s.pon_m_rev,
            s.p2p_m_rev ,
            s.rfs_status,
            s.bldg_id,
            s.gistool_id,
            s.hubname ,
            s.indexed ,
            s.cluster_id ,
            s.geom,
            o.tot_pon
        into sample_flask_joined_cluster_id FROM sample_flask_cluster_output s
        INNER JOIN sample_flask_outlier_output o
        ON s.cluster_id = o.cluster_id group by s.cluster_id,s.gid
        ,o.cluster_id,o.tot_pon;
        '''
        cursor.execute(sql)
        conn.commit()
        #Closing the connection
        conn.close()
        terminate_connections()
        return {
            "Status":"PASS"
        }
    except Exception as msg:
        try:
            conn.close()
            terminate_connections()
        except:
            pass
        return {
            "Status":"FAILED",
            "Error":msg
        }

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part

        for file in request.files.getlist('landbndry'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "landbndry"
            file.filename = ".".join(ls_name)
            print("Land Boundary File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        for file in request.files.getlist('demandpoints'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):
            
            ls_name = file.filename.split(".")
            ls_name[0] = "demandpoints"
            file.filename = ".".join(ls_name)
            print("Demandpoints file",file.filename)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('uploaded_file',
                                filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <h1>Land Boundary</h1>
      <input type=file name=landbndry multiple>
      <h1>Demand Points</h1>
      <input type=file name=demandpoints multiple>
      <input type=submit value=Upload>
    </form>
    '''
@app.route('/success',methods=['GET', 'POST'])
def uploaded_file():
    args = request.args
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if (file.endswith(".zip")):
                with zipfile.ZipFile((os.path.join(subdir, file)), "r") as zip_ref:
                    zip_ref.extractall(subdir)
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if(file.endswith(".shp")):
                pointDf = gpd.read_file(os.path.join(subdir, file))
                print(pointDf)
    qgis_int = r"C:\Program Files\QGIS 3.20.3\bin\python-qgis.bat"
    subprocess.run(f'{qgis_int} "C:/Users/jyothy/Desktop/New folder/geoflask/secondary_nb.py" ')
    # subprocess.run(f'python "C:/Users/jyothy/Desktop/New folder/geoflask/databse_commit.py" ')
    base_dir = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output"
    full_dir = os.walk(base_dir)
    shapefile_list = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if (file_[-3:] == 'shp') and (file_[:-4] == "nodeboundary_output") :
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
    terminate_connections()
    remove_table("node_boundary")  
    terminate_connections()
    
    cmds = 'shp2pgsql "' + shapefile_list[0] + '" node_boundary | psql ' #shp2pgsql <shapefile_name> <new_table> | psql
    subprocess.call(cmds, shell=True)
    terminate_connections()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            os.remove(os.path.join(subdir, file))
    geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
    print("***************************RUNNING***********************")
    curr_time = int(time.time())
    workspaceName = "NODE_" + str(curr_time)
    # workspaceName = get_workspace()
    table_name = "node_boundary"
    geo.create_workspace(workspace=workspaceName)
    x = geo.create_featurestore(store_name=table_name, workspace=workspaceName, db='UAT', host='localhost',port="5432",
                            pg_user='postgres',
                            pg_password='postgres')
    # print(x)
    geo.publish_featurestore(workspace=workspaceName, store_name=table_name, pg_table=table_name,srs_data="EPSG:27700")
    print("***************************EN--D***********************")
    print(os.path.join(os.getcwd(),"nodebndry.sld"))
    # geo.upload_style(path=os.path.join(os.getcwd(),"nodebndry.sld"), workspace=workspaceName)
    geo.publish_style(layer_name=table_name, style_name='nodeboundary', workspace=workspaceName, srs_name="EPSG:27700")

    print("***************************END***********************")
    return {
        "status":200,
        "workspace":workspaceName,
        "sample_flask":table_name
    }
@app.route('/load_data',methods=['GET', 'POST'])
def load_data():
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    df = pd.read_sql_query('select cluster_id,sum(pon_homes) as tot_pon from "sample_flask_cluster_output" group by cluster_id having sum(pon_homes) > 24 or sum(pon_homes) < 3',con=engine)
    engine.dispose()
    terminate_connections()
    print(df)
    return {
        "status": 200,
        "cluster_id":list(df["cluster_id"].astype(str).values),
        "sum_pon_homes":list(df["tot_pon"].astype(str).values)
    }
    
@app.route('/update_db',methods=['GET', 'POST'])  
def update_db():
    if request.method == 'POST':
        # check if the post request has the file part
        cluster_in = int(request.form["input_cluster_id"])
        cluster_out = int(request.form["output_cluster_id"])
        gis_tool_id = int(request.form["gis_tool_id"])
        
        print("Cluster_in",cluster_in)
        print("Cluster_out",cluster_out)
        print("GIS_tool_id",gis_tool_id)
        
        #IMPORT psycopg2
        #update db
        try:
            conn = psycopg2.connect(
               database=database, user=user, password=password, host=host, port= port
            )
            conn.autocommit = True

            cursor = conn.cursor()
            sql = f'UPDATE "sample_flask_cluster_output" SET "cluster_id" = {cluster_out} WHERE ("gistool_id" = {gis_tool_id}) AND ("cluster_id" = {cluster_in})'
            cursor.execute(sql)
            conn.commit()
            
            #Closing the connection
            conn.close()
            terminate_connections()
            ver = generate_outliers()
            curr_time = int(time.time())
            workspace_name = f"NODE_{curr_time}"
            # workspace_name = "NODE_555"
            table_name = "sample_flask_cluster_output"
            geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
            geo.create_workspace(workspace=workspace_name)
            x = geo.create_featurestore(store_name=table_name, workspace=workspace_name, db='UAT', host='localhost',
                                        port="5432",
                                        pg_user='postgres',
                                        pg_password='postgres', loose_bbox="")
            print(x)
            geo.publish_featurestore(workspace=workspace_name, store_name=table_name, pg_table=table_name,
                                     srs_data="EPSG:27700")
            geo.publish_style(layer_name=table_name, style_name='outlier', workspace=workspace_name,
                              srs_name="EPSG:27700")
        except Exception as msg:
            return {
                "Status":"FAILED",
                "Error":msg
            }

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        df = pd.read_sql_query(
            'select cluster_id,sum(pon_homes) as tot_pon from "sample_flask_cluster_output" group by cluster_id having sum(pon_homes) > 24 or sum(pon_homes) < 3',
            con=engine)
        engine.dispose()
        terminate_connections()
        print(df)
        
        filepath_cluster_output = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output\cluster_output"
        query = f"SELECT * FROM {table_name}"
        
        cmds = 'pgsql2shp -f "'+ filepath_cluster_output + f'" -h {host} -u {user} -P {password} {database} "'+ query + '"'
        
        
        subprocess.call(cmds, shell=True)
        return {
            "status": 200,
            "cluster_id": list(df["cluster_id"].astype(str).values),
            "sum_pon_homes": list(df["tot_pon"].astype(str).values),
            "workspace_name":workspace_name,
            "table_name":table_name
        }
        print("*********WPNAME***")
        # print(get_workspace())
        # return redirect(url_for('load_data'))
    return '''
    <!doctype html>
    <title>Cluster Correction Test Form</title>
    <h1>Cluster Correction</h1>
    <form method=post enctype=multipart/form-data>
      <input type=text name=input_cluster_id multiple>
      <input type=text name=output_cluster_id multiple>
      <input type=text name=gis_tool_id multiple>
      <input type=submit value=Upload>
    </form>
    '''
    
@app.route('/aerial_page',methods=['GET', 'POST'])  
def aerial_page():
    if request.method == 'POST':
        # check if the post request has the file part
        for file in request.files.getlist('demandpoints'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):
            
            ls_name = file.filename.split(".")
            ls_name[0] = "demandpoints"
            file.filename = ".".join(ls_name)
            print("Demandpoints file",file.filename)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        for file in request.files.getlist('streetlines'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "streetlines"
            file.filename = ".".join(ls_name)
            print("Streetlines File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('aerial_update_db',
                                filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
        <h1>demandpoints</h1>
      <input type=file name=demandpoints multiple>
      <br>
      <h1>streetlines</h1>
      <input type=file name=streetlines multiple>
      <input type=submit value=Upload>
    </form>
    '''

@app.route('/aerial_update_db',methods=['GET', 'POST'])  
def aerial_update_db():
    generate_random_workspace()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if (file.endswith(".zip")):
                with zipfile.ZipFile((os.path.join(subdir, file)), "r") as zip_ref:
                    zip_ref.extractall(subdir)
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if(file.endswith(".shp")):
                pointDf = gpd.read_file(os.path.join(subdir, file))
                print(pointDf)
    qgis_int = r"C:\Program Files\QGIS 3.20.3\bin\python-qgis.bat"
    subprocess.run(f'{qgis_int} "C:/Users/jyothy/Desktop/New folder/geoflask/secondary_ar_process.py" ')
    # subprocess.run(f'python "C:/Users/jyothy/Desktop/New folder/geoflask/databse_commit.py" ')
    base_dir = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output"
    full_dir = os.walk(base_dir)
    shapefile_list = []
    name_shape_file = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp' and (file_[:-4] in ['cluster_output','nodes_output','outlier_output']):
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
                name_shape_file.append(file_[:-4])
    for shape_path in range(len(shapefile_list)):
        remove_table(f"sample_flask_{name_shape_file[shape_path]}")
        print()
        print("Table name",name_shape_file[shape_path])
        print()
        cmds = 'shp2pgsql "' + shapefile_list[shape_path] + f'" sample_flask_{name_shape_file[shape_path]} | psql '
        subprocess.call(cmds, shell=True)
        terminate_connections()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            os.remove(os.path.join(subdir, file))
    try:
        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = f'ALTER TABLE sample_flask_cluster_output ALTER COLUMN gistool_id TYPE INTEGER;'
        cursor.execute(sql)
        conn.commit()

        # Closing the connection
        conn.close()
        terminate_connections()
    except Exception as msg:
        print("Unable to change type", msg)
    ver = generate_outliers()
    print(ver)
    geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
    curr_time = int(time.time())
    workspace_name = "NODE_" + str(curr_time)
    # workspace_name = "NODE_555"
    # set_workspace(workspace_name)
    # workspace_name = get_workspace()
    geo.create_workspace(workspace=workspace_name)
    arr = []
    for shape_path in range(len(shapefile_list)):
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *RUNNING ** ** ** ** ** ** ** ** ** ** ** *
        # sample_flask_cluster_output
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *END ** ** ** ** ** ** ** ** ** ** ** *
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *RUNNING ** ** ** ** ** ** ** ** ** ** ** *
        # sample_flask_nodes_output
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *END ** ** ** ** ** ** ** ** ** ** ** *
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *RUNNING ** ** ** ** ** ** ** ** ** ** ** *
        # sample_flask_outlier_output
        # ** ** ** ** ** ** ** ** ** ** ** ** ** *END ** ** ** ** ** ** ** ** ** ** ** *


        print("***************************RUNNING***********************")
        
        table_name = f"sample_flask_{name_shape_file[shape_path]}"
        print(table_name)
        x = geo.create_featurestore(store_name=table_name, workspace=workspace_name, db='UAT', host='localhost', port="5432",
                                    pg_user='postgres',
                                    pg_password='postgres',loose_bbox="")

        geo.publish_featurestore(workspace=workspace_name, store_name=table_name, pg_table=table_name,srs_data="EPSG:27700")
        geo.publish_style(layer_name=table_name, style_name='trial', workspace=workspace_name,
                          srs_name="EPSG:27700")
        print("***************************END***********************")
        #GEOSERVER CODE
    return {
        "status":200,
        "table_name":name_shape_file+["joined_cluster_id"],
        "workspace_name": workspace_name
    }

@app.route('/ug_page',methods=['GET', 'POST'])  
def ug_page():
    if request.method == 'POST':
        # check if the post request has the file part
        for file in request.files.getlist('demandpoints'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):
            
            ls_name = file.filename.split(".")
            ls_name[0] = "demandpoints"
            file.filename = ".".join(ls_name)
            print("Demandpoints file",file.filename)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        for file in request.files.getlist('streetlines'):
            # f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
            # if 'file' not in request.files:
            #     flash('No file part')
            #     return redirect(request.url)
            # file = request.files['file']
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "streetlines"
            file.filename = ".".join(ls_name)
            print("Streetlines File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('ug_update_db',
                                filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
        <h1>demandpoints</h1>
      <input type=file name=demandpoints multiple>
      <br>
      <h1>streetlines</h1>
      <input type=file name=streetlines multiple>
      <input type=submit value=Upload>
    </form>
    '''
@app.route('/ug_update_db',methods=['GET', 'POST'])  
def ug_update_db():
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if (file.endswith(".zip")):
                with zipfile.ZipFile((os.path.join(subdir, file)), "r") as zip_ref:
                    zip_ref.extractall(subdir)
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if(file.endswith(".shp")):
                pointDf = gpd.read_file(os.path.join(subdir, file))
                print(pointDf)
    qgis_int = r"C:\Program Files\QGIS 3.20.3\bin\python-qgis.bat"
    subprocess.run(f'{qgis_int} "C:/Users/jyothy/Desktop/New folder/geoflask/secondary_ug_process.py" ')
    # subprocess.run(f'python "C:/Users/jyothy/Desktop/New folder/geoflask/databse_commit.py" ')
    base_dir = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output"
    full_dir = os.walk(base_dir)
    shapefile_list = []
    name_shape_file = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp' and (file_[:-4] in ['cluster_output','nodes_output','outlier_output']):
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
                name_shape_file.append(file_[:-4])
    for shape_path in range(len(shapefile_list)):
        terminate_connections()
        remove_table(f"sample_flask_{name_shape_file[shape_path]}")
        print()
        print("Table name",name_shape_file[shape_path])
        print()
        terminate_connections()
        cmds = 'shp2pgsql "' + shapefile_list[shape_path] + f'" sample_flask_{name_shape_file[shape_path]} | psql '
        subprocess.call(cmds, shell=True)
        terminate_connections()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            os.remove(os.path.join(subdir, file))

    try:
        terminate_connections()
        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
        conn.autocommit = True

        cursor = conn.cursor()
        sql = f'ALTER TABLE sample_flask_cluster_output ALTER COLUMN gistool_id TYPE INTEGER;'
        cursor.execute(sql)
        conn.commit()

        # Closing the connection
        conn.close()
        terminate_connections()
    except Exception as msg:
        print("Unable to change type", msg)
    ver = generate_outliers()
    print(ver)
    geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
    curr_time = int(time.time())
    workspace_name = f"NODE_{curr_time}"
    
    geo.create_workspace(workspace=workspace_name)
    arr = []
    for shape_path in range(len(shapefile_list)):
    
        print("***************************RUNNING***********************")
        
        table_name = f"sample_flask_{name_shape_file[shape_path]}"
        
        x = geo.create_featurestore(store_name=f"sample_flask_{name_shape_file[shape_path]}", workspace=workspace_name, db='UAT', host='localhost', port="5432",
                                    pg_user='postgres',
                                    pg_password='postgres',loose_bbox="")
        print(x)
        geo.publish_featurestore(workspace=workspace_name, store_name=f"sample_flask_{name_shape_file[shape_path]}", pg_table=table_name,srs_data="EPSG:27700")
        
        print("***************************END***********************")
        #GEOSERVER CODE
    return {
        "status":200,
        "table_name":name_shape_file+["joined_cluster_id"],
        "workspace_name": workspace_name
    }
    
@app.route('/np_page',methods=['GET', 'POST'])  
def np_page():
    if request.method == 'POST':
        for file in request.files.getlist('existing_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            
            ls_name = file.filename.split(".")
            ls_name[0] = "1pia_structures"
            file.filename = ".".join(ls_name)
            print("Existing file",file.filename)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        for file in request.files.getlist('gaist_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "1cityfibre lincoln 2021"
            file.filename = ".".join(ls_name)
            print("Gaist File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            
        for file in request.files.getlist('landboundary_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "1Land_Registry_Cadastral_Parcels PREDEFINED"
            file.filename = ".".join(ls_name)
            print("Gaist File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('np_update_db',
                                filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
        <h1>Existing File</h1>
      <input type=file name=existing_files multiple>
      <br>
      <h1>Gaist File</h1>
      <input type=file name=gaist_files multiple>
      <br>
      <h1>Landboundary File</h1>
      <input type=file name=landboundary_files multiple>
      <input type=submit value=Upload>
    </form>
    '''

@app.route('/np_update_db',methods=['GET', 'POST'])
def np_update_db():
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if (file.endswith(".zip")):
                with zipfile.ZipFile((os.path.join(subdir, file)), "r") as zip_ref:
                    zip_ref.extractall(subdir)
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if(file.endswith(".shp")):
                pointDf = gpd.read_file(os.path.join(subdir, file))
                print(pointDf)
    qgis_int = r"C:\Program Files\QGIS 3.20.3\bin\python-qgis.bat"
    subprocess.run(f'{qgis_int} "C:/Users/jyothy/Desktop/New folder/geoflask/secondary_np.py" ')
    # subprocess.run(f'python "C:/Users/jyothy/Desktop/New folder/geoflask/databse_commit.py" ')
    base_dir = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output"
    full_dir = os.walk(base_dir)
    shapefile_list = []
    name_shape_file = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp' and (file_[:-4] in ['existing_output','proposed_output']):
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
                name_shape_file.append(file_[:-4])
    for shape_path in range(len(shapefile_list)):
        terminate_connections()
        remove_table(f"sample_flask_{name_shape_file[shape_path]}")
        terminate_connections()
        print()
        print("Table name",name_shape_file[shape_path])
        print()
        cmds = 'shp2pgsql "' + shapefile_list[shape_path] + f'" sample_flask_{name_shape_file[shape_path]} | psql '
        subprocess.call(cmds, shell=True)
        terminate_connections()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            os.remove(os.path.join(subdir, file))
    geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
    curr_time = int(time.time())
    workspace_name = f"NODE_{curr_time}"
    
    geo.create_workspace(workspace=workspace_name)
    arr = []
    for shape_path in range(len(shapefile_list)):
    
        print("***************************RUNNING***********************")
        
        table_name = f"sample_flask_{name_shape_file[shape_path]}"
        
        x = geo.create_featurestore(store_name=f"sample_flask_{name_shape_file[shape_path]}", workspace=workspace_name, db='UAT', host='localhost', port="5432",
                                    pg_user='postgres',
                                    pg_password='postgres',loose_bbox="")
        print(x)
        geo.publish_featurestore(workspace=workspace_name, store_name=f"sample_flask_{name_shape_file[shape_path]}", pg_table=table_name,srs_data="EPSG:27700")
        
        print("***************************END***********************")
        geo.publish_style(layer_name=table_name, style_name=table_name, workspace=workspace_name,srs_name="EPSG:27700")
        # geo.upload_style(path=os.path.join(os.getcwd(),"proposed.sld"), workspace=workspaceName)
    #geo.publish_style(layer_name=table_name, style_name='nodeboundary', workspace=workspaceName, srs_name="EPSG:27700")

    print("***************************END***********************")
        #GEOSERVER CODE
    return {
        "status":200,
        "table_name":name_shape_file,
        "workspace_name": workspace_name
    }

@app.route('/np_ug_page',methods=['GET', 'POST'])  
def np_ug_page():
    if request.method == 'POST':
        for file in request.files.getlist('existing_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            
            ls_name = file.filename.split(".")
            ls_name[0] = "1pia_structures"
            file.filename = ".".join(ls_name)
            print("Existing file",file.filename)

            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        for file in request.files.getlist('gaist_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "1cityfibre lincoln 2021"
            file.filename = ".".join(ls_name)
            print("Gaist File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            
        for file in request.files.getlist('landboundary_files'):

            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            # if file and allowed_file(file.filename):

            ls_name = file.filename.split(".")
            ls_name[0] = "1Land_Registry_Cadastral_Parcels PREDEFINED"
            file.filename = ".".join(ls_name)
            print("Gaist File",file.filename)
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('np_ug_update_db',
                                filename=filename))
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
        <h1>Existing File</h1>
      <input type=file name=existing_files multiple>
      <br>
      <h1>Gaist File</h1>
      <input type=file name=gaist_files multiple>
      <br>
      <h1>Landboundary File</h1>
      <input type=file name=landboundary_files multiple>
      <input type=submit value=Upload>
    </form>
    '''
    
@app.route('/np_ug_update_db',methods=['GET', 'POST'])
def np_ug_update_db():
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if (file.endswith(".zip")):
                with zipfile.ZipFile((os.path.join(subdir, file)), "r") as zip_ref:
                    zip_ref.extractall(subdir)
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            if(file.endswith(".shp")):
                pointDf = gpd.read_file(os.path.join(subdir, file))
                print(pointDf)
    qgis_int = r"C:\Program Files\QGIS 3.20.3\bin\python-qgis.bat"
    subprocess.run(f'{qgis_int} "C:/Users/jyothy/Desktop/New folder/geoflask/secondary_np_ug.py" ')
    # subprocess.run(f'python "C:/Users/jyothy/Desktop/New folder/geoflask/databse_commit.py" ')
    base_dir = r"C:\Users\jyothy\Desktop\New folder\geoflask\data output"
    full_dir = os.walk(base_dir)
    shapefile_list = []
    name_shape_file = []
    for source, dirs, files in full_dir:
        for file_ in files:
            if file_[-3:] == 'shp' and (file_[:-4] in ['existing_output_ug','proposed_output_ug']):
                shapefile_path = os.path.join(base_dir, file_)
                shapefile_list.append(shapefile_path)
                name_shape_file.append(file_[:-4])
    for shape_path in range(len(shapefile_list)):
        terminate_connections()
        remove_table(f"sample_flask_{name_shape_file[shape_path]}")
        terminate_connections()
        print()
        print("Table name",name_shape_file[shape_path])
        print()
        cmds = 'shp2pgsql "' + shapefile_list[shape_path] + f'" sample_flask_{name_shape_file[shape_path]} | psql '
        subprocess.call(cmds, shell=True)
        terminate_connections()
    for subdir, dirs, files in os.walk(app.config['UPLOAD_FOLDER']):
        for file in files:
            os.remove(os.path.join(subdir, file))
    geo = Geoserver('http://localhost:8080/geoserver', username='admin', password='geoserver')
    curr_time = int(time.time())
    workspace_name = f"NODE_{curr_time}"
    
    geo.create_workspace(workspace=workspace_name)
    arr = []
    for shape_path in range(len(shapefile_list)):
    
        print("***************************RUNNING***********************")
        
        table_name = f"sample_flask_{name_shape_file[shape_path]}"
        
        x = geo.create_featurestore(store_name=f"sample_flask_{name_shape_file[shape_path]}", workspace=workspace_name, db='UAT', host='localhost', port="5432",
                                    pg_user='postgres',
                                    pg_password='postgres',loose_bbox="")
        print(x)
        geo.publish_featurestore(workspace=workspace_name, store_name=f"sample_flask_{name_shape_file[shape_path]}", pg_table=table_name,srs_data="EPSG:27700")
        geo.publish_style(layer_name=table_name, style_name=table_name, workspace=workspace_name,srs_name="EPSG:27700")
        print("***************************END***********************")
        #GEOSERVER CODE
    return {
        "status":200,
        "table_name":name_shape_file,
        "workspace_name": workspace_name
    }