import subprocess
import os
os.environ['PATH'] += r';C:\Program Files\PostgreSQL\14\bin'
os.environ['PGPASSWORD'] = 'postgres'
shapefile_list = [r"C:\Users\jyothy\Desktop\geoflask\sample data\cluster1.shp"]
sql = "SELECT pid, (SELECT pg_terminate_backend(pid)) as killed from pg_stat_activity WHERE datname = 'UAT';"
cmds = f'psql -U postgres -w -c "{sql}"'
subprocess.call(cmds, shell=True)