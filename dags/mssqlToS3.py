from airflow import DAG
from helperUtils.mssql_to_s3_operator import MsSQLToS3Operator
from datetime import datetime, timedelta

database='Test'
schema='paul'
table='people'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG("SavePeopletoS3", default_args=default_args, schedule_interval=timedelta(1))

tSavePeopleToS3 = MsSQLToS3Operator(
    task_id='SavePeopleToS3',
    dag=dag,
    mssql_conn_id='sqlServer-playground',
    mssql_table=table,
    mssql_database=database,
    mssql_schema=schema,
    s3_conn_id='s3-playground',
    s3_bucket='acia-eap-raw',
    s3_key=database+'/'+schema+'/'+table+'/'+database+'.'+schema+'.'+table+'_'+datetime.today().isoformat()+'.csv',
    batchsize=100,primary_key='ID',
    package_ddl=True,
    incremental_key='update_dt',
    start= '2019-08-16 00:00:00',
    end='2019-08-17 23:59:59'
    )

tSavePeopleToS3