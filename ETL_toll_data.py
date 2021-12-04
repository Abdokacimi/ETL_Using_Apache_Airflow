# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments

# Task 1.1
default_args = {
    'owner': 'me',
    'start_date': days_ago(0),
    'email': ['me@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Task 1.2
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task 1.3

unzip_data=BashOperator(
    task_id='unzip',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";tar -xzf tolldata.tgz',
    dag=dag,
)

# Task 1.4
extract_data_from_csv=BashOperator(
    task_id='extract_csv',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# Task 1.5

extract_data_from_tsv=BashOperator(
    task_id='extract_tsv',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";cut -d" " -f5-7 vehicle-data.csv > tsv_data.csv',
    dag=dag,
)

# Task 1.6 
extract_data_from_fixed_width=BashOperator(
    task_id='fixed_width',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";awk \'{print $6,$7}\' payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# Task 1.7

consolidate_data=BashOperator(
    task_id='consolidate_task',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# Task 1.8

transform_data=BashOperator(
    task_id='transform_and_load',
    bash_command='cd "/home/project/airflow/dags/finalassignment/staging";tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data