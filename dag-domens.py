import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS, skiprows=8)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_top_10():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'],  skiprows=8)
    df['zone'] = df.domain.apply(lambda x: x.split('.')[-1])
    top_10_domain = df.groupby('zone').domain.count().sort_values(ascending=False).to_frame().head(10)
    with open('top_10_domain.csv', 'w') as f:
        f.write(top_10_domain.to_csv(header=False))


def get_longest():
    the_longest_domain_name = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'],  skiprows=8)
    the_longest_domain_name['domain_len'] = the_longest_domain_name['domain'].apply(lambda x: len(x))
    the_longest_domain_name = (the_longest_domain_name.sort_values(['domain_len', 'domain'], ascending=[False, True]).reset_index().domain[0])
    
    with open('the_longest_domain_name.txt', 'w') as f:
        f.write(the_longest_domain_name)
        
def get_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'],  skiprows=8)
    if 'airflow.com' not in df.domain.to_list():
        airflow_rank = 'airflow.com is not found'
    else:
        rank = df.query('domain == "airflow.com"')['rank'].values[0]
        airflow_rank = f'airflow.com is {rank} in rank'
    return airflow_rank
    with open('airflow_rank.txt','w') as f:
        f.write(airflow_rank)

def print_data(ds):
    with open('top_10_domain.csv', 'r') as f:
        top_10_domain = f.read()
        
    with open('the_longest_domain_name.csv', 'r') as f:
        the_longest_domain_name = f.read()
        
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()    
        
    date = ds

    print(f'Top domains zone by domain counts for date {date}')
    print(top_10_domain)
    
    print(f'The longest domail name for date {date} is {the_longest_domain_name}')
    print(f'Airfow.com rank for date {date} is {airflow_rank}')  


default_args = {
    'owner': 'b-dzhumaeva-37',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 3),
}
schedule_interval = '@daily'

dag = DAG('domains_by_dzhumaeva', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_top_10',
                    python_callable=get_top_10,
                    dag=dag)

t3 = PythonOperator(task_id='get_longest',
                    python_callable=get_longest,
                    dag=dag)

t4 = PythonOperator(task_id='get_rank',
                        python_callable=get_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2,t3,t4] >> t5

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)