import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    'owner': 'b-dzhumaeva-37',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 3),
}
schedule_interval = '@daily'
login = 'b-dzhumaeva-37'
year = 2003 + hash(f'{login}') % 23

@dag(default_args=default_args, catchup=False)
def lesson_03_b_dzhumaeva():
    @task()
    def get_data():
        games = pd.read_csv('/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv')        
        games_w_year = games[games.Year == year]
        games_w_year.columns = games_w_year.columns.str.lower()
        return games_w_year

    @task()
    def top_game(games_w_year):
        top_game = ', '.join(games_w_year.sort_values('global_sales', ascending=False) \
                                         .name.head(1) \
                                         .tolist())
        return top_game

    @task()
    def top_genres_eu(games_w_year):
        genres_eu = games_w_year.groupby('genre', as_index=False).eu_sales.sum()
        top_genres_eu = ', '.join(genres_eu[genres_eu.eu_sales 
                                            == genres_eu.eu_sales.max()].genre.tolist())
        return top_genres_eu

    @task()
    def top_platform_na(games_w_year):
        platform_na = games_w_year[games_w_year.na_sales > 1].groupby('platform', as_index=False).name.nunique()
        top_platform_na = ', '.join(platform_na[platform_na.name 
                                                == platform_na.name.max()].platform.tolist())
        return top_platform_na

    @task()
    def top_publishers_jp(games_w_year):
        publishers_jp = games_w_year.groupby('publisher', as_index=False).jp_sales.mean()
        top_publishers_jp = ', '.join(publishers_jp[publishers_jp.jp_sales 
                                                    == publishers_jp.jp_sales.max()].publisher.tolist())
        return top_publishers_jp
    
    @task()
    def eu_vs_jp(games_w_year):
        eu_vs_jp = games_w_year[games_w_year.eu_sales > games_w_year.jp_sales].name.nunique()
        return eu_vs_jp

    @task()
    def print_data(top_game,
                   top_genres_eu,
                   top_platform_na,
                   top_publishers_jp,
                   eu_vs_jp):

        print(f'''Data about games for {year}:
                  Top selling game: {top_game}
                  Top selling genres in EU: {top_genres_eu}
                  Top selling platforms in NA: {top_platform_na}
                  Top selling publishers in JP: {top_publishers_jp}
                  Number of games selling better in EU than in JP: {eu_vs_jp}''')

    games_data = get_data()
    
    top_game = top_game(games_data)
    top_genres_eu = top_genres_eu(games_data)
    top_platform_na = top_platform_na(games_data)
    top_publishers_jp = top_publishers_jp(games_data)
    eu_vs_jp = eu_vs_jp(games_data)

    print_data(top_game,
               top_genres_eu,
               top_platform_na,
               top_publishers_jp,
               eu_vs_jp)

lesson_03_b_dzhumaeva = lesson_03_b_dzhumaeva()