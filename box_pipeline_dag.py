from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 27),
}

dag = DAG('scrapy_crawl_dag', default_args=default_args, schedule_interval='@daily')#,  schedule_interval='@daily'

project_folder = '~/Code/airflow_projects/scrapy_boxoffice/boxoffice_scraper'
project_folder = os.path.expanduser(project_folder)  # Expand the tilde character

current_datetime = datetime.now().strftime("%B_%d_%H%M%S").lower()
output_file = f'scraped_{current_datetime}.csv'

crawl_command = f'cd {project_folder} && scrapy crawl boxoffice -o {output_file} && echo {output_file}'

analysis_file = f'analyzed_{current_datetime}.csv'


def analyze_ratings(**context):
    # Retrieve the output file path from the context
    output_file = context['ti'].xcom_pull(task_ids='scrapy_crawl_task')

    # Read the saved CSV file
    csv_file = f'{project_folder}/{output_file}'
    analysis_csv = f'{project_folder}/Rating_{analysis_file}'

    data = pd.read_csv(csv_file)

    
    data["Mpaa_rating"] = data["Mpaa_rating"].fillna("NotRated")

    # Preprocess currency columns
    data['Domestic_BO'] = data['Domestic_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['International_BO'] = data['International_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['Worldwide_BO'] = data['Worldwide_BO'].replace('[\$,]', '', regex=True).astype(float)

    # Calculate mean, total earnings, and count of movies for different MPAA ratings
    mpaa_stats = data.groupby('Mpaa_rating').agg({'Worldwide_BO': ['mean', 'sum', 'count']})
    mpaa_stats.columns = ['Mean_Earnings', 'Total_Earnings', 'Movie_Count']
    mpaa_stats.reset_index(inplace=True)

    # Round the mean earnings to 1 decimal place
    mpaa_stats['Mean_Earnings'] = mpaa_stats['Mean_Earnings'].round(1)

    # Sort the MPAA ratings based on mean earnings
    sorted_by_mean_earnings = mpaa_stats.sort_values('Mean_Earnings', ascending=False).head(10)

    # Sort the MPAA ratings based on movie count
    sorted_by_movie_count = mpaa_stats.sort_values('Movie_Count', ascending=False).head(10)

    # Sort the MPAA ratings based on total earnings
    sorted_by_total_earnings = mpaa_stats.sort_values('Total_Earnings', ascending=False).head(10)

    # Write the top 10 MPAA ratings to the same CSV file
    with open(analysis_csv, 'w') as file:
        # Write a separator line
        file.write('\n--------------------\n\n')

        # Write the top 10 MPAA ratings by mean earnings
        file.write('Top 10 MPAA Ratings by Mean Earnings\n')
        sorted_by_mean_earnings.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 MPAA ratings by movie count
        file.write('Top 10 MPAA Ratings by Movie Count\n')
        sorted_by_movie_count.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 MPAA ratings by total earnings
        file.write('Top 10 MPAA Ratings by Total Earnings\n')
        sorted_by_total_earnings.to_csv(file, index=False)




def analyze_distributors(**context):
    # Retrieve the output file path from the context
    output_file = context['ti'].xcom_pull(task_ids='scrapy_crawl_task')

    # Read the saved CSV file
    csv_file = f'{project_folder}/{output_file}'
    analysis_csv = f'{project_folder}/Distributors_{analysis_file}'

    data = pd.read_csv(csv_file)



    # Preprocess currency columns
    data['Domestic_BO'] = data['Domestic_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['International_BO'] = data['International_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['Worldwide_BO'] = data['Worldwide_BO'].replace('[\$,]', '', regex=True).astype(float)

    # Calculate mean, total earnings, and count of movies for different distributors
    distributor_stats = data.groupby('Distributor').agg({'Worldwide_BO': ['mean', 'sum', 'count']})
    distributor_stats.columns = ['Mean_Earnings', 'Total_Earnings', 'Movie_Count']
    distributor_stats.reset_index(inplace=True)

    # Round the mean earnings to 1 decimal place
    distributor_stats['Mean_Earnings'] = distributor_stats['Mean_Earnings'].round(1)

    # Sort the distributors based on count
    sorted_by_count = distributor_stats.sort_values('Movie_Count', ascending=False).head(10)

    # Sort the distributors based on average earnings
    sorted_by_avg_earnings = distributor_stats.sort_values('Mean_Earnings', ascending=False).head(10)

    # Sort the distributors based on total earnings
    sorted_by_total_earnings = distributor_stats.sort_values('Total_Earnings', ascending=False).head(10)


    # Write the top 10 distributors to the same CSV file
    with open(analysis_csv, 'w') as file:
        # Write the top 10 distributors by count
        file.write('Top 10 Distributors by Count\n')
        sorted_by_count.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 distributors by average earnings
        file.write('Top 10 Distributors by Average Earnings\n')
        sorted_by_avg_earnings.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 distributors by total earnings
        file.write('Top 10 Distributors by Total Earnings\n')
        sorted_by_total_earnings.to_csv(file, index=False)





def perform_analysis(**context):
    # Retrieve the output file path from the context
    output_file = context['ti'].xcom_pull(task_ids='scrapy_crawl_task')

    # Read the saved CSV file
    csv_file = f'{project_folder}/{output_file}'
    analysis_csv = f'{project_folder}/BO_{analysis_file}'

    data = pd.read_csv(csv_file)
    # Read the CSV file and preprocess currency columns
    data['Domestic_BO'] = data['Domestic_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['International_BO'] = data['International_BO'].replace('[\$,]', '', regex=True).astype(float)
    data['Worldwide_BO'] = data['Worldwide_BO'].replace('[\$,]', '', regex=True).astype(float)

    # Perform analysis tasks
    # Sort the movies based on Domestic_BO
    sorted_domestic_bo = data.sort_values('Domestic_BO', ascending=False)

    # Sort the movies based on International_BO
    sorted_international_bo = data.sort_values('International_BO', ascending=False)

    # Sort the movies based on Worldwide_BO
    sorted_worldwide_bo = data.sort_values('Worldwide_BO', ascending=False)

    # Get the top 10 movies for each category
    top_10_domestic = sorted_domestic_bo.head(10)
    top_10_international = sorted_international_bo.head(10)
    top_10_worldwide = sorted_worldwide_bo.head(10)

    # Write the top 10 movies to the CSV file
    with open(analysis_csv, 'w') as file:
        # Write the top 10 Domestic_BO movies
        file.write('Top 10 Domestic_BO\n')
        top_10_domestic.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 International_BO movies
        file.write('Top 10 International_BO\n')
        top_10_international.to_csv(file, index=False)
        file.write('\n')

        # Write a separator line
        file.write('--------------------\n\n')

        # Write the top 10 Worldwide_BO movies
        file.write('Top 10 Worldwide_BO\n')
        top_10_worldwide.to_csv(file, index=False)
    

analyze_ratings_task = PythonOperator(
    task_id='analyze_ratings_task',
    python_callable=analyze_ratings,
    provide_context=True,
    dag=dag
)

analyze_distributors_task = PythonOperator(
    task_id='analyze_distributors_task',
    python_callable=analyze_distributors,
    provide_context=True,
    dag=dag
)

BO_analysis_task = PythonOperator(
    task_id='BO_analysis_task',
    python_callable=perform_analysis,
    provide_context=True,
    dag=dag
)

scrapy_crawl_task = BashOperator(
    task_id='scrapy_crawl_task',
    bash_command=crawl_command,
    dag=dag
)

scrapy_crawl_task >> [BO_analysis_task, analyze_distributors_task, analyze_ratings_task]