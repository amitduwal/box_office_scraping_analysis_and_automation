# Box Office Project

The Box Office project is a data pipeline implemented using Apache Airflow, Scrapy, and pandas. It automates the process of scraping box office data from a website and performs analysis on the collected data to extract valuable insights.

The website used for scraping is `boxofficemojo.com `which was done for educational purposes only.

## Project Structure

The project has the following structure:

- `box_pipeline_dag.py`: is the Airflow DAG file that defines the data pipeline and should be placed in `dags/`.
- `boxoffice_scraper/`: Contains the Scrapy project`boxoffice_spider.py` responsible for scraping box office data.
- `boxoffice_scraper/analysis.ipynb`: Contains additional Python scripts for anaysis
- `scraped, BO_analyzed, Distributors_analyzed, Ratings_analyzed `: are the various CSV files

## Requirements

The project requires the following dependencies:

- Apache Airflow
- Scrapy
- pandas

You can install the dependencies using the `requirements.txt` file:

```
pip install -r requirements.txt
```

## Usage

1. Set up Apache Airflow by following the installation instructions provided in the official documentation.

2. Copy the `box_office_dag.py` file from the `dags/` directory to the Airflow DAGs folder.

3. Configure the project folder path in the `box_office_dag.py` file to point to the project's location on your system.

4. Run the Airflow scheduler and web server.

5. Access the Airflow web UI and trigger the `scrapy_crawl_task` manually or schedule it to run daily.

6. Once the crawl task is executed, the scraped data will be stored in a CSV file with a timestamp.

7. The analysis tasks `analyze_ratings_task`, `analyze_distributors_task`, and `BO_analysis_task` will perform analysis on the scraped data and generate separate CSV files with the top 10 records for each category.

8. You can access the analysis results in the generated CSV files for further exploration and decision-making.

## Conclusion

The Box Office project demonstrates the power of using Apache Airflow, Scrapy, and pandas to automate the collection and analysis of box office data. By utilizing the capabilities of Airflow for task scheduling and dependency management, the project provides a reliable and scalable solution for regularly updating and extracting valuable insights from box office information.
[Screencast from 29-5-23 02:39:41 अपराह्न +0545.webm](https://github.com/amitduwal/box_office_scraping_analysis_and_automation/assets/43780571/8a79ac6b-5179-4562-aedc-6cb1270fee27)

