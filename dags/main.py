from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import csv
import re
import os

def extract_data(urls):
    alldata = []
    for url in urls:
        print(f"\nExtracting data from {url}")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        links = [link.get('href') for link in soup.find_all('a', href=True)]
        articles = soup.find_all('article')
        article_data = []
        for idx, article in enumerate(articles):
            title = article.find('h2').text.strip() if article.find('h2') else None
            description = article.find('p').text.strip() if article.find('p') else None
            article_data.append({'id': idx+1, 'title': title, 'description': description, 'source': url})
        
        print(f"Extracted {len(article_data)} articles and {len(links)} links from {url}")
        alldata.extend(article_data)
    return alldata


def preprocess(text):
    clean_text = re.sub('<.*?>', '', text)
    clean_text = re.sub('[^a-zA-Z]', ' ', clean_text)
    clean_text = clean_text.lower()
    clean_text = re.sub(' +', ' ', clean_text)
    return clean_text

def clean_data(data):
    cleaned_data = []
    for article in data:
        article['title'] = preprocess(article['title']) if article.get('title') else None
        article['description'] = preprocess(article['description']) if article.get('description') else None
        cleaned_data.append(article)
    return cleaned_data


def save_to_csv(file_name, articles):
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'title', 'description', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for article in articles:
            writer.writerow(article)


##### airflow task specific functions

def dvc_push():
    os.system('dvc add /mnt/d/uni stuff/Semester #8/mlops/work/MLOps_Assignment_2/airflow-data-scraping-pipeline/data/extracted.csv')
    os.system('dvc push')
    os.system('git add /mnt/d/uni stuff/Semester #8/mlops/work/MLOps_Assignment_2/airflow-data-scraping-pipeline/data/extracted.csv.dvc')
    os.system('git commit -m "Updated dataset"')
    os.system('git push origin main')
    os.system('git status')


urls = ['https://www.dawn.com/', 'https://www.bbc.com/']
filename = "/mnt/d/uni stuff/Semester #8/mlops/work/MLOps_Assignment_2/airflow-data-scraping-pipeline/data/extracted.csv"


######  dag specifications

default_args = {
    'owner': 'm-huzaifa',
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple DAG for my MLOps assignment',
)

###### airflow operators

with dag:
    task1 = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        op_kwargs={'urls': urls},
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='preprocess_task',
        python_callable=clean_data,
        op_kwargs={'data': task1.output},
        provide_context=True
    )

    task3 = PythonOperator(
        task_id='save_task',
        python_callable=save_to_csv,
        op_kwargs={'file_name': filename, 'articles': task2.output},
        provide_context=True
    )

    task4 = PythonOperator(
        task_id='dvc_push_task',
        python_callable=dvc_push,
    )


# tasks execution order for airflow
task1 >> task2 >> task3 >> task4 
