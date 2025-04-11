from kafka import KafkaProducer, KafkaAdminClient
import json
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Function to fetch data from the randomuser.me API
def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()
        return res.json()['results'][0]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

# Format the fetched data for Kafka
def format_data(res):
    if res is None:
        return None

    location = res['location']
    data = {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

    return data

# Function to check if Kafka is ready
def wait_for_kafka():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers='kafka1:29092')
        admin_client.list_topics()
        return True
    except Exception as e:
        logging.error(f"Kafka is not ready: {e}")
        return False

# Get API data and return the formatted result
def get_api_data():
    res = get_data()
    return format_data(res)

# Send data to Kafka
def streamming_to_kafka(**kwargs):
    if not wait_for_kafka():
        raise Exception("Kafka is not available")

    producer = KafkaProducer(
        bootstrap_servers='kafka1:29092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    data = get_api_data()
    if data:
        producer.send("test_streamming", value=data)
        producer.flush()
        logging.info("Data sent to Kafka successfully")
    else:
        logging.error("No data to send to Kafka")

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),  # Start from Jan 1, 2024
}

# Define the DAG
dag = DAG(
    'kafka_streamming_dag',
    default_args=default_args,
    description='A weekly DAG to stream random user data to Kafka',
    schedule_interval='@weekly',  # Run once a week
    catchup=True,  # Run for all previous weeks since start_date
)

# Define the task
stream_kafka_task = PythonOperator(
    task_id='stream_to_kafka',
    python_callable=streamming_to_kafka,
    dag=dag,
)

# Set the task
stream_kafka_task