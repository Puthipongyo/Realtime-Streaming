from kafka import KafkaProducer, KafkaAdminClient
import json
import logging
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO)

# Fetch data from the API
def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()
        return res.json()['results'][0]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

# Format the fetched data
def format_data(res):
    if res is None:
        return None

    location = res['location']
    return {
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

# Wait until Kafka is ready
def wait_for_kafka():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers='kafka1:29092')
        admin_client.list_topics()
        return True
    except Exception as e:
        logging.error(f"Kafka is not ready: {e}")
        return False

# Streaming task
def streamming_to_kafka(**kwargs):
    if not wait_for_kafka():
        raise Exception("Kafka is not available")

    producer = KafkaProducer(
        bootstrap_servers='kafka1:29092',  # 👈 updated for host
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    start_time = time.time()
    duration = 10 * 60  # 10 minutes

    while (time.time() - start_time) < duration:
        data = format_data(get_data())
        if data:
            producer.send("test_streamming", value=data)
            producer.flush()
            logging.info(f"✅ Sent data to Kafka: {data['username']}")
        else:
            logging.warning("⚠️ No data to send.")
        time.sleep(3)

# Default DAG args
default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 8),  # today's date
}

# Define DAG
dag = DAG(
    'kafka_streamming_dag_10min_loop',
    default_args=default_args,
    description='Send data to Kafka every 3 seconds for 10 minutes',
    schedule_interval='@once',
    catchup=False,
)

# Define task
stream_kafka_task = PythonOperator(
    task_id='stream_to_kafka_every_3s_for_10min',
    python_callable=streamming_to_kafka,
    dag=dag,
)

stream_kafka_task