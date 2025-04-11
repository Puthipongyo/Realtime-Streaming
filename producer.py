from kafka import KafkaProducer
import json
import logging
import pandas as pd
from time import sleep
import datetime
import requests
from airflow import DAG

def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        res.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        return res.json()['results'][0]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None

def format_data(res):
    global count
    if res is None:
        return None
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def getApi():
    res = get_data()
    formatted_res = format_data(res)
    return formatted_res

def streamming():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    data = getApi()  # This will call getApi to get the formatted data
    producer.send("test_streamming", value=data)
    producer.flush()
    sleep(5)

if __name__ == "__main__":
    while True:
        streamming()