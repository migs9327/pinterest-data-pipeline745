#%%
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
from kafka import KafkaConsumer
from kafka import KafkaClient
from kafka.cluster import ClusterMetadata
#%%
random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():

    sleep(random.randrange(0, 2))
    random_row = random.randint(0, 11000)
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:

        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
        
        for row in pin_selected_row:
            pin_result = dict(row._mapping)

        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        
        for row in geo_selected_row:
            geo_result = dict(row._mapping)

        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)
        
        for row in user_selected_row:
            user_result = dict(row._mapping)

        return pin_result, geo_result, user_result
    

def custom_json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def invoke_response(url, result):

    print(f"Calling {url} with result {result}")

    payload = json.dumps(
        {
            'records': [
                {
                    "value": result
                }
        ]
        }, 
        indent=4,
        default=custom_json_serializer
    )

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.status_code)

#%%
def main():
    while True:
        results = run_infinite_post_data_loop()
        invoke_url = "https://9v10hftic7.execute-api.us-east-1.amazonaws.com/test"
        user_id = "0ec5d6ae59bd"
        topic_endings = ('.pin', '.geo', '.user')
        urls = [f"{invoke_url}/topics/{user_id}{topic_ending}" for topic_ending in topic_endings]
        urls_results = zip(urls, results)

        for url, result in urls_results:
            invoke_response(url, result)

if __name__ == "__main__":
    main()
# %%
bootstrap_server_string = "b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098"
meta_cluster_conn = ClusterMetadata(
    bootstrap_servers=bootstrap_server_string
    )
print(meta_cluster_conn.brokers())
#%%
# Create a connection to our KafkaBroker to check if it is running
client_conn = KafkaClient(
    bootstrap_servers=bootstrap_server_string, # Specific the broker address to connect to
    client_id="Broker test" # Create an id from this client for reference
)

# Check that the server is connected and running
print(client_conn.bootstrap_connected())
# Check our Kafka version number
print(client_conn.check_version())
# %%
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_server_string,
    value_deserializer=lambda message: json.loads(message),
    auto_offset_reset="earliest"
    )
consumer.subscribe(topics=["0ec5d6ae59bd.pin", "0ec5d6ae59bd.geo", "0ec5d6ae59bd.user"])
#%%
def message_generator(consumer):
    for message in consumer:
        yield message    

# %%
