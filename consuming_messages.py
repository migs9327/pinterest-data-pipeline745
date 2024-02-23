#%%
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
