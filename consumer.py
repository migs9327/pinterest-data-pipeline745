#%%
from kafka import KafkaConsumer
from json import loads

# create our consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

data_stream_consumer.subscribe(topics=['0ec5d6ae59bd.pin', '0ec5d6ae59bd.geo', '0ec5d6ae59bd.user'])

# Loops through all messages in the consumer and prints them out individually
for message in data_stream_consumer:
    print(message.value)
    print(message.topic)
    print(message.timestamp)
# %%
