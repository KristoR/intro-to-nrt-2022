# task 1:

# https://randomuser.me/documentation

# Topic per nationality
# Partition by gender

import confluent_kafka
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def fetch()-> list:
    url = "https://randomuser.me/api"

    params = dict()
    params["results"] = "15"
    params["nat"] = "US,FR,DE,NZ"

    for i in range(10):
        r = requests.get(url, params=params)
        if r.status_code != 200:
            if i==9:
                print("API failed after 10 tries")
                return None
            time.sleep(1)
        else:
            break

    return r.json()["results"]

def produce_events(events:list):
    conf = {'bootstrap.servers': "localhost:19092,localhost:29092"}
    producer = Producer(conf)

    for i in events:
        topic = i["nat"] # we want to have each nationality in its own topic
        key = i["gender"] # we want to partition by gender
        producer.produce(topic="completelynew", key=key, value=str(i), callback=acked)

    producer.poll(1)
    producer.flush() 

if __name__ == '__main__':
    
    users = fetch()
    
    # create new topics
    # note: not good programming, conf and nationalities should be in a single location
    ac = AdminClient(conf={'bootstrap.servers': "localhost:19092,localhost:29092"})
    nats = ["US","FR","DE","NZ"]
    new_topics = []
    for n in nats:
        new_topics.append(NewTopic(topic=n, num_partitions=2, replication_factor=3))
        
    ac.create_topics(new_topics)

    produce_events(users)

    


