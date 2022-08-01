# pip install confluent-kafka
 
import requests
from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def fetch_wiki_edits()-> list:
    # https://www.mediawiki.org/w/api.php?action=help&modules=query%2Brecentchanges
    url = "https://en.wikipedia.org/w/api.php"

    params = dict()
    params["action"] = "query"
    params["list"] = "recentchanges"
    params["rclimit"] = "100"
    params["rcnamespace"] = "0"
    params["format"] = "json"

    r = requests.get(url, params=params)

    return r.json()["query"]["recentchanges"]

def produce_events(events:list):
    conf = {'bootstrap.servers': "localhost:19092,localhost:29092"}
    topic = "wiki"

    producer = Producer(conf)

    for i in events:
        producer.produce(topic=topic, value=str(i), callback=acked)

    producer.poll(1)
    producer.flush() 

if __name__ == '__main__':
    
    wiki_edits = fetch_wiki_edits()
    produce_events(wiki_edits)



