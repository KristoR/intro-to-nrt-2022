# pip install fastavro

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from sqlalchemy import create_engine, text
import getpass
import psycopg2

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

sr = SchemaRegistryClient({"url": 'http://localhost:8081'})

# https://avro.apache.org/docs/current/spec.html
schema_str = """{
    "name": "Order",
    "type": "record",
    "fields": [
        {
            "name": "order_id",
            "type": "long"
        },
        {
            "name": "product_id",
            "type": "long"
        },
        {
            "name": "user_id",
            "type": "string"
        }
    ]
}"""

avro_serializer = AvroSerializer(sr,schema_str)

producer_conf = {'bootstrap.servers': "localhost:19092,localhost:29092",
                    'value.serializer': avro_serializer}

producer = SerializingProducer(producer_conf)

#sample = {"order_id": 100,
#"product_id": 20,
#"user_id": 1}

#producer.produce(topic="sales", value=sample, on_delivery=delivery_report)

host = ""
port = ""
db = ""
user = ""
pw = getpass.getpass()

engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:/{db')

with engine.connect() as conn:
    result = conn.execute(text("SELECT order_id, product_id, customer_id FROM practice.sales_order"))
    
for r in result:
    event = dict(r.items())
    producer.produce(topic="sales", value=event, on_delivery=delivery_report)
