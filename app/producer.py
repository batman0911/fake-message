from confluent_kafka import Producer
from common.config_loader import Config
from common.Singleton import Singleton
import random
import time
import json


conf = Config().conf()


class KafkaProducer(metaclass=Singleton):
    __producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def __init__(self):
        """load config from app.yml and init kafka producer"""
        bootstrap_server = conf.get("kafka").get("bootstrap.servers")
        print(f'start init kafka server: {bootstrap_server}')
        self.__producer = Producer({'bootstrap.servers': bootstrap_server})

    def producer(self):
        return self.__producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == '__main__':
    topic = conf.get('kafka').get('topic')
    
    print(f'start kafka producer, topic: {topic}')
    
    producer = KafkaProducer().producer()
    running = True
    
    while running:
        wait_time = random.randint(1, 1000)
        time.sleep(wait_time * 1e-3)
        cust_id = random.randint(1, 1e6)
        source = random.randint(1, 64)
        target = random.randint(1, 64)
        
        key = str(cust_id)
        msg = {
            'customer_id': cust_id,
            'source': source,
            'target': target
        }
        
        producer.produce(topic, key=key, value=json.dumps(msg), callback=delivery_report)
        print(f'sent, key: {key}')
