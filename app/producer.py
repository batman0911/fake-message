from confluent_kafka import Producer
from common.config_loader import Config
from common.Singleton import Singleton
import random

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
    producer = KafkaProducer().producer()
    for i in range(1000):
        cust_id = random.randint(1, 1e6)
        source = random.randint(1, 64)
        target = random.randint(1, 64)
        msg = f"'customer_id': {cust_id}, 'source': {source}, 'target': {target}"
        key = f'key_{i}'
        producer.produce("python-kafka", key=key, value=msg, callback=delivery_report)
    producer.flush()