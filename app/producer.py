from confluent_kafka import Producer
from common.config_loader import Config
from common.Singleton import Singleton
import random
import time
import json


conf = Config().conf()

NUM_CUSTOMER = conf.get('customer').get('total')
MAX_WAITING_TIME = conf.get('customer').get('max_waiting_time')
STAYED_PROP = conf.get('customer').get('stayed_prop')

customer_state = list()


def init_state():
    for i in range(NUM_CUSTOMER):
        customer_state.append(random.randint(0, 62))


class KafkaProducer(metaclass=Singleton):
    __producer = Producer({'bootstrap.servers': conf.get('kafka').get('bootstrap.servers')})

    def __init__(self):
        """load config from app.yml and init kafka producer"""
        bootstrap_server = conf.get("kafka").get("bootstrap.servers")
        print(f'start init kafka server: {bootstrap_server}')
        self.__producer = Producer({'bootstrap.servers': bootstrap_server})
        
        print(self.__producer)

    def producer(self):
        return self.__producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        
        
def push_messages(running, producer, topic):
    while running:
        wait_time = random.randint(0, MAX_WAITING_TIME)
        time.sleep(wait_time * 1e-3)
        cust_id = random.randint(0, NUM_CUSTOMER - 1)
        source = customer_state[cust_id]
        target =  source if random.random() < STAYED_PROP else random.randint(0, 62)
        
        key = str(cust_id)
        msg = {
            'customer_id': cust_id,
            'source': source,
            'target': target
        }
        
        producer.produce(topic, key=key, value=json.dumps(msg), callback=delivery_report)
        
        # update current state of customer
        customer_state[cust_id] = target
        print(f'sent message, {msg}')


if __name__ == '__main__':
    
    init_state()
    
    topic = conf.get('kafka').get('topic')

    print(f'start kafka producer, topic: {topic}')
    
    producer = KafkaProducer().producer()
    running = True
    
    push_messages(running, producer, topic)

    
        
