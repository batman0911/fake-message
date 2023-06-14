from common.Singleton import Singleton
from common.config_loader import Config
from confluent_kafka import Consumer

conf = Config().conf()


class KafkaConsumer(metaclass=Singleton):

    __consumer = Consumer({
        'bootstrap.servers': conf.get('kafka').get('bootstrap.servers'),
        'group.id': 'linhnm',
        'auto.offset.reset': 'earliest'
    })

    def __init__(self):
        kafka_conf = conf.get("kafka")
        bootstrap_server = kafka_conf.get("bootstrap.servers")
        group_id = kafka_conf.get("group.id")
        auto_offset_reset = kafka_conf.get("auto.offset.reset") if kafka_conf.get("auto.offset.reset") is not None \
            else 'earliest'

        print(f'init kafka consumer, server: {bootstrap_server}, group-id: {group_id}')
        self.__consumer = Consumer({
            'bootstrap.servers': bootstrap_server,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset
        })
        
        print(f'init kafka consumer, server: {bootstrap_server}, topic: {topic}, group-id: {group_id}')

    def consumer(self):
        return self.__consumer


if __name__ == '__main__':
    topic = conf.get('kafka').get('topic')
    
    consumer = KafkaConsumer().consumer()
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'consumer error: {msg.error()}')
            continue

        print(f'receive message, key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}')

    consumer.close()


