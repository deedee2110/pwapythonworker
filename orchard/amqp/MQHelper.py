from typing import List

import pika

from orchard.settings import config


class MQHelper():
    """AMQP Helper"""

    def __init__(self, queue_name = None):
        self.connection = None
        self.channel = None
        self.MQ_HOST = config.MQ_HOST
        self.MQ_PORT = config.MQ_PORT
        self.MQ_USER = config.MQ_USER
        self.MQ_PASS = config.MQ_PASS
        self.MQ_EXCHANGE = config.MQ_EXCHANGE if config.MQ_EXCHANGE != '' else ''
        self.MQ_EXCHANGE_TYPE = config.MQ_EXCHANGE_TYPE if config.MQ_EXCHANGE_TYPE != '' else 'topic'
        self.queue_name = config.MQ_QUEUE_NAME
        if queue_name is not None:
            self.queue_name = queue_name
        print('MQHelper for AMQP (by Krerk Piromsopa, Ph.D.')
        print('MQHelper')
        print('\tMQ_HOST\t',config.MQ_HOST)
        print('\tMQ_EXCHANGE\t', config.MQ_EXCHANGE)
        print('\tMQ_QUEUE_NAME\t', config.MQ_QUEUE_NAME)

    def get_channel(self):
        if self.channel is not None:
            return self.channel
        credentials = pika.PlainCredentials(self.MQ_USER, self.MQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.MQ_HOST,
                port=self.MQ_PORT,
                virtual_host='/',
                credentials=credentials
            )
        )
        self.connection = connection
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)
        if self.MQ_EXCHANGE != '':
            # create exchange
            channel.exchange_declare(exchange=self.MQ_EXCHANGE,
                                     exchange_type=self.MQ_EXCHANGE_TYPE)
        self.channel = channel
        return channel

    def close(self):
        connection = self.connection
        connection.close()
        self.connection = None
        self.channel = None

    def publish(self, message, routing_key=None):
        m_routing_key=self.queue_name
        if routing_key is not None:
            m_routing_key = routing_key
        try:
            channel = self.get_channel()
            channel.basic_publish(
                exchange=self.MQ_EXCHANGE,
                routing_key=m_routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )
            print("amqp - sent %r" % message)
            self.close()
            return True
        except:
            print("amqp - Error sending message")
            return False

    def start_consume(self, callback, binding_keys: List[str] = []):
        try:
            channel = self.get_channel()
        except:
            print("amqp - Error sending message")
            return False
        # binding
        for binding_key in binding_keys:
            channel.queue_bind(exchange=self.MQ_EXCHANGE,
                               queue=self.queue_name,
                               routing_key=binding_key)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)

        channel.start_consuming()

# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body.decode())
#     time.sleep(body.count(b'.'))
#     print(" [x] Done")
#     ch.basic_ack(delivery_tag=method.delivery_tag)
