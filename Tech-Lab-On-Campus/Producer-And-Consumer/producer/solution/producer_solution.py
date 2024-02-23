from producer_interface import mqProducerInterface
import pika
import os


class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        # Save parameters to instance variable
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = connection.channel()

        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(self.exchange_name)

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(self.exchange_name, self.routing_key, message)