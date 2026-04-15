import pika
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=5672)
        )
        
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type='topic',
            durable=True
        )
    
    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message.encode('utf-8')
        )
        
        self.channel.close()
        
        self.connection.close()