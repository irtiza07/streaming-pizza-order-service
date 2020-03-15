from pykafka import KafkaClient
from enum import Enum

client = KafkaClient(hosts="127.0.0.1:9092")
ORDER_RECEIVED_TOPIC = client.topics[b"order_received"]
MARKETING_TOPIC = client.topics[b"send_marketing_email"]

class EmailType(Enum):
  PROMOTIONAL = 'promotional',
  COUPON = 'coupon', 
  FREE_DELIVERY = 'free_delivery'