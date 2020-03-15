import config
import json
import random
import uuid

# For simplicity lets assume that one order can only have one pizza
NUMBER_OF_ORDERS = 2
USER_IDS = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
TYPES_OF_PIZZA = ['cheese_pizza', 'veggie_pizza', 'steak_pizza', 'chicken_pizza', 'pineapple_pizza']

def generate_pizza_order():
  return {
    'order_id' : uuid.uuid4().hex,
    'user_id' : random.choice(USER_IDS),
    'pizza' : random.choice(TYPES_OF_PIZZA)
  }

def send_message_downstream(kafka_topic, order):
  with kafka_topic.get_sync_producer() as producer:
    payload = json.dumps(order).encode('utf-8')
    producer.produce(payload)

def run_service():
  for i in range(100):
    order = generate_pizza_order()
    print(f"order number: {i} ", order)
    send_message_downstream(config.ORDER_RECEIVED_TOPIC, order)