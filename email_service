import config
import json
import random
import uuid

def send_email(user_id, pizza):
  email_type = random.choice([
    config.EmailType.PROMOTIONAL.value,
    config.EmailType.COUPON.value,
    config.EmailType.FREE_DELIVERY.value
  ])
  if email_type == config.EmailType.PROMOTIONAL.value:
    print(f"Hello {user_id}!! You gotta check our new variety of {pizza}")
  if email_type == config.EmailType.COUPON.value:
    print(f"Hello {user_id}!! Use this coupon to get your next pizza!!")
  if email_type == config.EmailType.FREE_DELIVERY.value:
    print(f"Hello {user_id}!! Free delivery when you order your next {pizza}")

def run_service():
  consumer = config.MARKETING_TOPIC.get_simple_consumer()
  for message in consumer:
    decoded_payload = json.loads(message.value.decode('utf-8'))
    print("Order received from pipeline: ", decoded_payload)
    user_id = decoded_payload['user_id']
    pizza = decoded_payload['pizza']

    send_email(user_id, pizza)



