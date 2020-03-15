import config
import json
import random
import uuid

USER_PREFERENCES = {}
POPULAR_PIZZAS = {}

def get_skeleton_pizzas_dict():
  return {
    'cheese_pizza' : 0,
    'veggie_pizza' : 0,
    'steak_pizza' : 0,
    'chicken_pizza' : 0,
    'pineapple_pizza': 0
  }

def update_popular_pizzas(pizza):
  if pizza in POPULAR_PIZZAS:
    POPULAR_PIZZAS[pizza] = POPULAR_PIZZAS[pizza] + 1
  else:
    POPULAR_PIZZAS.update({pizza : 1})

def update_user_preference(user_id, pizza):
  if user_id not in USER_PREFERENCES:
    USER_PREFERENCES.update({ user_id : get_skeleton_pizzas_dict()})
  USER_PREFERENCES[user_id][pizza] = USER_PREFERENCES[user_id][pizza] + 1


def send_downstream_message(user_id):
  user_specific_preferences = USER_PREFERENCES[user_id]
  for pizza, count in user_specific_preferences.items():
    if count >= 5 and count % 5 == 0:
      email_info = {
        'user_id': user_id, 
        'pizza' : pizza,
      }
      with config.MARKETING_TOPIC.get_sync_producer() as producer:
        payload = json.dumps(email_info).encode('utf-8')
        producer.produce(payload)


def run_service():
  consumer = config.ORDER_RECEIVED_TOPIC.get_simple_consumer()
  for message in consumer:
    decoded_payload = json.loads(message.value.decode('utf-8'))
    print("Order received: ", decoded_payload)
    user_id = decoded_payload['user_id']
    pizza = decoded_payload['pizza']

    update_popular_pizzas(pizza)
    update_user_preference(user_id, pizza)

    send_downstream_message(user_id)



