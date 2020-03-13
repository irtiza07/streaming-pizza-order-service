from pykafka import KafkaClient

client = KafkaClient(hosts="127.0.0.1:9092")
test_topic = client.topics[b"test-topic"]

with test_topic.get_sync_producer() as producer:
  for count in range(10):
    print(count)
    msg = f"message is here {count}"
    producer.produce(msg.encode('utf-8'))

consumer = test_topic.get_simple_consumer()
print("Done producing")
print("------------------------------------------------------------------------------------------------------------------------")
print(type(consumer))

for message in consumer:
  if message is not None:
    print(message.offset, message.value)
  else:
    print("Message is none")