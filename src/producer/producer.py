# producer.py
import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

# Create a Kafka producer instance
# bootstrap_servers ooint to the Kafka port exposed in docker-compose
producer = KafkaProducer(
    bootstrap_servers='localhost:9094', # Port is 9094！
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Serialize dict into JSON
)

print("Kafka Producer started. Press Ctrl+C to stop.")

user_ids = ['user1', 'user2', 'user3', 'user4', 'user5']
event_types = ['click', 'page_view', 'purchase', 'add_to_cart']
pages = ['/home', '/products/1', '/products/2', '/cart', '/checkout']
topic_name = 'user_events'

def produce():
    # Genarate a random user event
    event_type = random.choice(event_types)
    event = {
        'user_id': random.choice(user_ids),
        'event_type': event_type,
        'timestamp': datetime.utcnow().isoformat() + 'Z', # Use ISO 8601 format
        'page': random.choice(pages),
        'value': (
            round(random.uniform(5, 100), 2)
            if (event_type == "purchase" and random.random() > 0.8)
            else None  # Simulate purchase value
        )
    }

    # Sent to 'user_events' Topic
    future = producer.send(topic_name, value=event)

    # Block till message is send
    result = future.get(timeout=10) 

    print(f"Sent event: {event}")

def nearly_real_producer():
    while True:
        produce()
        # Sent message per 1-3 sec
        time.sleep(random.uniform(1, 3))

def quick_init():
    for _ in range(62568):
        produce()

try:
    nearly_real_producer()
    # quick_init()
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush() # flush data to queue
    producer.close()
    print("Producer closed.")