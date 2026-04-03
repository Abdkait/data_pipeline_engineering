import os
import json
import time
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "user-events"

fake = Faker("ru_RU")

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Событие доставлено в {msg.topic()} [{msg.partition()}]")

def generate_user_event(user_id=None):
    if not user_id:
        user_id = f"user-{fake.random_number(digits=3)}"
    
    return {
        "user_id": user_id,
        "event_type": fake.random_element(elements=("view", "search", "download", "print")),
        "document_id": f"law-{fake.random_number(digits=5)}",
        "timestamp": fake.iso8601()
    }

def produce_user_events(num_messages=10):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': '1'  # Потеря не критична
    }
    producer = Producer(conf)
    
    print(f"Начинаем отправку {num_messages} событий в топик '{TOPIC}'...")
    
    for i in range(num_messages):
        event = generate_user_event()
        key = event["user_id"].encode('utf-8')
        value = json.dumps(event, ensure_ascii=False).encode('utf-8')
        
        producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
        producer.poll(0)
        time.sleep(0.5)
        
    producer.flush()
    print("Отправка завершена.")

if __name__ == "__main__":
    produce_user_events()
