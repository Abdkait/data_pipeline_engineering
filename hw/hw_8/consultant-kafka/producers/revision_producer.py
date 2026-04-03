import os
import json
import time
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "revisions"

fake = Faker("ru_RU")

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Редакция доставлена в {msg.topic()} [{msg.partition()}]")

def generate_revision(doc_id=None, rev_number=1):
    if not doc_id:
        doc_id = f"law-{fake.random_number(digits=5)}"
    
    return {
        "document_id": doc_id,
        "revision_number": rev_number,
        "change_description": f"Изменена статья {fake.random_int(min=1, max=100)}",
        "effective_date": fake.date_between(start_date='today', end_date='+1y').isoformat(),
        "new_text": f"Статья в новой редакции: {fake.text(max_nb_chars=200)}"
    }

def produce_revisions(num_messages=10):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all'
    }
    producer = Producer(conf)
    
    print(f"Начинаем отправку {num_messages} редакций в топик '{TOPIC}'...")
    
    # Имитация отправки нескольких редакций для одних и тех же документов
    base_docs = [f"law-{fake.random_number(digits=5)}" for _ in range(3)]
    
    for i in range(num_messages):
        doc_id = base_docs[i % len(base_docs)]
        rev_number = (i // len(base_docs)) + 1
        
        revision = generate_revision(doc_id, rev_number)
        key = revision["document_id"].encode('utf-8')
        value = json.dumps(revision, ensure_ascii=False).encode('utf-8')
        
        producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
        producer.poll(0)
        time.sleep(0.5)
        
    producer.flush()
    print("Отправка завершена.")

if __name__ == "__main__":
    produce_revisions()
