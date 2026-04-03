import os
import json
import time
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "laws"

fake = Faker("ru_RU")

def delivery_report(err, msg):
    """ Вызывается при успешной доставке сообщения или ошибке """
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Закон доставлен в {msg.topic()} [{msg.partition()}]")

def generate_law(doc_id=None):
    if not doc_id:
        doc_id = f"law-{fake.random_number(digits=5)}"
    
    return {
        "document_id": doc_id,
        "title": f"О внесении изменений в {fake.word()} кодекс",
        "body": f"Статья 1. {fake.text(max_nb_chars=200)}",
        "document_type": "federal_law",
        "publication_date": fake.date_this_year().isoformat(),
        "source": "gosduma"
    }

def produce_laws(num_messages=10):
    # Настройки продюсера
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all'  # Гарантия at least once
    }
    producer = Producer(conf)
    
    print(f"Начинаем отправку {num_messages} законов в топик '{TOPIC}'...")
    
    for i in range(num_messages):
        law = generate_law()
        key = law["document_id"].encode('utf-8')
        value = json.dumps(law, ensure_ascii=False).encode('utf-8')
        
        producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
        producer.poll(0)  # Обработка коллбеков
        time.sleep(0.5)   # Имитация задержки
        
    producer.flush()
    print("Отправка завершена.")

if __name__ == "__main__":
    produce_laws()
