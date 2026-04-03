import os
import json
import time
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "court-decisions"

fake = Faker("ru_RU")

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Судебное решение доставлено в {msg.topic()} [{msg.partition()}]")

def generate_court_decision(case_id=None):
    if not case_id:
        case_id = f"A40-{fake.random_number(digits=5)}/{fake.year()}"
    
    return {
        "case_id": case_id,
        "court_id": f"arbitr_{fake.city().lower()}",
        "court_name": f"Арбитражный суд г. {fake.city()}",
        "judge": fake.name(),
        "decision_date": fake.date_this_year().isoformat(),
        "decision_text": f"Решение: {fake.text(max_nb_chars=300)}",
        "parties": [fake.company(), fake.company()]
    }

def produce_court_decisions(num_messages=10):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all'
    }
    producer = Producer(conf)
    
    print(f"Начинаем отправку {num_messages} судебных решений в топик '{TOPIC}'...")
    
    for i in range(num_messages):
        decision = generate_court_decision()
        key = decision["court_id"].encode('utf-8')
        value = json.dumps(decision, ensure_ascii=False).encode('utf-8')
        
        producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
        producer.poll(0)
        time.sleep(0.5)
        
    producer.flush()
    print("Отправка завершена.")

if __name__ == "__main__":
    produce_court_decisions()
