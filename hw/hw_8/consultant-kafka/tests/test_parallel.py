import os
import json
import time
import pytest
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Сценарий D: 100 судебных решений распределяются по consumer'ам
def test_parallel():
    test_prefix = f"test-parallel-{int(time.time())}"
    
    # 1. Отправляем 100 судебных решений в топик court-decisions
    decisions = []
    for i in range(100):
        case_id = f"{test_prefix}-{i}"
        decisions.append({
            "case_id": case_id,
            "court_id": f"arbitr_{i % 5}",
            "court_name": "Арбитражный суд",
            "judge": "Иванов И.И.",
            "decision_date": "2025-01-20",
            "decision_text": f"Решение {i}",
            "parties": ["ООО Ромашка", "ООО Василёк"]
        })
        
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    for dec in decisions:
        producer.produce("court-decisions", key=dec["court_id"].encode('utf-8'), value=json.dumps(dec).encode('utf-8'))
    producer.flush()
    
    # 2. Создаем 3 consumer'ов в одной группе (cg-court-processor)
    # Они должны распределить партиции между собой
    group_id = f"test-cg-court-processor-{int(time.time())}"
    
    consumers = []
    for i in range(3):
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(["court-decisions"])
        consumers.append(consumer)
        
    # Ждем, пока они прочитают 100 сообщений (или таймаут)
    received_count = 0
    start_time = time.time()
    
    while received_count < 100 and (time.time() - start_time) < 15:
        for consumer in consumers:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue
                
            data = json.loads(msg.value().decode('utf-8'))
            if data.get("case_id", "").startswith(test_prefix):
                received_count += 1
                
    for consumer in consumers:
        consumer.close()
        
    assert received_count == 100, f"Ожидалось 100 сообщений, получено: {received_count}"
