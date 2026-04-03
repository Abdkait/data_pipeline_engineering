import os
import json
import time
import pytest
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Сценарий D: 100 судебных решений распределяются по партициям и сохраняют порядок
def test_parallel():
    test_prefix = f"test-parallel-{int(time.time())}"
    
    # 1. Отправляем 100 сообщений с 5 разными court_id
    decisions = []
    for i in range(100):
        court_id = f"court_{i % 5}"
        case_id = f"{test_prefix}-{i}"
        decisions.append({
            "case_id": case_id,
            "court_id": court_id,
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
    
    # 2. Одним consumer'ом в уникальной группе читаем все 100 сообщений
    group_id = f"test-cg-court-processor-{int(time.time())}"
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(["court-decisions"])
    
    received_count = 0
    start_time = time.time()
    
    # Словарь для хранения партиции для каждого court_id
    court_partitions = {}
    # Множество всех задействованных партиций
    used_partitions = set()
    
    while received_count < 100 and (time.time() - start_time) < 15:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
            
        data = json.loads(msg.value().decode('utf-8'))
        if data.get("case_id", "").startswith(test_prefix):
            received_count += 1
            
            court_id = data.get("court_id")
            partition = msg.partition()
            
            used_partitions.add(partition)
            
            # Проверяем что сообщения с одинаковым court_id пришли из одной и той же партиции
            if court_id in court_partitions:
                assert court_partitions[court_id] == partition, f"Сообщения для {court_id} попали в разные партиции!"
            else:
                court_partitions[court_id] = partition
                
    consumer.close()
        
    assert received_count == 100, f"Ожидалось 100 сообщений, получено: {received_count}"
    
    # Проверяем что задействовано больше 1 партиции (данные реально распределились)
    assert len(used_partitions) > 1, f"Все сообщения попали в одну партицию: {used_partitions}. Ожидалось распределение."
