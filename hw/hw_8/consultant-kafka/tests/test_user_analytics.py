import os
import json
import time
import pytest
from collections import defaultdict
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Сценарий E: подсчёт популярных документов
def test_user_analytics():
    test_prefix = f"test-analytics-{int(time.time())}"
    
    # 1. Отправляем события просмотров в топик user-events
    events = [
        {"user_id": "user-1", "event_type": "view", "document_id": f"{test_prefix}-doc1", "timestamp": "2025-01-01T10:00:00"},
        {"user_id": "user-2", "event_type": "view", "document_id": f"{test_prefix}-doc1", "timestamp": "2025-01-01T10:05:00"},
        {"user_id": "user-3", "event_type": "view", "document_id": f"{test_prefix}-doc2", "timestamp": "2025-01-01T10:10:00"},
        {"user_id": "user-4", "event_type": "search", "document_id": f"{test_prefix}-doc3", "timestamp": "2025-01-01T10:15:00"},
        {"user_id": "user-5", "event_type": "view", "document_id": f"{test_prefix}-doc1", "timestamp": "2025-01-01T10:20:00"}
    ]
    
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    for ev in events:
        producer.produce("user-events", key=ev["user_id"].encode('utf-8'), value=json.dumps(ev).encode('utf-8'))
    producer.flush()
    
    # 2. Проверяем, что аналитика правильно считает просмотры
    group_id = f"test-cg-analytics-{int(time.time())}"
    
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(["user-events"])
    
    doc_views = defaultdict(int)
    start_time = time.time()
    
    while len(doc_views) < 2 and (time.time() - start_time) < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
            
        data = json.loads(msg.value().decode('utf-8'))
        if data.get("document_id", "").startswith(test_prefix):
            if data.get("event_type") == "view":
                doc_views[data.get("document_id")] += 1
                
    consumer.close()
    
    # doc1 должен иметь 3 просмотра, doc2 - 1 просмотр, doc3 (search) - 0
    assert doc_views[f"{test_prefix}-doc1"] == 3, f"Ожидалось 3 просмотра для doc1, получено: {doc_views[f'{test_prefix}-doc1']}"
    assert doc_views[f"{test_prefix}-doc2"] == 1, f"Ожидался 1 просмотр для doc2, получено: {doc_views[f'{test_prefix}-doc2']}"
    assert doc_views.get(f"{test_prefix}-doc3", 0) == 0, "События search не должны учитываться"
