import os
import json
import time
import pytest
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Сценарий C: search_indexer и notification_sender оба получают сообщение
def test_fanout():
    test_doc_id = f"test-fanout-{int(time.time())}"
    
    # 1. Отправляем документ в топик processed-documents
    processed_data = {
        "document_id": test_doc_id,
        "title": "О тестировании fan-out",
        "document_type": "federal_law",
        "processed_text": "Статья 1. Тест.",
        "tags": ["тест", "fan-out"],
        "processed_at": "2025-01-01T12:00:00"
    }
    
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    producer.produce("processed-documents", key=test_doc_id.encode('utf-8'), value=json.dumps(processed_data).encode('utf-8'))
    producer.flush()
    
    # 2. Проверяем, что обе группы (indexer и notification) получают сообщение
    # Создаем двух consumer'ов с разными group.id
    group_indexer = f"test-cg-indexer-{int(time.time())}"
    group_notifier = f"test-cg-notifier-{int(time.time())}"
    
    def consume_one(group_id):
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(["processed-documents"])
        
        start_time = time.time()
        found = False
        
        while not found and (time.time() - start_time) < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
                
            data = json.loads(msg.value().decode('utf-8'))
            if data.get("document_id") == test_doc_id:
                found = True
                
        consumer.close()
        return found
        
    # Проверяем, что обе группы получили сообщение
    assert consume_one(group_indexer), "Indexer не получил сообщение"
    assert consume_one(group_notifier), "Notifier не получил сообщение"
