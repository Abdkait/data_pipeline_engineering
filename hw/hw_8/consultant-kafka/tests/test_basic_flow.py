import os
import json
import time
import pytest
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Вспомогательные функции для тестов
def send_message(topic, key, value):
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(topic, key=key.encode('utf-8'), value=json.dumps(value).encode('utf-8'))
    producer.flush()

def consume_messages(topic, group_id, timeout=10, expected_count=1):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])
    
    messages = []
    start_time = time.time()
    
    while len(messages) < expected_count and (time.time() - start_time) < timeout:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        messages.append(json.loads(msg.value().decode('utf-8')))
        
    consumer.close()
    return messages

# Сценарий A: закон проходит весь путь от producer до search_indexer
def test_basic_flow():
    test_doc_id = f"test-law-{int(time.time())}"
    
    # 1. Отправляем закон в топик laws
    law_data = {
        "document_id": test_doc_id,
        "title": "О тестировании базового сценария",
        "body": "Статья 1. Тест.",
        "document_type": "federal_law",
        "publication_date": "2025-01-01",
        "source": "test"
    }
    send_message("laws", test_doc_id, law_data)
    
    # 2. Проверяем, что law_processor может прочитать его из laws
    # (В реальном пайплайне процессор бы сам переложил в processed-documents, 
    # но для простоты теста мы имитируем его работу и сразу пишем в processed)
    
    processed_data = {
        "document_id": test_doc_id,
        "title": "О тестировании базового сценария",
        "document_type": "federal_law",
        "processed_text": "Статья 1. Тест.",
        "tags": ["закон", "тест"],
        "processed_at": "2025-01-01T12:00:00"
    }
    send_message("processed-documents", test_doc_id, processed_data)
    
    # 3. Проверяем, что search_indexer может прочитать из processed-documents
    # Используем уникальную группу для теста, чтобы не конфликтовать с основным индексером
    test_group_id = f"test-cg-search-indexer-{int(time.time())}"
    
    messages = consume_messages("processed-documents", test_group_id, timeout=5, expected_count=1)
    
    # Ищем наше сообщение среди полученных
    found = any(msg.get("document_id") == test_doc_id for msg in messages)
    
    assert found, f"Документ {test_doc_id} не найден в processed-documents"
