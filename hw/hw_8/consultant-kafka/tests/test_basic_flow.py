import os
import json
import time
import pytest
import threading
from confluent_kafka import Producer, Consumer
from consumers.law_processor import consume_and_process

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
    
    # 1. Запускаем law_processor в отдельном потоке
    stop_event = threading.Event()
    test_processor_group = f"test-cg-laws-processor-{int(time.time())}"
    processor_thread = threading.Thread(
        target=consume_and_process, 
        args=(stop_event,),
        kwargs={'group_id': test_processor_group}
    )
    processor_thread.start()
    time.sleep(3)  # Ждём пока процессор подпишется на топик
    
    try:
        # 2. Отправляем закон в топик laws
        law_data = {
            "document_id": test_doc_id,
            "title": "О тестировании базового сценария налоги",
            "body": "Статья 1. Тест.",
            "document_type": "federal_law",
            "publication_date": "2025-01-01",
            "source": "test"
        }
        send_message("laws", test_doc_id, law_data)
        
        # 3. Проверяем, что в "processed-documents" появился обработанный документ с тегами
        test_group_id = f"test-cg-search-indexer-{int(time.time())}"
        
        messages = consume_messages("processed-documents", test_group_id, timeout=15, expected_count=1)
        
        # Ищем наше сообщение среди полученных
        found_msg = next((msg for msg in messages if msg.get("document_id") == test_doc_id), None)
        
        assert found_msg is not None, f"Документ {test_doc_id} не найден в processed-documents"
        assert "налоги" in found_msg.get("tags", []), "Тег 'налоги' не был добавлен процессором"
        assert found_msg.get("processed_text") is not None, "Текст не был обработан"
        
    finally:
        # 4. Останавливаем law_processor
        stop_event.set()
        processor_thread.join(timeout=5)
