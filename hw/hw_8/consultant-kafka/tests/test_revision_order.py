import os
import json
import time
import pytest
from confluent_kafka import Producer, Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Сценарий B: редакции одного документа приходят по порядку
def test_revision_order():
    test_doc_id = f"test-law-{int(time.time())}"
    
    # 1. Отправляем 3 редакции одного документа в топик revisions
    revisions = [
        {
            "document_id": test_doc_id,
            "revision_number": 1,
            "change_description": "Первая редакция",
            "effective_date": "2025-01-01",
            "new_text": "Текст 1"
        },
        {
            "document_id": test_doc_id,
            "revision_number": 2,
            "change_description": "Вторая редакция",
            "effective_date": "2025-02-01",
            "new_text": "Текст 2"
        },
        {
            "document_id": test_doc_id,
            "revision_number": 3,
            "change_description": "Третья редакция",
            "effective_date": "2025-03-01",
            "new_text": "Текст 3"
        }
    ]
    
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    for rev in revisions:
        producer.produce("revisions", key=test_doc_id.encode('utf-8'), value=json.dumps(rev).encode('utf-8'))
    producer.flush()
    
    # 2. Проверяем, что они читаются в том же порядке
    # (Так как ключ одинаковый, они попадут в одну партицию и порядок сохранится)
    test_group_id = f"test-cg-revision-order-{int(time.time())}"
    
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': test_group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(["revisions"])
    
    received_revisions = []
    start_time = time.time()
    
    while len(received_revisions) < 3 and (time.time() - start_time) < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
            
        data = json.loads(msg.value().decode('utf-8'))
        if data.get("document_id") == test_doc_id:
            received_revisions.append(data.get("revision_number"))
            
    consumer.close()
    
    assert received_revisions == [1, 2, 3], f"Ожидался порядок [1, 2, 3], получено: {received_revisions}"
