import os
import json
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "processed-documents"
GROUP_ID = "cg-search-indexer"

def consume_and_index():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"Запуск Search Indexer (группа: {GROUP_ID}). Ожидание сообщений...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")
                continue

            val = msg.value().decode('utf-8')
            doc_data = json.loads(val)
            doc_id = doc_data.get("document_id")
            doc_type = doc_data.get("document_type")
            
            # Имитация индексации (просто логируем)
            print(f"[ИНДЕКСАЦИЯ] Документ {doc_id} (тип: {doc_type}) успешно добавлен в поисковый индекс. Теги: {doc_data.get('tags', [])}")
            
    except KeyboardInterrupt:
        print("Остановка индексера...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_index()
