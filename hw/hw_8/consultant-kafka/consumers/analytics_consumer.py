import os
import json
import time
from collections import defaultdict
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "user-events"
GROUP_ID = "cg-analytics"

def consume_and_analyze():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"Запуск Analytics Consumer (группа: {GROUP_ID}). Ожидание событий...")

    # In-memory счетчик просмотров документов
    doc_views = defaultdict(int)
    last_print_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            
            # Периодически печатаем топ-5 (каждые 5 секунд)
            current_time = time.time()
            if current_time - last_print_time >= 5:
                if doc_views:
                    top_docs = sorted(doc_views.items(), key=lambda x: x[1], reverse=True)[:5]
                    print("\n--- ТОП-5 ПОПУЛЯРНЫХ ДОКУМЕНТОВ ---")
                    for i, (doc_id, views) in enumerate(top_docs, 1):
                        print(f"{i}. {doc_id} - просмотров: {views}")
                    print("-----------------------------------\n")
                last_print_time = current_time

            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")
                continue

            val = msg.value().decode('utf-8')
            event_data = json.loads(val)
            
            event_type = event_data.get("event_type")
            doc_id = event_data.get("document_id")
            user_id = event_data.get("user_id")
            
            # Считаем только просмотры для простоты
            if event_type == "view" and doc_id:
                doc_views[doc_id] += 1
                print(f"[АНАЛИТИКА] Зафиксирован просмотр документа {doc_id} пользователем {user_id}")
            
    except KeyboardInterrupt:
        print("Остановка аналитики...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_analyze()
