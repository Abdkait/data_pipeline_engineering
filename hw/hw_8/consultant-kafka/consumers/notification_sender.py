import os
import json
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "processed-documents"
GROUP_ID = "cg-notification-sender"

def consume_and_notify():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"Запуск Notification Sender (группа: {GROUP_ID}). Ожидание сообщений...")

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
            title = doc_data.get("title", "")
            
            # Имитация отправки уведомлений
            print(f"[УВЕДОМЛЕНИЕ] Подписчикам отправлено уведомление о документе {doc_id} (тип: {doc_type}): '{title}'")
            
    except KeyboardInterrupt:
        print("Остановка отправителя уведомлений...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_and_notify()
