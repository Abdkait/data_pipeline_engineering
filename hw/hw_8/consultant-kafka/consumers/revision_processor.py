import os
import json
from datetime import datetime
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "revisions"
OUTPUT_TOPIC = "processed-documents"
GROUP_ID = "cg-revision-processor"

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки в processed-documents: {err}")
    else:
        print(f"-> Успешно отправлено в {msg.topic()} [{msg.partition()}]")

def process_revision(revision_data):
    """ Имитация обработки редакции """
    doc_id = revision_data.get("document_id")
    rev_number = revision_data.get("revision_number")
    change_desc = revision_data.get("change_description", "")
    
    tags = ["редакция", f"ред. №{rev_number}"]
    
    return {
        "document_id": doc_id,
        "title": f"Редакция №{rev_number} документа {doc_id}",
        "document_type": "revision",
        "processed_text": f"Изменения: {change_desc}",
        "tags": tags,
        "processed_at": datetime.now().isoformat()
    }

def consume_and_process():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([INPUT_TOPIC])
    
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all'
    }
    producer = Producer(producer_conf)

    print(f"Запуск Revision Processor (группа: {GROUP_ID}). Ожидание сообщений...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")
                continue

            val = msg.value().decode('utf-8')
            revision_data = json.loads(val)
            doc_id = revision_data.get("document_id")
            rev_number = revision_data.get("revision_number")
            
            print(f"Получена редакция №{rev_number} для документа: {doc_id}")
            
            processed_data = process_revision(revision_data)
            print(f"Обработана редакция {doc_id} (№{rev_number}), добавлены теги: {processed_data['tags']}")
            
            key = doc_id.encode('utf-8')
            value = json.dumps(processed_data, ensure_ascii=False).encode('utf-8')
            
            producer.produce(OUTPUT_TOPIC, key=key, value=value, callback=delivery_report)
            producer.poll(0)
            
    except KeyboardInterrupt:
        print("Остановка процессора...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    consume_and_process()
