import os
import json
from datetime import datetime
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "laws"
OUTPUT_TOPIC = "processed-documents"
GROUP_ID = "cg-laws-processor"

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки в processed-documents: {err}")
    else:
        print(f"-> Успешно отправлено в {msg.topic()} [{msg.partition()}]")

def process_law(law_data):
    """ Имитация обработки закона (извлечение тегов и т.д.) """
    doc_id = law_data.get("document_id")
    title = law_data.get("title", "")
    
    # Простая имитация тегирования
    tags = ["закон"]
    if "налог" in title.lower():
        tags.extend(["налоги", "НК РФ"])
    if "улов" in title.lower() or "преступ" in title.lower():
        tags.extend(["УК РФ", "уголовное право"])
        
    return {
        "document_id": doc_id,
        "title": title,
        "document_type": law_data.get("document_type", "federal_law"),
        "processed_text": law_data.get("body", "")[:100] + "...",
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

    print(f"Запуск Law Processor (группа: {GROUP_ID}). Ожидание сообщений...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")
                continue

            # Получаем сообщение
            val = msg.value().decode('utf-8')
            law_data = json.loads(val)
            doc_id = law_data.get("document_id")
            
            print(f"Получен закон: {doc_id}")
            
            # Обрабатываем
            processed_data = process_law(law_data)
            print(f"Обработан закон {doc_id}, добавлены теги: {processed_data['tags']}")
            
            # Отправляем результат
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
