import os
import json
from datetime import datetime
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = "court-decisions"
OUTPUT_TOPIC = "processed-documents"
GROUP_ID = "cg-court-processor"

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки в processed-documents: {err}")
    else:
        print(f"-> Успешно отправлено в {msg.topic()} [{msg.partition()}]")

def process_court_decision(decision_data):
    """ Имитация обработки судебного решения """
    case_id = decision_data.get("case_id")
    court_name = decision_data.get("court_name", "")
    decision_text = decision_data.get("decision_text", "")
    
    # Простая имитация тегирования
    tags = ["суд", "арбитраж"]
    if "удовлетворить" in decision_text.lower():
        tags.append("иск удовлетворен")
    elif "отказать" in decision_text.lower():
        tags.append("в иске отказано")
        
    return {
        "document_id": case_id,
        "title": f"Решение по делу {case_id} ({court_name})",
        "document_type": "court_decision",
        "processed_text": decision_text[:100] + "...",
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

    print(f"Запуск Court Processor (группа: {GROUP_ID}). Ожидание сообщений...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Consumer: {msg.error()}")
                continue

            val = msg.value().decode('utf-8')
            decision_data = json.loads(val)
            case_id = decision_data.get("case_id")
            
            print(f"Получено судебное решение: {case_id}")
            
            processed_data = process_court_decision(decision_data)
            print(f"Обработано решение {case_id}, добавлены теги: {processed_data['tags']}")
            
            key = case_id.encode('utf-8')
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
