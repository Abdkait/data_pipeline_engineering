import os
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def setup_topics():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    # Определяем топики с их настройками
    topics = [
        NewTopic(
            "laws", 
            num_partitions=3, 
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str(30 * 24 * 60 * 60 * 1000)  # 30 дней
            }
        ),
        NewTopic(
            "court-decisions", 
            num_partitions=6, 
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str(14 * 24 * 60 * 60 * 1000)  # 14 дней
            }
        ),
        NewTopic(
            "revisions", 
            num_partitions=3, 
            replication_factor=1,
            config={
                "cleanup.policy": "compact"
            }
        ),
        NewTopic(
            "processed-documents", 
            num_partitions=6, 
            replication_factor=1,
            config={
                "cleanup.policy": "compact"
            }
        ),
        NewTopic(
            "user-events", 
            num_partitions=3, 
            replication_factor=1,
            config={
                "cleanup.policy": "delete",
                "retention.ms": str(7 * 24 * 60 * 60 * 1000)   # 7 дней
            }
        )
    ]
    
    # Создаем топики
    fs = admin_client.create_topics(topics)
    
    # Ждем завершения операции
    for topic, f in fs.items():
        try:
            f.result()  # Ждем результат
            print(f"✅ Топик '{topic}' успешно создан.")
        except Exception as e:
            print(f"⚠️ Ошибка при создании топика '{topic}': {e}")

if __name__ == "__main__":
    print("Создание топиков Kafka...")
    setup_topics()
