import time
import os
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def wait_for_kafka(timeout=60):
    print("Ожидание готовности Kafka...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            metadata = admin.list_topics(timeout=5)
            if metadata.topics is not None:
                print("Kafka готова!")
                return True
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Kafka не запустилась")

if __name__ == "__main__":
    wait_for_kafka()
