.PHONY: up down setup-topics produce-laws produce-courts produce-revisions produce-events run-law-processor run-court-processor run-revision-processor run-indexer run-notifier run-analytics test wait-kafka setup

PYTHON = python3

up:
	docker compose up -d

down:
	docker compose down -v

wait-kafka:
	$(PYTHON) wait_for_kafka.py

setup-topics:
	$(PYTHON) setup_topics.py

setup: wait-kafka setup-topics

produce-laws:
	$(PYTHON) producers/law_producer.py

produce-courts:
	$(PYTHON) producers/court_producer.py

produce-revisions:
	$(PYTHON) producers/revision_producer.py

produce-events:
	$(PYTHON) producers/user_event_producer.py

run-law-processor:
	$(PYTHON) consumers/law_processor.py

run-court-processor:
	$(PYTHON) consumers/court_processor.py

run-revision-processor:
	$(PYTHON) consumers/revision_processor.py

run-indexer:
	$(PYTHON) consumers/search_indexer.py

run-notifier:
	$(PYTHON) consumers/notification_sender.py

run-analytics:
	$(PYTHON) consumers/analytics_consumer.py

test:
	$(PYTHON) -m pytest tests/ -v
