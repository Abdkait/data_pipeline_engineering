.PHONY: up down setup-topics produce-laws produce-courts produce-revisions produce-events run-law-processor run-court-processor run-revision-processor run-indexer run-notifier run-analytics test wait-kafka setup

up:
	docker-compose up -d

down:
	docker-compose down -v

wait-kafka:
	python wait_for_kafka.py

setup-topics:
	python setup_topics.py

setup: wait-kafka setup-topics

produce-laws:
	python producers/law_producer.py

produce-courts:
	python producers/court_producer.py

produce-revisions:
	python producers/revision_producer.py

produce-events:
	python producers/user_event_producer.py

run-law-processor:
	python consumers/law_processor.py

run-court-processor:
	python consumers/court_processor.py

run-revision-processor:
	python consumers/revision_processor.py

run-indexer:
	python consumers/search_indexer.py

run-notifier:
	python consumers/notification_sender.py

run-analytics:
	python consumers/analytics_consumer.py

test:
	pytest tests/ -v
