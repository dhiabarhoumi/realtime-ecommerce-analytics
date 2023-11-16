.PHONY: all setup topics produce stream dash test clean

all: setup topics produce stream dash

setup:
	pre-commit install || true
	pip install -r requirements.txt

topics:
	bash docker/kafka-init.sh

produce:
	python src/producers/replay.py --file data/events.csv.gz --rate 500

stream:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
		src/streaming/stream_job.py --brokers localhost:9092 --mongo_uri mongodb://localhost:27017 \
		--mongo_db analytics --mongo_col kpis_minute --checkpoint checkpoints/main/ --out_parquet data/curated/

dash:
	streamlit run src/dashboard/app.py

test:
	pytest -q --disable-warnings --maxfail=1 --cov=src

lint:
	ruff check src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

format:
	black src/ tests/
	isort src/ tests/
	ruff check --fix src/ tests/

clean:
	docker compose down -v
	rm -rf checkpoints/ data/curated/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -name "*.pyc" -delete