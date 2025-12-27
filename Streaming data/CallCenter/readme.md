# Callcenter ML Streaming Project

Real time data processing pipeline utilizing Apache Kafka, Docker and decision making ML algorithms, such as KNN, random forest and logistical regression.

## Used technologies
* Python 3.x
* Apache Kafka
* Docker & Docker Compose
* Scikit-learn

## How to run
1. Clone the repo: `git clone ...`
2. Install dependencies with pip: `pip install -r requirements.txt`
3. Up Kafka with Docker Compose: `docker-compose up -d`
4. Run the producer: `python streamer_kafka_producer.py`
5. Run the consumer: `python ml_streaming_processor.py`