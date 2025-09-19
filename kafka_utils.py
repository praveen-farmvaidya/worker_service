import os
import json
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

log = structlog.get_logger()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
JOBS_TOPIC = "jobs_topic"
producer: AIOKafkaProducer | None = None

async def get_kafka_producer():
    global producer
    if producer is None:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        await producer.start()
    return producer

async def stop_kafka_producer():
    if producer: await producer.stop()

async def publish_job_to_orchestrator(job_details: dict) -> bool:
    kafka_producer = await get_kafka_producer()
    try:
        await kafka_producer.send_and_wait(JOBS_TOPIC, value=job_details)
        log.info("🚀 Job published to orchestrator.", job_id=job_details.get("job_id"))
        return True
    except KafkaError as e:
        log.error("Failed to publish job to orchestrator.", error=str(e))
        return False