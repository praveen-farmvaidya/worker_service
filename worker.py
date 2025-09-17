#
# FILE: worker_service/worker.py (Corrected and Final)
#
from dotenv import load_dotenv
load_dotenv()

import os
import json
import structlog
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

# --- Configuration ---
log = structlog.get_logger()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")

# --- Consumer Task 1: Email Worker ---
async def email_worker_task():
    log.info("Email worker task starting...")
    consumer = AIOKafkaConsumer(
        "email_topic",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='email_group', # Added comma
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    log.info("✅ Email worker task connected to Kafka.")
    try:
        async for message in consumer:
            task = message.value
            recipient = task.get("email")
            otp = task.get("otp")
            
            if not recipient or not otp:
                log.error("Invalid email task received", task=task)
                continue

            log.info(
                "📧 SIMULATING SENDING EMAIL",
                sender=SENDER_EMAIL,
                recipient=recipient,
                subject="Your FarmVidhya Verification Code",
                body=f"Your OTP is: {otp}"
            )
    finally:
        log.warning("Email worker task shutting down.")
        await consumer.stop()

# --- Consumer Task 2: DLQ Alerter ---
async def dlq_alerter_task():
    log.info("DLQ Alerter task starting...")
    consumer = AIOKafkaConsumer(
        "dlq_topic",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='dlq_alerter_group', # Added comma
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    await consumer.start()
    log.info("✅ DLQ Alerter task connected to Kafka.")
    try:
        async for message in consumer:
            failed_job = message.value
            log.critical(
                "🚨 CRITICAL FAILURE: JOB IN DLQ",
                job_id=failed_job.get("job_id"),
                user_id=failed_job.get("user_id"),
                reason=failed_job.get("failure_reason"),
                job_details=failed_job
            )
    finally:
        log.warning("DLQ Alerter task shutting down.")
        await consumer.stop()

# --- Main Entry Point ---
async def main():
    log.info("Starting consolidated worker service...")
    await asyncio.gather(
        email_worker_task(),
        dlq_alerter_task()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KafkaError, KeyboardInterrupt) as e:
        log.error("Worker service stopped.", reason=str(e))
