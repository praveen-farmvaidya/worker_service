from dotenv import load_dotenv
import os
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

import json
import structlog
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from azure.storage.queue.aio import QueueClient
from azure.core.exceptions import ResourceExistsError
from uuid import UUID

from . import db_utils, kafka_utils

# --- Configuration ---
log = structlog.get_logger()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_UPLOAD_QUEUE_NAME = os.getenv("AZURE_STORAGE_UPLOADS_QUEUE", "upload-notifications")

# --- Consumer Task 1: Email Worker (Unchanged) ---
async def email_worker_task():
    # ... (This function's code is correct and remains the same)
    log.info("Email worker task starting...")
    consumer = AIOKafkaConsumer("email_topic", bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='email_group', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    await consumer.start()
    log.info("✅ Email worker task connected to Kafka.")
    try:
        async for message in consumer:
            task = message.value
            if task.get("email_type") == "otp_verification":
                log.info("📧 SIMULATING SENDING EMAIL", sender=SENDER_EMAIL, recipient=task.get("recipient"), subject="Your FarmVidhya Verification Code", body=f"Your OTP is: {task.get('payload', {}).get('otp')}")
    finally:
        log.warning("Email worker task shutting down.")
        await consumer.stop()


# --- Consumer Task 2: DLQ Alerter (Unchanged) ---
async def dlq_alerter_task():
    # ... (This function's code is correct and remains the same)
    log.info("DLQ Alerter task starting...")
    consumer = AIOKafkaConsumer("dlq_topic", bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='dlq_alerter_group', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    await consumer.start()
    log.info("✅ DLQ Alerter task connected to Kafka.")
    try:
        async for message in consumer:
            failed_job = message.value
            log.critical("🚨 CRITICAL FAILURE: JOB IN DLQ", job_id=failed_job.get("job_id"), user_id=failed_job.get("user_id"), reason=failed_job.get("failure_reason"), job_details=failed_job)
    finally:
        log.warning("DLQ Alerter task shutting down.")
        await consumer.stop()


# --- Consumer Task 3: Azure Queue Listener (DEFINITIVELY CORRECTED) ---
async def azure_queue_listener_task():
    log.info("Azure Queue listener task starting...")
    if not AZURE_STORAGE_CONNECTION_STRING:
        log.error("Azure Storage Connection String not configured. Listener cannot start.")
        return

    queue_client = QueueClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING, queue_name=AZURE_UPLOAD_QUEUE_NAME)
    
    try:
        await queue_client.create_queue()
        log.info("Azure Queue created or already exists.", queue_name=AZURE_UPLOAD_QUEUE_NAME)
    except ResourceExistsError:
        log.info("Azure Queue already exists.", queue_name=AZURE_UPLOAD_QUEUE_NAME)
    except Exception as e:
        log.critical("Failed to connect/create Azure Queue. Uploads will not be processed.", error=str(e))
        return

    while True:
        try:
            # --- vvv THIS IS THE FINAL, CORRECT SYNTAX vvv ---
            # 'receive_messages' returns an async iterator. We loop over it with 'async for'.
            messages = queue_client.receive_messages(messages_per_page=10, visibility_timeout=60)
            async for msg in messages:
            # --- ^^^ END OF FINAL, CORRECT SYNTAX ^^^ ---
                try:
                    log.info("Received message from Azure Queue.", message_id=msg.id)
                    event_data_list = json.loads(msg.content)
                    if not isinstance(event_data_list, list): continue
                    
                    event_data = event_data_list[0]
                    
                    if event_data.get("eventType") == "Microsoft.Storage.BlobCreated":
                        blob_url = event_data.get('data', {}).get('url')
                        if not blob_url: continue
                        
                        parts = blob_url.split('/')
                        # URL format: https://<account>.blob.core.windows.net/<container>/<job_id>/<filename>
                        if len(parts) >= 6 and parts[3] == os.getenv("AZURE_STORAGE_UPLOADS_CONTAINER", "uploads"):
                            job_id_str = parts[4]
                            job_id = UUID(job_id_str)
                            
                            updated = await db_utils.update_job_status_from_upload(job_id, "PENDING")
                            if updated:
                                job_details = await db_utils.get_job_details(job_id)
                                if job_details:
                                    job_message = {"job_id": str(job_details.id), "user_id": str(job_details.user_id), "blob_name": job_details.blob_name, "model_identifier": job_details.model_identifier, "workflow_type": job_details.workflow_type, "input_data": job_details.input_data, "retry_count": 0}
                                    published = await kafka_utils.publish_job_to_orchestrator(job_message)
                                    if published:
                                        await queue_client.delete_message(msg)
                                        log.info("Successfully processed upload notification.", job_id=job_id_str)
                except Exception as e:
                    log.error("Error processing Azure Queue message.", error=str(e), message_id=msg.id)
        except Exception as e:
            log.error("Error polling Azure Queue.", error=str(e))
            await asyncio.sleep(10)

# --- Main Entry Point (Unchanged) ---
async def main():
    log.info("Starting consolidated worker service...")
    await asyncio.gather(email_worker_task(), dlq_alerter_task(), azure_queue_listener_task())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KafkaError, KeyboardInterrupt) as e:
        log.error("Worker service stopped.", reason=str(e))
    finally:
        asyncio.run(kafka_utils.stop_kafka_producer())
