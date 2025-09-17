# Worker Service (Utilities)

## Service Description

The Worker Service is a consolidated, lightweight background worker responsible for handling miscellaneous, low-priority asynchronous tasks for the platform. It runs multiple Kafka consumers concurrently within a single process to efficiently manage system utilities like sending emails and processing alerts.

## Role in System Architecture (Execution Flow)

This service is a general-purpose utility consumer.

*   **Upstream Triggers:**
    *   Consumes messages from the `email_topic` on **Kafka**, published by the `identity_service`.
    *   Consumes messages from the `dlq_topic` on **Kafka**, published by the `orchestration_service` on terminal job failure.
*   **Downstream Dependencies:**
    *   *(Future)* An external email provider (e.g., SendGrid, AWS SES).
    *   *(Future)* An external alerting system (e.g., PagerDuty, Slack).

## Core Features & Workflow Explanation

This service uses `asyncio.gather` to run two independent, long-lived tasks in a single process.

1.  **Email Worker Task:**
    *   Connects to Kafka as part of the `email_group`.
    *   Consumes messages from the `email_topic`.
    *   Each message contains a recipient email and an OTP.
    *   For the POC, it logs the action of sending an email to the console. In production, this would be replaced with a call to an email API.
2.  **DLQ Alerter Task:**
    *   Connects to Kafka as part of the `dlq_alerter_group`.
    *   Consumes messages from the `dlq_topic`.
    *   Each message represents a job that has failed all its retries.
    *   It logs the failed job details with a `CRITICAL` log level. In production, this would trigger an alert to an on-call engineer via a service like PagerDuty.

## Getting Started (Local Setup)

### Prerequisites

*   Python 3.11+
*   A running Kafka instance.

### Installation

1.  Navigate to the `Backend` directory and activate the virtual environment:
    ```bash
    cd ~/Backend
    source venv/bin/activate
    ```
2.  Install dependencies from the root `requirements.txt` file.

### Configuration

Create a `.env` file in the `worker_service` directory.

**.env.example**
```ini
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SENDER_EMAIL=noreply@farmvidhya.ai
```

### Running the Service

This is a background worker, not a web server.

```bash
# Navigate to the service directory
cd worker_service

# Run the worker script
python worker.py
```
The worker will start and log the status of both concurrent tasks.

### Running Tests

*(Test suite to be implemented)*

## Technical Dependencies

*   **Message Queue:** Kafka
*   **Key Libraries:**
    *   `aiokafka` (for running concurrent async consumers)

## API Endpoints

This service has **no API endpoints**. It operates solely as a Kafka consumer.

## Observability

*   **Logging:** Uses `structlog` for structured logs. Key events are categorized by task (email vs. DLQ). The most important log event is the `CRITICAL` log for any message received from the DLQ.
*   **Metrics:** *(To be implemented)* Key metrics: number of emails sent, number of critical alerts triggered, and Kafka consumer lag for both topics.
*   **Error Handling:** Each consumer task runs in its own `try...finally` block. If one consumer task were to fail, the other would continue running until the parent process is stopped (thanks to `asyncio.gather`).
