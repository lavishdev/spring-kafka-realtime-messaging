# SpringAppUsingKafka

A Spring Boot application demonstrating real-time messaging using **Apache Kafka** in KRaft mode (no ZooKeeper). Messages are produced via a REST API and consumed asynchronously by a listener service.

---

## Tech Stack

| Technology       | Version  |
|------------------|----------|
| Java             | 25       |
| Spring Boot      | 3.5.0    |
| Spring Kafka     | (managed by Boot) |
| Apache Kafka     | 7.5.0 (Confluent) |
| Lombok           | (managed by Boot) |
| Maven            | Wrapper  |
| Docker / Compose | -        |

---

## Project Structure

```
src/main/java/com/lavish/springappusingkafka/
│
├── controller/
│   └── MessageController.java        # REST endpoints
│
├── service/
│   ├── KafkaProducerService.java     # Sends messages to Kafka topic
│   └── KafkaRealtimeNotificationConsumerService.java  # Listens & processes messages
│
├── model/
│   └── Message.java                  # Kafka message payload
│
└── DTO/
    └── messageRequest.java           # Incoming REST request body
```

---

## How It Works

```
REST Client
    │
    ▼  POST /api/message/sendmessage
MessageController
    │
    ▼
KafkaProducerService  ──────────►  Kafka Topic: driver-location
                                          │
                                          ▼
                         KafkaRealtimeNotificationConsumerService
                                  (group: demo-group)
```

1. Client sends a POST request with `messagecontent` and `sender`.
2. Controller generates a UUID as the message key and calls the producer.
3. Producer publishes a `Message` object to the `driver-location` topic.
4. Consumer listens on the same topic, logs metadata, and processes the message.

---

## Prerequisites

- [Docker & Docker Compose](https://docs.docker.com/get-docker/)
- Java 25+
- Maven 3.x

---

## Getting Started

### 1. Start Kafka (KRaft mode — no ZooKeeper needed)

```bash
docker-compose up -d
```

This starts a single-node Kafka broker on `localhost:9092` using Confluent's KRaft image.

Wait for the health check to pass:

```bash
docker ps   # STATUS should show "healthy"
```

### 2. Build and Run the Application

```bash
./mvnw spring-boot:run
```

The app starts on `http://localhost:8080`.

---

## API Reference

### Health Check

```
GET /api/message/checkhealth
```

**Response:**
```
200 OK
Kafka Server is up and running
```

---

### Send a Message

```
POST /api/message/sendmessage
Content-Type: application/json
```

**Request Body:**
```json
{
  "messagecontent": "Driver is 2 km away",
  "sender": "driver-service"
}
```

**Response:**
```
200 OK
Message sent with Id: <uuid>
```

---

## Kafka Configuration

Configured in `src/main/resources/application.yml`:

| Setting                   | Value                                      |
|---------------------------|--------------------------------------------|
| Bootstrap Server          | `localhost:9092`                           |
| Topic                     | `driver-location`                          |
| Consumer Group            | `demo-group`                               |
| Producer Value Serializer | `JsonSerializer`                           |
| Consumer Value Deserializer | `JsonDeserializer`                       |
| Trusted Packages          | `com.lavish.springappusingkafka.model`     |
| Producer Acks             | `all` (strongest durability guarantee)     |
| Producer Retries          | `3`                                        |
| Auto Offset Reset         | `earliest`                                 |

---

## Docker Compose Details

The `docker-compose.yml` runs Kafka in **KRaft mode** (no ZooKeeper dependency):

- **Image:** `confluentinc/cp-kafka:7.5.0`
- **Ports:** `9092` (external), `9093` (controller)
- **Auto topic creation:** enabled
- **Health check:** `kafka-broker-api-versions` command every 10s

---

## Known Issues / Improvements

- `messageRequest.java` — class name should follow Java convention and be renamed to `MessageRequest`.
- Logger in `KafkaRealtimeNotificationConsumerService` mixes `{}` placeholders with string concatenation on the topic/partition/offset log line — this should use proper parameterized logging.
- `KafkaProducerService` — the constant `Topic` should be renamed to `TOPIC` per Java naming conventions.
- No dead-letter topic (DLT) configured for failed message processing.
- No retry/backoff policy defined for the consumer.

---

## License

This project is for learning/demonstration purposes.
