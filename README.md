# Build Your Own Kafka (C++)

A lightweight Kafka-compatible message broker implemented from scratch in modern C++.

This project was built to understand the internal architecture of Apache Kafka, including binary protocol parsing, concurrent networking, KRaft metadata management, and disk-backed log storage.

Unlike using the official Kafka libraries, this broker directly implements core Kafka APIs and communicates with Kafka clients using the native binary wire protocol.

---

## Features

- Implemented a multithreaded Kafka-compatible broker in modern C++17
- Supports concurrent client connections using POSIX sockets and `std::thread`
- Implements Kafka binary protocol parsing and serialization
- Reads KRaft metadata logs to discover topics and partitions
- Supports Produce API for appending RecordBatches to partition logs
- Supports Fetch API for serving persisted RecordBatches
- Implements ApiVersions and DescribeTopicPartitions APIs
- Dynamically constructs compact Kafka protocol responses
- Efficiently streams RecordBatch data directly from disk
- Uses disk-backed storage similar to Kafka log segments

---

## Supported Kafka APIs

| API | Key | Description |
|------|----:|-------------|
| ApiVersions | 18 | Negotiates supported protocol versions |
| Produce | 0 | Appends RecordBatch data to partition logs |
| Fetch | 1 | Reads RecordBatch data from disk |
| DescribeTopicPartitions | 75 | Returns topic metadata from KRaft logs |

---

## Architecture

```

Kafka Client
│
▼
TCP Socket Server
│
▼
Request Parser
│
├── ApiVersions
├── Produce
├── Fetch
└── DescribeTopicPartitions
│
▼
Metadata Manager
(KRaft Parser)
│
▼
Partition Log Storage

```

---

## Technologies Used

- C++17
- POSIX Sockets
- Multithreading (`std::thread`)
- Binary Protocol Parsing
- File I/O
- TCP Networking
- Make

---

## Building

```bash
make
```

or

```bash
g++ -std=c++17 kafka_broker.cpp -o kafka_broker -pthread
```

---

## Running

```bash
./kafka_broker
```

The broker listens on:

```
localhost:9092
```

---

## Project Structure

```
.
├── kafka_broker.cpp
├── Makefile
├── README.md
└── .gitignore
```

---

## Learning Outcomes

Through this project I gained hands-on experience with:

- Kafka wire protocol
- Binary serialization
- Concurrent server design
- Disk-backed storage engines
- TCP socket programming
- KRaft metadata parsing
- Modern C++ systems programming

---
