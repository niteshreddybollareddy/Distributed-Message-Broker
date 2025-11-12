# Project Blueprint: Ultra Low-Latency Distributed Message Broker

## 🎯 Goal
Architect a highly scalable distributed event streaming broker optimized for zero-allocation, mechanical sympathy, and raw Linux kernel I/O bypassing via Java 22's FFM API.

## 🛠 Tech Stack
- **Language:** Java 22 (using Project Panama / FFM API)
- **Network & I/O:** Linux `io_uring`, Netty
- **Core Patterns:** LMAX Disruptor Pattern, Lock-Free Concurrency, JNI-Bypass
- **Concepts:** True Zero-copy, Garbage Collection (GC) avoidance, custom binary protocols

## ✅ Acceptance Criteria & Target Metrics

### 1. Throughput per Node (The io_uring Engine)
- **Target:** Process **> 1,000,000 messages/sec** per single node (writing to NVMe SSD) with JVM CPU utilization **< 30%**.
- **Implementation:** Replace standard Java NIO (`MappedByteBuffer`/mmap) with `io_uring`. Use Java 22's Foreign Function & Memory API (Project Panama) to interface directly with `liburing.so`, bypassing standard JNI overhead.
- **Requirements:** Must achieve true zero-copy asynchronous direct disk I/O, entirely avoiding OS page fault latency spikes that plague standard mmap implementations under high memory pressure.

### 2. Inter-Thread Messaging (The 133.8ns Benchmark)
- **Target:** Inter-thread contention latency must be `~133.8ns` per publish operation.
- **Implementation:** Implement the **LMAX Disruptor pattern** (a lock-free RingBuffer).
- **Requirements:** Prove mechanical sympathy by explicitly adding primitive padding to `MessageEvent` classes to prevent CPU cache-line false sharing. Absolutely zero GC pauses in the message routing hot path.

### 3. Replication & Networking
- **Target:** Strict global ordering and robust consumer group consistency.
- **Implementation:** Deterministic leader-follower replication over raw TCP using Netty.
- **Requirements:** Bypass HTTP/JSON abstractions entirely. Design a raw, typed binary protocol. Implement high-watermark tracking so consumers only read fully replicated data.

### ⚠️ Interview "Truth" & Edge Cases (Be prepared to answer)
When asked what makes this strong, explain:
- **Mechanical Sympathy:** You respect the CPU cache (padding for false sharing) and OS mechanisms.
- **Modern API:** Using Java 22 FFM API puts you on the bleeding edge of the JVM.

Be ready to explain the trade-offs:
- **Consensus:** The leader-follower topology is hardcoded. Explain that you would use Raft or ZooKeeper in a production environment for automatic leader election.
- **Durability:** Discuss the trade-off between strict `fsync()` durability and low-latency asynchronous I/O.
- **Backpressure:** Be ready to discuss how Netty buffers filling up on slow consumers could lead to OOM on the leader, and how you would implement flow control.
