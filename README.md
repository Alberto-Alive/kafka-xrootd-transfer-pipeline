# Kafka XRootD Transfer Pipeline

A mini-lab simulating CERN-style XRootD data-transfer events using Kafka.
Includes:

- **Python producer** generating fake XRootD transfer events  
- **Python consumer** aggregating transfer stats  
- **Prometheus** scraping consumer metrics  
- **Grafana** visualizing throughput and failures  

Designed as a hands-on exploration of event-driven monitoring pipelines in distributed systems.

## Architecture
Kafka → Consumer → Prometheus → Grafana

## Run
```bash
docker-compose up --build
```
## ✔ Project Setup & Progress Checklist

Below is a living checklist tracking the setup and development progress of the
Kafka–XRootD transfer pipeline. Tasks are marked completed as we implement them.

### 🏗 Environment & Infrastructure
- [x] Install Docker / Docker Desktop
- [x] Create and activate Python environment (`mlx`)
- [x] Install `kafka-python` and project dependencies
- [x] Fix incorrect `kafka` package import issue

### 🐳 Docker & Kafka Services
- [x] Fix Kafka advertised listeners for Windows host access
- [x] Expose Kafka correctly on `localhost:9092`
- [x] Start Zookeeper and Kafka via `docker-compose`
- [x] Resolve `NoBrokersAvailable` connection issue
- [x] Remove or fix broken `consumer` service lacking a Dockerfile
- [ ] Add working consumer implementation + Dockerfile
- [ ] Add automatic topic creation script (optional)

### 📦 Producer / Consumer
- [x] Implement functional Kafka producer
- [ ] Implement Kafka consumer (standalone or Dockerized)
- [ ] Add schema validation for producer events
- [ ] Add error handling & retries
- [ ] Benchmark throughput (optional)

### 📊 Monitoring (Prometheus & Grafana)
- [x] Include Prometheus and Grafana services in compose
- [ ] Add Prometheus Kafka exporter
- [ ] Create Grafana dashboards for:
  - [ ] Producer throughput
  - [ ] Consumer lag
  - [ ] Broker health

### 📘 Documentation & Repo Quality
- [x] Add README fixes for Windows + Docker Desktop
- [x] Add troubleshooting section
- [ ] Add full architecture diagram
- [ ] Add contribution guidelines (optional)
- [ ] Add CI workflow (GitHub Actions)

