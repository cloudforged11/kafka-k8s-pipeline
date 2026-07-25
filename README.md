# Kafka Event Pipeline on Kubernetes

A self-contained event streaming pipeline built on Kubernetes using the **Strimzi Kafka Operator**.

## Architecture

```
Producer (Java) → Kafka Topic (sample-events, 3 partitions) → Consumer (Java)
```

All components run as Kubernetes Deployments, with Kafka managed via Strimzi custom resources.

## Tech Stack

- **Kubernetes** (Minikube)
- **Strimzi Kafka Operator** (Kafka lifecycle management)
- **Apache Kafka 3.7.0** (event streaming)
- **Java 21** (producer & consumer applications)
- **Docker** (containerization with multi-stage builds)
- **Helm** (application deployment)

## Prerequisites

- Docker Desktop
- `kubectl`
- `minikube`
- `helm`
- Java JDK 17+

## Setup Instructions

### 1. Start the cluster

```bash
minikube start --cpus=4 --memory=6144
kubectl create namespace kafka
```

### 2. Install Strimzi Kafka Operator

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl get pods -n kafka --watch
# Wait until strimzi-cluster-operator pod is Running
```

### 3. Deploy Kafka cluster and topics

```bash
kubectl apply -f k8s/kafka-cluster.yaml -n kafka
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
kubectl apply -f k8s/topics.yaml -n kafka
```

### 4. Build and load application images

```bash
docker build -t sample-producer:v1 ./producer
docker build -t sample-consumer:v1 ./consumer
minikube image load sample-producer:v1
minikube image load sample-consumer:v1
```

### 5. Deploy with Helm

```bash
helm install event-pipeline ./helm/event-pipeline -n kafka
```

### 6. Verify

```bash
kubectl logs -f deployment/event-pipeline-consumer -n kafka
```

You should see the consumer printing events published by the producer.

## What I'd Add Next

- Prometheus + Grafana monitoring via Strimzi Kafka Exporter
- mTLS between producer/consumer and Kafka brokers
- Schema Registry with Avro serialization
- CI/CD pipeline with GitHub Actions
- Dead letter queue for failed messages

## Cleanup

```bash
helm uninstall event-pipeline -n kafka
kubectl delete -f k8s/topics.yaml -n kafka
kubectl delete -f k8s/kafka-cluster.yaml -n kafka
minikube stop
```
