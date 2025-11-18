# Real-Time E-Commerce Analytics Platform

> **Portfolio Note**: This is a portfolio recreation demonstrating the architecture and implementation approach of a production system I built at Omfys Technologies. Code has been anonymized and uses synthetic data to protect client confidentiality.

## 🎯 Overview

Unified analytics platform processing **1 Lakh+ daily transactions** from 100 online stores with **99.95% data accuracy**, featuring CDC pipelines, Lambda architecture, and Kubernetes deployment.

## 📊 Key Metrics

- **Volume**: 1L+ transactions/day
- **Accuracy**: 99.95%
- **Throughput**: 50K requests/min
- **Latency**: <200ms p95
- **Cost Reduction**: 38%
- **Uptime**: 99.95%

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌───────┐     ┌──────────────┐
│  MySQL DBs  │────▶│ Debezium │────▶│ Kafka │────▶│ Spark Stream │
│ (100 stores)│     │   CDC    │     │       │     │              │
└─────────────┘     └──────────┘     └───┬───┘     └──────┬───────┘
                                         │                 │
                                         ▼                 ▼
                                    ┌─────────┐      ┌──────────┐
                                    │S3 Parquet│      │Real-time │
                                    │         │      │Dashboard │
                                    └────┬────┘      └──────────┘
                                         │
                                         ▼
                                    ┌─────────┐
                                    │EMR Batch│
                                    │         │
                                    └────┬────┘
                                         │
                                         ▼
                                    ┌──────────┐
                                    │ Redshift │
                                    │ Warehouse│
                                    └──────────┘
```

## 🛠️ Tech Stack

- **Streaming**: Apache Kafka, Spark Structured Streaming
- **Storage**: Amazon S3 (Parquet), Redshift, PostgreSQL
- **Orchestration**: Apache Airflow (100+ DAGs)
- **Compute**: AWS EMR, EKS
- **Containers**: Docker, Kubernetes (20+ services)
- **CDC**: Debezium
- **Load Balancing**: Nginx
- **Monitoring**: Prometheus, Grafana

## ⚡ Key Features

### 1. Change Data Capture (CDC) Pipeline
- Debezium connectors capturing MySQL binlog changes
- Real-time streaming to Kafka (12 topics, 3-broker cluster)
- Schema Registry for Avro serialization and schema evolution
- Throughput: 10K events/second with exactly-once semantics

### 2. Lambda Architecture
- **Stream Layer**: Real-time processing with Kafka Streams for immediate insights
- **Batch Layer**: Historical analysis with Spark on EMR for deep analytics
- **Serving Layer**: Combined views in Redshift for unified querying

### 3. Kubernetes Deployment
- Horizontal Pod Autoscaling (HPA) based on CPU/memory and Kafka lag
- Cluster autoscaling handling 3x traffic spikes
- 20+ microservices managed via Helm charts
- Blue-green deployment strategy for zero-downtime releases

### 4. Performance Optimization
- Nginx load balancing across 12 FastAPI microservices
- Parquet with Snappy compression reducing storage by 60%
- Predicate pushdown and partition pruning
- Query optimization reducing compute costs by 38%

## 📁 Project Structure

```
ecommerce-realtime-analytics/
├── src/
│   ├── kafka_consumer.py         # Transaction stream consumer
│   ├── spark_streaming.py        # Real-time KPI computation
│   ├── batch_processing.py       # EMR batch analytics
│   └── api/
│       ├── main.py               # FastAPI application
│       └── routes/               # API endpoints
├── airflow/
│   └── dags/
│       ├── etl_pipeline.py       # ETL orchestration
│       └── data_quality.py       # Quality checks
├── kubernetes/
│   ├── deployment.yaml           # Service deployments
│   ├── hpa.yaml                  # Auto-scaling config
│   ├── service.yaml              # Load balancer
│   └── configmap.yaml            # Configuration
├── config/
│   ├── kafka_config.yaml         # Kafka settings
│   └── spark_config.yaml         # Spark optimization
├── docs/
│   ├── architecture.md           # Detailed architecture
│   └── deployment.md             # Deployment guide
├── docker-compose.yml
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- Kubernetes cluster (for production)
- Apache Kafka 3.0+
- Apache Spark 3.4+
- AWS Account (for S3, EMR, Redshift)

### Local Development Setup

```bash
# Clone repository
git clone https://github.com/Amanroy666/ecommerce-realtime-analytics.git
cd ecommerce-realtime-analytics

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start local services
docker-compose up -d

# Verify services
docker-compose ps
```

### Configuration

Create `.env` file:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
POSTGRES_HOST=localhost
POSTGRES_USER=analytics
POSTGRES_PASSWORD=your_password
POSTGRES_DB=ecommerce
REDIS_HOST=localhost
REDIS_PORT=6379
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
```

Update `config/kafka_config.yaml`:
```yaml
bootstrap_servers:
  - localhost:9092
topics:
  transactions: ecommerce-transactions
  customers: ecommerce-customers
  products: ecommerce-products
schema_registry: http://localhost:8081
consumer_group: analytics-consumer-group
```

## 💡 Key Implementation Highlights

### Real-Time Stream Processing

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark with optimizations
spark = SparkSession.builder \
    .appName("E-Commerce Real-Time Analytics") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

# Read from Kafka
transaction_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
parsed_stream = transaction_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Calculate real-time metrics with windowing
metrics = parsed_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("store_id")
    ).agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers")
    )

# Write to multiple sinks
query = metrics.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

### CDC Integration with Debezium

```json
{
  "name": "mysql-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql-server",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${file:/secrets/mysql-password.txt:password}",
    "database.server.id": "184054",
    "database.server.name": "ecommerce",
    "table.include.list": "ecommerce.transactions,ecommerce.customers,ecommerce.products",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.ecommerce",
    "tasks.max": "3",
    "snapshot.mode": "initial",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}
```

### Kubernetes Auto-Scaling

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: analytics-api-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: analytics-api
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30
      - type: Pods
        value: 4
        periodSeconds: 30
      selectPolicy: Max
```

## 📈 Performance Results

| Metric | Before Optimization | After Optimization | Improvement |
|--------|---------------------|-------------------|-------------|
| Query Latency (p95) | 2.5s | 180ms | 92% ↓ |
| Infrastructure Cost | $12,000/month | $7,400/month | 38% ↓ |
| Data Accuracy | 94% | 99.95% | 6.3% ↑ |
| System Uptime | 98.5% | 99.95% | 1.5% ↑ |
| Throughput | 20K req/min | 50K req/min | 150% ↑ |

### Cost Breakdown

| Component | Monthly Cost | Optimization |
|-----------|--------------|--------------|
| EMR Cluster | $3,200 | Spot instances, auto-scaling |
| Redshift | $2,100 | Reserved instances, compression |
| EKS | $1,500 | Node auto-scaling, rightsizing |
| S3 Storage | $400 | Lifecycle policies, Glacier |
| Data Transfer | $200 | VPC endpoints, CloudFront |
| **Total** | **$7,400** | **38% reduction** |

## 🔒 Security

- **Network**: VPC with private subnets, NAT gateways, security groups
- **Access Control**: IAM roles with least privilege, MFA enabled
- **Encryption**: 
  - At rest: AES-256 encryption for S3, RDS, Redshift
  - In transit: TLS 1.3 for all communications
- **Secrets**: AWS Secrets Manager with automatic rotation
- **Audit**: CloudTrail logging, CloudWatch alarms
- **Compliance**: SOC 2, GDPR compliant architecture

## 📊 Monitoring & Observability

### Metrics Tracked
- System metrics: CPU, memory, disk I/O
- Application metrics: Request rate, error rate, latency (p50, p95, p99)
- Business metrics: Revenue, transactions, customer counts
- Kafka metrics: Consumer lag, broker performance
- Spark metrics: Job duration, stage failures, executor memory

### Alerting Rules
- Consumer lag > 1000 messages
- API latency p95 > 500ms
- Error rate > 1%
- Disk usage > 85%
- Data pipeline failures

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/

# Run load tests
locust -f tests/load/locustfile.py --host=http://localhost:8000
```

## 📚 Documentation

- [Architecture Details](docs/architecture.md)
- [API Documentation](docs/api.md)
- [Deployment Guide](docs/deployment.md)
- [Monitoring Setup](docs/monitoring.md)
- [Troubleshooting](docs/troubleshooting.md)

## 🤝 Contributing

This is a portfolio project demonstrating production patterns. For questions or discussions about the implementation, feel free to reach out.

## 👤 Author

**Aman Roy**
- Data Engineer at Omfys Technologies
- 2.5+ years of experience in building enterprise data platforms
- Email: contactaman000@gmail.com
- LinkedIn: [linkedin.com/in/amanxroy](https://linkedin.com/in/amanxroy)
- GitHub: [@Amanroy666](https://github.com/Amanroy666)

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details

---

**Note**: This project showcases the architecture and implementation patterns used in a production system. Actual production code and data remain confidential under client agreements.
