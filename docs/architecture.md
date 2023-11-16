# Architecture Overview

## System Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│  Data Sources   │    │    Kafka     │    │ Spark Streaming │
│                 │    │   (KRaft)    │    │                 │
│ • Replay CSV    │───▶│ events_raw   │───▶│ • Parse/Validate│
│ • Live Stream   │    │ deadletter   │    │ • Deduplicate   │
│ • Wikimedia     │    │              │    │ • Sessionize    │
└─────────────────┘    └──────────────┘    │ • Aggregate     │
                                           └─────────────────┘
                                                     │
                                ┌────────────────────┼────────────────────┐
                                ▼                    ▼                    ▼
                        ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
                        │   MongoDB    │   │   Parquet    │   │  Dashboard   │
                        │  (Hot Store) │   │ (Cold Store) │   │  (Streamlit) │
                        │              │   │              │   │              │
                        │ • KPIs       │   │ • Archival   │   │ • Live KPIs  │
                        │ • Upserts    │   │ • Analytics  │   │ • Funnel     │
                        │ • Real-time  │   │ • Compliance │   │ • Time Series│
                        └──────────────┘   └──────────────┘   └──────────────┘
                                │                                     ▲
                                ▼                                     │
                        ┌──────────────┐                             │
                        │   FastAPI    │─────────────────────────────┘
                        │ (REST API)   │
                        │              │
                        │ • /kpis      │
                        │ • /funnel    │
                        │ • /health    │
                        └──────────────┘
```

## Data Flow

### 1. Event Ingestion
- **Replay Producer**: Reads `data/events.csv.gz` and streams at configurable rate (500-2000 eps)
- **Live Producer**: Converts Wikimedia RecentChange events to e-commerce schema
- **Kafka Topics**: 
  - `events_raw` (3 partitions) - Main event stream
  - `events_deadletter` (1 partition) - Malformed events

### 2. Stream Processing (Spark Structured Streaming)
```
Raw Events → Parse/Validate → Watermark → Deduplicate → Sessionize → Window → Aggregate
```

#### Key Components:
- **Event Time Processing**: 10-minute watermark for late arrivals
- **Deduplication**: Stateful dedup by `event_id` within watermark window
- **Sessionization**: 30-minute inactivity timeout per user
- **Windowing**: 1-minute tumbling windows for real-time KPIs

#### Aggregations:
- Sessions (distinct `session_id`)
- Event counts by type (page_view, add_to_cart, purchase)
- Conversion funnel rates
- Revenue metrics (total, AOV)
- Latency percentiles (P95 end-to-end)

### 3. Storage Strategy

#### Hot Path (MongoDB)
- **Purpose**: Real-time dashboard queries
- **Schema**: Minute-level KPI documents
- **Operations**: Upsert by `minute` timestamp (idempotent)
- **Retention**: Last 7 days (configurable)

#### Cold Path (Parquet)
- **Purpose**: Historical analytics, compliance
- **Partitioning**: `year/month/day/hour`
- **Format**: Columnar for efficient OLAP queries
- **Retention**: Indefinite (cost-effective storage)

### 4. Serving Layer

#### Streamlit Dashboard
- **Real-time KPIs**: Auto-refresh every 5 seconds
- **Visualizations**: Funnel charts, time series, anomaly alerts
- **Features**: 
  - Live conversion rate monitoring
  - Revenue tracking
  - System health indicators
  - Z-score anomaly detection

#### FastAPI Endpoints
- **Purpose**: Programmatic access, external integrations
- **Endpoints**:
  - `GET /kpis/latest` - Most recent metrics
  - `GET /kpis?minutes_back=30` - Historical data
  - `GET /funnel?minutes_back=5` - Conversion rates
  - `GET /health` - System status

## Performance Characteristics

### Throughput
- **Sustained**: 2,000+ events/second
- **Peak**: 5,000+ events/second (short bursts)
- **Bottlenecks**: MongoDB write throughput, dashboard queries

### Latency
- **End-to-end P95**: < 850ms (event ingestion → dashboard)
- **Processing**: < 200ms (Kafka → aggregation)
- **Dashboard refresh**: 5-second intervals

### Scalability
- **Horizontal**: Increase Kafka partitions, Spark parallelism
- **Vertical**: Larger Spark executor memory for stateful operations
- **Storage**: MongoDB sharding, Parquet partitioning

## Fault Tolerance

### Exactly-Once Processing
- **Kafka**: Idempotent producers, transactional consumers
- **Spark**: Checkpointing with write-ahead logs
- **MongoDB**: Upsert operations (idempotent by design)

### Recovery Scenarios
1. **Spark Job Failure**: Resume from last checkpoint
2. **Kafka Broker Down**: Consumer group rebalancing
3. **MongoDB Unavailable**: Backpressure, retry with exponential backoff
4. **Data Corruption**: Dead letter queue for malformed events

## Monitoring & Observability

### Key Metrics
- **Throughput**: Events/second ingested and processed
- **Latency**: P95 end-to-end timing
- **Error Rate**: Malformed events, processing failures
- **Resource Usage**: Spark executor memory, Kafka lag

### Alerting
- **High Latency**: P95 > 2 seconds
- **Processing Lag**: Kafka consumer lag > 1 minute
- **Conversion Anomalies**: Z-score > 3 standard deviations
- **System Health**: Service availability, database connectivity

## Cloud Migration Path

### Managed Services Mapping
- **Kafka** → Google Pub/Sub, AWS Kinesis, Azure Event Hubs
- **Spark** → Dataproc, EMR, Azure Synapse Analytics
- **MongoDB** → Atlas, DocumentDB, CosmosDB
- **Dashboard** → Cloud Run, ECS, App Service

### Considerations
- **Network**: VPC peering, private endpoints
- **Security**: IAM roles, encryption at rest/transit
- **Cost**: Reserved instances, auto-scaling policies
- **Compliance**: Data residency, audit logging