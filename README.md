┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    MicroK8s Cluster (Ubuntu VM)                              │
│                                                                                              │
│  ┌───────────────────┐     ┌──────────────────┐     ┌──────────────────────────────────┐    │
│  │   Massive.com      │     │   Apache Kafka    │     │   Apache Spark                   │    │
│  │   WebSocket API    │     │   (Strimzi)       │     │   Structured Streaming           │    │
│  │                    │     │                    │     │                                  │    │
│  │  wss://delayed     │────▶│  Topic: second    │────▶│  SecondStreamJob                │    │
│  │  .massive.com      │     │  Topic: minute    │     │  MinuteStreamJob                │    │
│  │  /stocks           │     │                    │     │                                  │    │
│  └───────┬────────────┘     └──────────────────┘     └──────────┬───────────────────────┘    │
│          │                                                       │                            │
│          │  WebSocket Producer                                   │  foreachBatch               │
│          │  (ws_client.py)                                       │  (JDBC Write)               │
│          │                                                       ▼                            │
│          │                                              ┌──────────────────────┐             │
│          │                                              │  PostgreSQL +        │             │
│          │                                              │  TimescaleDB         │             │
│          │                                              │                      │             │
│          │                                              │  stock_agg_second    │             │
│          │                                              │  stock_agg_minute    │             │
│          │                                              │  dead_letter_queue   │             │
│          │                                              └──────────┬───────────┘             │
│          │                                                         │                         │
│          │                                                         │  SQL Queries             │
│          │                                                         ▼                         │
│          │                                              ┌──────────────────────┐             │
│          └─────────────────────────────────────────────▶│  Panel/Bokeh GUI     │             │
│                        (Callbacks steuern Producer)      │  (Web Dashboard)     │             │
│                                                          │                      │             │
│                                                          │  Candlestick Chart   │             │
│                                                          │  Line Chart          │             │
│                                                          │  Ticker Table        │             │
│                                                          │  Stream Controls     │             │
│                                                          └──────────────────────┘             │
│                                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐    │
│  │  Monitoring: Prometheus + Grafana                                                     │    │
│  │  6 Dashboards │ 16 Alert Rules │ 3 Datasources                                      │    │
│  └──────────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐    │
│  │  Automatisierung: Ansible (10 Playbooks) + Helm (1 Chart) + Docker (3 Images)        │    │
│  └──────────────────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────────────────┘



Phase 1: Daten-Ingestion
─────────────────────────
                          ┌─────────────────┐
 Massive.com WS API ────▶ │  ws_client.py    │
                          │                  │
                          │  1. Connect      │
                          │  2. Auth         │
                          │  3. Subscribe    │
                          │  4. Receive      │
                          └────────┬─────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │ message_parser   │
                          │                  │
                          │  - JSON Parse    │
                          │  - Validate      │
                          │  - AggregateData │
                          └────────┬─────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │ kafka_producer   │
                          │                  │
                          │  - Serialize     │
                          │  - Partition     │    Key = Symbol
                          │  - Produce       │──────────────────▶ Kafka
                          │  - Flush         │
                          └─────────────────┘


Phase 2: Stream Processing
──────────────────────────
                          ┌─────────────────┐
 Kafka Topic ────────────▶│ Spark readStream │
                          │ (Kafka Source)   │
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ parse_kafka_msgs  │  from_json()
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ transform_schema  │  Spalten-Mapping
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ validate_data     │  Preis/Volume/Timestamp
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ deduplicate       │  Watermark + dropDuplicates
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ add_derived_cols  │  ML Features
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ foreachBatch      │
                          │  ├── QualityCheck │──▶ Dead Letter Queue
                          │  └── JDBC Write   │──▶ TimescaleDB
                          └─────────────────┘


Phase 3: Visualisierung
───────────────────────
                          ┌─────────────────┐
 TimescaleDB ────────────▶│ data_provider    │
                          │  SQL Queries     │
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ chart_callbacks   │  Periodic 2s Update
                          │                  │
                          │  ├── Candlestick │
                          │  └── Line Chart  │
                          └────────┬─────────┘
                                   │
                          ┌────────▼─────────┐
                          │ Panel/Bokeh GUI  │  Browser :30006
                          └─────────────────┘
