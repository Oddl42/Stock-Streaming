Datenfluss:

  Kafka Topic                  Spark Structured Streaming              TimescaleDB
  ─────────                    ──────────────────────────              ───────────
                               ┌─────────────────────────┐
  stocks.aggregates.second ──▶ │  1. Read from Kafka      │
                               │  2. Parse JSON            │
                               │  3. Transform & Validate  │ ──▶  stock_agg_second
                               │  4. Quality Checks        │
                               │  5. Write foreachBatch     │
                               └─────────────────────────┘

                               ┌─────────────────────────┐
  stocks.aggregates.minute ──▶ │  1. Read from Kafka      │
                               │  2. Parse JSON            │
                               │  3. Transform & Validate  │ ──▶  stock_agg_minute
                               │  4. Quality Checks        │
                               │  5. Write foreachBatch     │
                               └─────────────────────────┘



backend/spark_streaming/
├── __init__.py                    # Package Exports
├── spark_session.py               # SparkSession Factory (~110 Zeilen)
│                                    → Optimierte Config für Streaming + Kafka + JDBC
│                                    → Adaptive Query Execution
│                                    → Kryo Serialization
│                                    → Prometheus Metrics Integration
│
├── schemas.py                     # Daten-Schemas (~100 Zeilen)
│                                    → Massive.com WebSocket Schema (Second + Minute)
│                                    → TimescaleDB Ziel-Schema
│                                    → Kafka Message Schema
│
├── transformations.py             # DataFrame Transformations (~200 Zeilen)
│                                    → JSON Parsing aus Kafka
│                                    → Schema-Mapping (Massive → TimescaleDB)
│                                    → Validierung (Preise, Volume, Timestamps)
│                                    → Deduplication mit Watermarks
│                                    → ML-Features (price_range, shadows, etc.)
│
├── quality_checks.py              # Datenqualität (~110 Zeilen)
│                                    → Plausibilitäts-Checks
│                                    → Good/Bad Data Split
│                                    → Statistik-Tracking
│
├── db_sink.py                     # TimescaleDB Writer (~210 Zeilen)
│                                    → JDBC Append
│                                    → Upsert via Staging Table
│                                    → Retry-Logik mit Backoff
│                                    → Dead Letter Queue
│
├── metrics.py                     # Prometheus Metrics (~170 Zeilen)
│                                    → Counters, Gauges, Histograms
│                                    → Custom Metrics Server
│                                    → Batch-Level Tracking
│
├── second_stream_job.py           # Sekunden-Job (~210 Zeilen)
│                                    → Komplette Pipeline: Kafka → Transform → DB
│                                    → foreachBatch mit Quality Checks
│                                    → Graceful Shutdown
│                                    → Status-Monitoring
│
├── minute_stream_job.py           # Minuten-Job (~190 Zeilen)
│                                    → Analoge Pipeline für Minuten-Daten
│                                    → Andere Watermarks & Trigger-Zeiten
│
├── stream_job_manager.py          # Job-Manager (~250 Zeilen)
│                                    → Thread-basiertes Management
│                                    → Start/Stop für beide Jobs
│                                    → Health Checks
│                                    → Status-Reporting
│
└── entrypoint.py                  # CLI Entrypoint (~100 Zeilen)
                                     → spark-submit kompatibel
                                     → Unterstützt: second, minute, both
                                     → Kubernetes-ready


# ================================================
# Option 1: Direkt mit Python (Entwicklung)
# ================================================

# Nur Sekunden-Job
python -m backend.spark_streaming.entrypoint --job second

# Nur Minuten-Job
python -m backend.spark_streaming.entrypoint --job minute

# Beide Jobs gleichzeitig
python -m backend.spark_streaming.entrypoint --job both --log-level DEBUG


# ================================================
# Option 2: Via spark-submit (Produktion)
# ================================================

spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    --master local[*] \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    backend/spark_streaming/entrypoint.py --job second


# ================================================
# Option 3: Kubernetes via spark-submit
# ================================================

spark-submit \
    --master k8s://https://$(microk8s kubectl cluster-info | grep -oP 'https://\S+') \
    --deploy-mode cluster \
    --name stock-streaming-second \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    --conf spark.kubernetes.container.image=stock-platform/spark-job:latest \
    --conf spark.kubernetes.namespace=stock-platform \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.executor.instances=2 \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=1g \
    backend/spark_streaming/entrypoint.py --job second
