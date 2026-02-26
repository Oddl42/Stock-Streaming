Datenfluss:

  Massive.com                WebSocket Producer               Kafka
  ───────────                ──────────────────               ─────

  wss://delayed              ┌────────────────────┐
  .massive.com ─────────────▶│  1. ws_client       │
  /stocks                    │     (asyncio)       │
                             │                     │
                             │  2. message_parser   │
                             │     (validate/parse) │
                             │                     │
                             │  3. kafka_producer   │──────▶ stocks.aggregates.second
                             │     (produce)        │──────▶ stocks.aggregates.minute
                             └────────────────────┘
                                      │
                             ┌────────┴────────┐
                             │ reconnect_handler│  (bei Verbindungsabbruch)
                             │ rate_limiter     │  (API Rate Limits)
                             │ health_check     │  (K8s Probes)
                             │ metrics          │  (Prometheus)
                             └─────────────────┘


backend/websocket_producer/
├── __init__.py              Package Exports
├── message_parser.py        JSON Parsing & Validierung
│                                 → AggregateData Dataclass
│                                 → Preis-/Volume-Validierung
│                                 → Status-Messages
│
├── reconnect_handler.py     Reconnection Logik
│                                 → Exponential Backoff + Jitter
│                                 → Connection State Machine
│                                 → Auto-Reset nach stabiler Verbindung
│
├── rate_limiter.py          Rate Limiting
│                                 → Token Bucket
│                                 → Sliding Window
│                                 → Connection/Subscription/Message Limits
│
├── metrics.py               Prometheus Metriken
│                                 → Counters, Gauges, Histograms
│                                 → Message-Durchsatz
│                                 → Kafka-Produce Latenz
│
├── kafka_producer.py        Kafka Producer Wrapper
│                                 → Async Produce mit Callbacks
│                                 → Batching & Compression (LZ4)
│                                 → Idempotenz (exactly-once)
│                                 → Queue Management & Retry
│
├── ticker_manager.py        Ticker-Verwaltung
│                                 → CSV Laden & Validierung
│                                 → Subscription Batches (100er Gruppen)
│                                 → Subscribe/Unsubscribe Parameter
│
├── health_check.py          HTTP Health Server
│                                 → /health (Liveness)
│                                 → /ready (Readiness)
│                                 → /status (Detail-JSON)
│
├── ws_client.py             WebSocket Client (Kern)
│                                 → Connect → Auth → Subscribe → Listen
│                                 → Automatische Reconnection
│                                 → Message Processing Pipeline
│                                 → Graceful Shutdown
│
├── stream_manager.py        Stream Orchestrierung
│                                 → Gemeinsamer Kafka Producer
│                                 → Start/Stop Second/Minute
│                                 → Health & Metrics Integration
│
└── entrypoint.py            CLI Entrypoint
                                  → --stream second/minute/both
                                  → --tickers / --top10 / --all
                                  → Signal Handling
                                  
                                  
                                  
# ============================================================
# Lokale Entwicklung
# ============================================================

# Top 10 Ticker, Sekunden-Stream
python -m backend.websocket_producer.entrypoint \
    --stream second --top10 --log-level DEBUG

# Spezifische Ticker, Minuten-Stream
python -m backend.websocket_producer.entrypoint \
    --stream minute --tickers AAPL,MSFT,GOOGL,NVDA

# Alle S&P 500, beide Streams
python -m backend.websocket_producer.entrypoint \
    --stream both --all

# ============================================================
# Docker
# ============================================================

docker build -f infrastructure/docker/Dockerfile.ws-producer -t ws-producer .

docker run -e MASSIVE_API_KEY=your_key \
           -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
           -p 8092:8092 \
           ws-producer --stream second --top10

# ============================================================
# Health Checks testen
# ============================================================

curl http://localhost:8092/health
curl http://localhost:8092/ready
curl http://localhost:8092/status | jq .
