# ============================================================
# VORAUSSETZUNGEN
# ============================================================
# - Ubuntu VM (22.04 LTS empfohlen)
# - Mindestens 8GB RAM, 4 CPU Cores, 50GB Disk
# - Internetzugang
# - sudo-Rechte
# ============================================================

# System aktualisieren
sudo apt update && sudo apt upgrade -y

# Git installieren (falls nicht vorhanden)
sudo apt install -y git

# Projekt klonen
git clone https://github.com/your-org/stock-streaming-platform.git
cd stock-streaming-platform


# ============================================================
# PHASE 1: Python Environment Setup
# ============================================================

# 1a. Miniconda installieren (falls nicht vorhanden)
if ! command -v conda &> /dev/null; then
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda3
    eval "$($HOME/miniconda3/bin/conda shell.bash hook)"
    conda init bash
    source ~/.bashrc
fi

# 1b. Setup-Skript ausführen
chmod +x setup.sh
./setup.sh

# 1c. Conda Environment aktivieren
conda activate stock-streaming

# 1d. Verifizieren
python --version          # 3.11.x
python -c "import panel; print(panel.__version__)"
python -c "import pyspark; print(pyspark.__version__)"
python -c "import confluent_kafka; print(confluent_kafka.__version__)"

# 1e. API Key eintragen
nano .env
# → MASSIVE_API_KEY=your_actual_api_key_here


# ============================================================
# PHASE 2: Kubernetes Cluster Setup via Ansible
# ============================================================

cd infrastructure/ansible

# 2a. Ansible Inventory prüfen
ansible-inventory --list --yaml

# 2b. OPTION A: Vollständiges Setup (empfohlen)
ansible-playbook playbooks/00_full_setup.yaml

# 2c. OPTION B: Schrittweise
ansible-playbook playbooks/01_prerequisites.yaml    # ~3 Min
ansible-playbook playbooks/02_microk8s.yaml          # ~5 Min
ansible-playbook playbooks/03_docker.yaml            # ~3 Min
ansible-playbook playbooks/04_cluster_init.yaml      # ~2 Min
ansible-playbook playbooks/05_build_images.yaml      # ~10 Min
ansible-playbook playbooks/06_deploy_infra.yaml      # ~8 Min
ansible-playbook playbooks/07_deploy_app.yaml        # ~3 Min
ansible-playbook playbooks/08_deploy_monitoring.yaml # ~3 Min
ansible-playbook playbooks/09_verify.yaml            # ~1 Min

# Gesamtdauer: ca. 35-40 Minuten

# 2d. Cluster Status prüfen
microk8s kubectl get pods -A
microk8s kubectl get svc -A

# Erwartete Ausgabe:
╔══════════════════════════════════════════════════════════════╗
║           🚀 STOCK STREAMING PLATFORM - DEPLOYED           ║
╠══════════════════════════════════════════════════════════════╣
║                                                            ║
║  🖥️  Panel GUI:     http://localhost:30006/app             ║
║  ⚡ Spark UI:      http://localhost:30080                  ║
║  📊 Grafana:       http://localhost:30003                  ║
║  🔗 Kafka (ext):   localhost:30092                         ║
║                                                            ║
╚══════════════════════════════════════════════════════════════╝


# ============================================================
# PHASE 3 (ALTERNATIV): Lokales Development mit Docker Compose
# ============================================================

cd infrastructure/docker

# 3a. Infrastruktur starten (Kafka, TimescaleDB, Spark)
docker compose up -d

# 3b. Warten bis alles bereit ist
docker compose ps
# Alle Services sollten "running (healthy)" zeigen

# 3c. TimescaleDB initialisieren
cd ../..
conda activate stock-streaming
python -m backend.database.hypertable_setup

# 3d. WebSocket Producer starten (Terminal 1)
python -m backend.websocket_producer.entrypoint \
    --stream second --top10 --log-level INFO

# 3e. Spark Streaming Job starten (Terminal 2)
python -m backend.spark_streaming.entrypoint \
    --job second --log-level INFO

# 3f. Panel GUI starten (Terminal 3)
panel serve frontend/app.py \
    --show --autoreload --port 5006 \
    --allow-websocket-origin="*"

# 3g. Browser öffnen
# → http://localhost:5006/app


# ============================================================
# PHASE 4: Tests
# ============================================================

cd /path/to/stock-streaming-platform
conda activate stock-streaming

# 4a. Unit Tests (schnell, keine externen Services nötig)
chmod +x scripts/run_tests.sh
./scripts/run_tests.sh unit

# 4b. Spark Tests (benötigen Java)
./scripts/run_tests.sh spark

# 4c. Integration Tests (Docker Compose muss laufen)
./scripts/run_tests.sh integration

# 4d. Alle Tests
./scripts/run_tests.sh all

# 4e. Schnelle CI/CD Tests
./scripts/run_tests.sh fast

# Erwartetes Ergebnis:
# ============================================
#   🧪 ~115 Tests
#   ✅ Coverage: >70%
# ============================================


# Browser öffnen: http://localhost:30006/app (K8s) oder http://localhost:5006/app (lokal)


# GUI Anleitung:
┌─────────────────────────────────────────────────────────────────────────────┐
│  SCHRITT 1: Ticker auswählen                                                │
│  ─────────────────────────────                                              │
│  In der Sidebar unter "🎯 Ticker Auswahl":                                 │
│                                                                             │
│  [Alle S&P 500] [Top 10] [Manuell]                                        │
│                                                                             │
│  • "Alle S&P 500" → Alle 500 Ticker werden gestreamt                      │
│  • "Top 10" → Top 10 nach Marktkapitalisierung (Default)                  │
│  • "Manuell" → Eigene Ticker eingeben: AAPL, MSFT, GOOGL                  │
│    → Autocomplete-Suche verfügbar                                          │
│    → "✅ Auswahl anwenden" klicken                                         │
│                                                                             │
│  Die ausgewählten Ticker erscheinen in der Tabelle unten.                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  SCHRITT 2: Stream-Typ wählen                                              │
│  ────────────────────────────                                               │
│  Unter "🔄 Stream Controls":                                               │
│                                                                             │
│  [Sekunden] [Minuten]                                                       │
│                                                                             │
│  • "Sekunden" → Aggregates per Second (A-Events)                           │
│  • "Minuten" → Aggregates per Minute (AM-Events)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  SCHRITT 3: Stream starten                                                  │
│  ────────────────────────                                                   │
│  [▶ Start Stream]  [⏹ Stop Stream]                                         │
│                                                                             │
│  Nach dem Start:                                                            │
│  • 🟢 Status-Indikator wird grün                                          │
│  • WebSocket Producer verbindet sich mit Massive.com                       │
│  • Spark Streaming Job startet                                             │
│  • Daten fließen: WS → Kafka → Spark → TimescaleDB                       │
│                                                                             │
│  WICHTIG: Beim Wechsel von Sekunden↔Minuten wird der                       │
│  aktuelle Stream automatisch gestoppt.                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│  SCHRITT 4: Chart-Typ wählen                                               │
│  ───────────────────────────                                                │
│  Unter "📊 Chart-Typ":                                                     │
│                                                                             │
│  [Candlestick] [Linie]                                                      │
│                                                                             │
│  Wechsel erfolgt sofort ohne Stream-Unterbrechung.                          │
├─────────────────────────────────────────────────────────────────────────────┤
│  SCHRITT 5: Ticker zum Plotten wählen                                      │
│  ────────────────────────────────────                                       │
│  Unter "🔽 Plot-Ticker":                                                   │
│                                                                             │
│  [AAPL ▼]                                                                   │
│                                                                             │
│  • Dropdown enthält alle ausgewählten Ticker                               │
│  • Es wird immer nur EIN Ticker geplottet                                  │
│  • ALLE ausgewählten Ticker werden gestreamt + in DB geschrieben           │
│  • Chart aktualisiert sich automatisch alle 2 Sekunden                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  SCHRITT 6: Stream stoppen                                                  │
│  ────────────────────────                                                   │
│  [⏹ Stop Stream]                                                           │
│                                                                             │
│  • WebSocket-Verbindung wird sauber geschlossen                            │
│  • Kafka Producer flusht ausstehende Nachrichten                           │
│  • Spark Streaming Job wird beendet                                        │
│  • Chart-Update stoppt                                                     │
│  • 🔴 Status-Indikator wird rot                                           │
└─────────────────────────────────────────────────────────────────────────────┘


# Grafana öffnen
open http://localhost:30003

# Login: admin / grafana2024!

# Grafana Metriken:
┌──────────────────────────────────────────────────────────────┐
│  🟢 GESUNDER ZUSTAND:                                       │
│  ─────────────────                                           │
│  • Alle 6 Service-Indikatoren: UP (grün)                    │
│  • WS Messages/s: > 0 (Daten fließen)                      │
│  • Kafka Consumer Lag: < 1000                               │
│  • Spark Batch Time p99: < 5s                               │
│  • DB Cache Hit Ratio: > 95%                                │
│  • Rejection Rate: < 5%                                     │
│  • Errors: 0                                                │
├──────────────────────────────────────────────────────────────┤
│  🟡 WARNUNG:                                                 │
│  ──────────                                                  │
│  • Consumer Lag > 1000 → Spark verarbeitet zu langsam       │
│  • Batch Time p99 > 10s → DB-Write zu langsam              │
│  • Cache Hit < 90% → shared_buffers erhöhen                │
│  • Rejection Rate > 10% → Datenqualität prüfen             │
│  • Reconnections > 0.5/s → API/Netzwerk-Problem            │
├──────────────────────────────────────────────────────────────┤
│  🔴 KRITISCH:                                                │
│  ───────────                                                 │
│  • Service DOWN → Pod-Logs prüfen                           │
│  • DB Write Errors > 0 → TimescaleDB prüfen                │
│  • Kafka Broker DOWN → Strimzi Operator prüfen             │
│  • No Batches Processed → Spark Job neu starten            │
└──────────────────────────────────────────────────────────────┘


# Troubleshooting:

# ============================================================
# PROBLEM: Pods starten nicht
# ============================================================
microk8s kubectl get pods -n stock-platform
microk8s kubectl describe pod <pod-name> -n stock-platform
microk8s kubectl logs <pod-name> -n stock-platform

# Häufige Ursache: Image nicht gefunden
microk8s kubectl get events -n stock-platform --sort-by='.lastTimestamp'

# Lösung: Images neu bauen
ansible-playbook playbooks/05_build_images.yaml -e "force_rebuild=true"


# ============================================================
# PROBLEM: Kafka Topics existieren nicht
# ============================================================
microk8s kubectl exec -n kafka kafka-cluster-kafka-0 -- \
    bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Lösung: Topics manuell erstellen
microk8s kubectl exec -n kafka kafka-cluster-kafka-0 -- \
    bin/kafka-topics.sh --create --topic stocks.aggregates.second \
    --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092


# ============================================================
# PROBLEM: TimescaleDB Tabellen fehlen
# ============================================================
microk8s kubectl exec -n stock-platform \
    $(microk8s kubectl get pods -n stock-platform -l app=timescaledb \
    -o jsonpath='{.items[0].metadata.name}') -- \
    psql -U postgres -d stock_streaming -c "\dt"

# Lösung: Init-Scripts erneut ausführen
python -m backend.database.hypertable_setup


# ============================================================
# PROBLEM: WebSocket Producer verbindet nicht
# ============================================================
# 1. API Key prüfen
echo $MASSIVE_API_KEY

# 2. DNS-Auflösung testen
nslookup delayed.massive.com

# 3. WebSocket-Test
python -c "
import asyncio, websockets
async def test():
    async with websockets.connect('wss://delayed.massive.com/stocks') as ws:
        print('Connected!')
asyncio.run(test())
"

# 4. Logs prüfen
microk8s kubectl logs -f deployment/ws-producer -n stock-platform


# ============================================================
# PROBLEM: Spark Job crasht
# ============================================================
# Logs prüfen
microk8s kubectl logs -f deployment/spark-job-second -n stock-platform

# Häufige Ursache: Kafka nicht erreichbar
# → Kafka Service-DNS prüfen:
microk8s kubectl get svc -n kafka

# Häufige Ursache: Checkpoint-Verzeichnis
# → Checkpoint löschen und neu starten:
microk8s kubectl exec deployment/spark-job-second -n stock-platform -- \
    rm -rf /opt/spark-checkpoints/second/*


# ============================================================
# PROBLEM: GUI zeigt keine Daten
# ============================================================
# 1. Prüfe ob Daten in der DB sind
microk8s kubectl exec -n stock-platform \
    $(microk8s kubectl get pods -n stock-platform -l app=timescaledb \
    -o jsonpath='{.items[0].metadata.name}') -- \
    psql -U postgres -d stock_streaming -c \
    "SELECT count(*), max(time) FROM stock_agg_second;"

# 2. Prüfe ob Demo-Modus aktiv ist
# In frontend/callbacks/chart_callbacks.py:
#   self._use_demo_data = True  → Demo-Daten (kein DB-Zugriff)
#   self._use_demo_data = False → Echte DB-Daten

# 3. Panel-Logs prüfen
microk8s kubectl logs -f deployment/panel-gui -n stock-platform


# ============================================================
# PROBLEM: Hoher Consumer Lag in Kafka
# ============================================================
# Consumer Lag prüfen
microk8s kubectl exec -n kafka kafka-cluster-kafka-0 -- \
    bin/kafka-consumer-groups.sh --describe \
    --group spark-stock-streaming-second \
    --bootstrap-server localhost:9092

# Lösung: Spark Workers hochskalieren
microk8s kubectl scale deployment spark-worker \
    --replicas=3 -n stock-platform


# ============================================================
# KOMPLETT NEU STARTEN
# ============================================================
ansible-playbook playbooks/99_teardown.yaml
ansible-playbook playbooks/00_full_setup.yaml


# Cheet-Sheet

# ============================================================
# KUBERNETES
# ============================================================
alias k='microk8s kubectl'
alias h='microk8s helm3'

k get pods -A                              # Alle Pods
k get pods -n stock-platform               # App Pods
k get pods -n kafka                        # Kafka Pods
k get pods -n monitoring                   # Monitoring Pods
k get svc -A                               # Alle Services
k logs -f deploy/ws-producer -n stock-platform      # WS Producer Logs
k logs -f deploy/spark-job-second -n stock-platform # Spark Logs
k logs -f deploy/panel-gui -n stock-platform        # GUI Logs
k top pods -n stock-platform               # Resource Usage
k exec -it deploy/timescaledb -n stock-platform -- psql -U postgres -d stock_streaming

# ============================================================
# DOCKER COMPOSE (Lokale Entwicklung)
# ============================================================
cd infrastructure/docker
docker compose up -d                       # Starten
docker compose ps                          # Status
docker compose logs -f kafka               # Kafka Logs
docker compose down                        # Stoppen
docker compose down -v                     # Stoppen + Volumes löschen

# ============================================================
# PYTHON / PANEL
# ============================================================
conda activate stock-streaming
panel serve frontend/app.py --show --autoreload --port 5006
python -m backend.websocket_producer.entrypoint --stream second --top10
python -m backend.spark_streaming.entrypoint --job second
python -m backend.database.hypertable_setup

# ============================================================
# TESTS
# ============================================================
./scripts/run_tests.sh unit
./scripts/run_tests.sh spark
./scripts/run_tests.sh all
pytest tests/unit/test_message_parser.py -v    # Einzelne Datei
pytest tests/ -k "test_parse" -v               # Nach Name filtern

# ============================================================
# ANSIBLE
# ============================================================
cd infrastructure/ansible
ansible-playbook playbooks/00_full_setup.yaml          # Alles
ansible-playbook playbooks/09_verify.yaml              # Health Check
ansible-playbook playbooks/99_teardown.yaml            # Aufräumen
ansible-playbook playbooks/05_build_images.yaml \
    --tags "spark-job" -e "force_rebuild=true"         # Ein Image neu bauen

# ============================================================
# DATENBANK
# ============================================================
# Direkte Verbindung
psql -h localhost -p 5432 -U postgres -d stock_streaming

# Nützliche Queries
SELECT count(*), min(time), max(time) FROM stock_agg_second;
SELECT symbol, count(*) FROM stock_agg_second GROUP BY symbol ORDER BY count DESC LIMIT 10;
SELECT * FROM timescaledb_information.hypertables;
SELECT * FROM timescaledb_information.jobs;
SELECT * FROM dead_letter_queue ORDER BY rejected_at DESC LIMIT 10;
