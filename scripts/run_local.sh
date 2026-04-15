#!/bin/bash
set -e

# ============================================================
# Lokaler Start aller Services - Stock Streaming Platform
# ============================================================
# Startet alle Komponenten lokal fuer Entwicklung und Testing
# OHNE Kubernetes.
#
# Voraussetzungen:
#   - Conda Environment "stock-streaming" aktiviert
#   - Docker Compose Services laufen (Kafka, TimescaleDB, Spark)
#   - .env Datei mit API Key konfiguriert
#
# Verwendung:
#   ./scripts/run_local.sh              # Alles starten (Top 10)
#   ./scripts/run_local.sh all          # Alles starten (Top 10)
#   ./scripts/run_local.sh all --all    # Alles starten (ALLE S&P 500)
#   ./scripts/run_local.sh gui          # Nur GUI
#   ./scripts/run_local.sh producer     # Nur WS Producer (Top 10)
#   ./scripts/run_local.sh producer --all  # Nur WS Producer (alle)
#   ./scripts/run_local.sh spark        # Nur Spark Jobs
#   ./scripts/run_local.sh infra        # Nur Docker Compose
#   ./scripts/run_local.sh init         # DB initialisieren + Tickers seeden
#   ./scripts/run_local.sh stop         # Alles stoppen
#   ./scripts/run_local.sh status       # Status anzeigen
#
# Umgebungsvariablen:
#   TICKER_MODE=top10|all|custom        # Default: top10
#   TICKER_SYMBOLS="AAPL,MSFT,GOOGL"   # Nur bei TICKER_MODE=custom
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# PYTHONPATH global setzen
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"

if [[ -f "${CONDA_PREFIX}/bin/java" ]]; then
    export JAVA_HOME="${CONDA_PREFIX}"
fi

# Farben
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# PID-Datei fuer Background-Prozesse
PID_DIR="/tmp/stock-platform-pids"
mkdir -p "$PID_DIR"

# ============================================================
# Ticker-Modus bestimmen
# ============================================================
# Prioritaet: CLI-Argument > Umgebungsvariable > Default (top10)

TICKER_MODE="${TICKER_MODE:-top10}"

# CLI-Argumente parsen (--all, --top10, --symbols)
parse_ticker_args() {
    for arg in "$@"; do
        case "$arg" in
            --all)
                TICKER_MODE="all"
                ;;
            --top10)
                TICKER_MODE="top10"
                ;;
            --symbols=*)
                TICKER_MODE="custom"
                TICKER_SYMBOLS="${arg#*=}"
                ;;
        esac
    done
}

# Alle Argumente nach dem Command parsen
parse_ticker_args "${@:2}"

get_producer_ticker_args() {
    case "$TICKER_MODE" in
        all)
            echo "--all-sp500"
            ;;
        custom)
            echo "--symbols ${TICKER_SYMBOLS}"
            ;;
        top10|*)
            echo "--top10"
            ;;
    esac
}

get_ticker_description() {
    case "$TICKER_MODE" in
        all)    echo "Alle S&P 500 (~502 Ticker)" ;;
        custom) echo "Custom: ${TICKER_SYMBOLS}" ;;
        top10)  echo "Top 10" ;;
    esac
}

# ============================================================
# Hilfsfunktionen
# ============================================================

check_conda() {
    if [[ -z "$CONDA_DEFAULT_ENV" ]] || [[ "$CONDA_DEFAULT_ENV" != "stock-streaming" ]]; then
        echo -e "${RED}❌ Conda Environment 'stock-streaming' nicht aktiviert!${NC}"
        echo "   Bitte ausfuehren: conda activate stock-streaming"
        exit 1
    fi
    echo -e "${GREEN}✅ Conda Environment: $CONDA_DEFAULT_ENV${NC}"
}

check_env_file() {
    if [[ ! -f ".env" ]]; then
        echo -e "${RED}❌ .env Datei nicht gefunden!${NC}"
        echo "   Bitte ausfuehren: cp .env.example .env && nano .env"
        exit 1
    fi

    source .env
    if [[ -z "$MASSIVE_API_KEY" ]] || [[ "$MASSIVE_API_KEY" == "your_api_key_here" ]]; then
        echo -e "${YELLOW}⚠️  MASSIVE_API_KEY nicht gesetzt in .env${NC}"
        echo "   WS Producer wird nicht funktionieren."
    else
        echo -e "${GREEN}✅ API Key konfiguriert${NC}"
    fi
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker nicht installiert!${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker verfuegbar${NC}"
}

wait_for_service() {
    local name=$1
    local host=$2
    local port=$3
    local max_wait=$4

    echo -n "   Warte auf $name ($host:$port)..."
    for i in $(seq 1 $max_wait); do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo -e " ${GREEN}bereit!${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e " ${RED}TIMEOUT!${NC}"
    return 1
}

# ============================================================
# Kommandos
# ============================================================

start_infra() {
    echo -e "${CYAN}═══ Docker Compose Infrastruktur starten ═══${NC}"
    cd infrastructure/docker
    docker compose up -d
    cd "$PROJECT_DIR"

    echo ""
    echo -e "${YELLOW}Warte auf Services...${NC}"
    wait_for_service "Kafka" "localhost" 29092 60
    wait_for_service "TimescaleDB" "localhost" 5432 30
    wait_for_service "Spark Master" "localhost" 8080 30

    echo ""
    echo -e "${GREEN}✅ Infrastruktur bereit!${NC}"
    echo ""
    echo "   Kafka:        localhost:29092"
    echo "   TimescaleDB:  localhost:5432"
    echo "   Spark UI:     http://localhost:8080"
    echo "   Kafka UI:     http://localhost:8088"
}

stop_infra() {
    echo -e "${CYAN}═══ Docker Compose stoppen ═══${NC}"
    cd infrastructure/docker
    docker compose down
    cd "$PROJECT_DIR"
    echo -e "${GREEN}✅ Infrastruktur gestoppt${NC}"
}

init_db() {
    echo -e "${CYAN}═══ Datenbank initialisieren ═══${NC}"
    python scripts/init_db.py
    echo ""
    echo -e "${CYAN}═══ Ticker seeden ═══${NC}"
    python scripts/seed_tickers.py
    echo -e "${GREEN}✅ Datenbank bereit!${NC}"
}

start_producer() {
    echo -e "${CYAN}═══ WebSocket Producer starten ═══${NC}"

    local ticker_args=$(get_producer_ticker_args)
    local ticker_desc=$(get_ticker_description)

    # Pruefe ob bereits laeuft
    if [[ -f "$PID_DIR/ws-producer.pid" ]]; then
        local pid=$(cat "$PID_DIR/ws-producer.pid")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}WS Producer laeuft bereits (PID: $pid)${NC}"
            echo -e "${YELLOW}Stoppe zuerst: ./scripts/run_local.sh stop${NC}"
            return
        fi
    fi

    echo -e "   Ticker-Modus: ${GREEN}${ticker_desc}${NC}"
    echo -e "   Producer-Args: ${ticker_args}"

    python -m backend.websocket_producer.entrypoint \
        --stream second --all --log-level INFO \
        &> /tmp/stock-platform-ws-producer.log &

    echo $! > "$PID_DIR/ws-producer.pid"
    echo -e "${GREEN}✅ WS Producer gestartet (PID: $!)${NC}"
    echo "   Logs: tail -f /tmp/stock-platform-ws-producer.log"
}

start_spark() {
    echo -e "${CYAN}═══ Spark Streaming Jobs starten ═══${NC}"

    # Second Job
    if [[ -f "$PID_DIR/spark-second.pid" ]]; then
        local pid=$(cat "$PID_DIR/spark-second.pid")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Spark Second Job laeuft bereits (PID: $pid)${NC}"
        fi
    else
        python -m backend.spark_streaming.entrypoint \
            --job second --log-level INFO \
            &> /tmp/stock-platform-spark-second.log &

        echo $! > "$PID_DIR/spark-second.pid"
        echo -e "${GREEN}✅ Spark Second Job gestartet (PID: $!)${NC}"
    fi

    # Minute Job
    if [[ -f "$PID_DIR/spark-minute.pid" ]]; then
        local pid=$(cat "$PID_DIR/spark-minute.pid")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Spark Minute Job laeuft bereits (PID: $pid)${NC}"
        fi
    else
        python -m backend.spark_streaming.entrypoint \
            --job minute --log-level INFO \
            &> /tmp/stock-platform-spark-minute.log &

        echo $! > "$PID_DIR/spark-minute.pid"
        echo -e "${GREEN}✅ Spark Minute Job gestartet (PID: $!)${NC}"
    fi

    echo "   Logs: tail -f /tmp/stock-platform-spark-second.log"
    echo "         tail -f /tmp/stock-platform-spark-minute.log"
}

start_gui() {
    echo -e "${CYAN}═══ Panel GUI starten ═══${NC}"

    if [[ -f "$PID_DIR/panel-gui.pid" ]]; then
        local pid=$(cat "$PID_DIR/panel-gui.pid")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Panel GUI laeuft bereits (PID: $pid)${NC}"
            return
        fi
    fi

    panel serve frontend/app.py \
        --port 5006 \
        --address 0.0.0.0 \
        --allow-websocket-origin="*" \
        --autoreload \
        --num-procs 1 \
        &> /tmp/stock-platform-panel.log &

    echo $! > "$PID_DIR/panel-gui.pid"
    echo -e "${GREEN}✅ Panel GUI gestartet (PID: $!)${NC}"
    echo "   URL:  http://localhost:5006/app"
    echo "   Logs: tail -f /tmp/stock-platform-panel.log"
}

stop_all() {
    echo -e "${CYAN}═══ Alle Prozesse stoppen ═══${NC}"

    for pidfile in "$PID_DIR"/*.pid; do
        if [[ -f "$pidfile" ]]; then
            local name=$(basename "$pidfile" .pid)
            local pid=$(cat "$pidfile")

            if kill -0 "$pid" 2>/dev/null; then
                echo "   Stoppe $name (PID: $pid)..."
                kill "$pid" 2>/dev/null || true
                sleep 2
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pidfile"
        fi
    done

    echo -e "${GREEN}✅ Alle Prozesse gestoppt${NC}"
}

show_status() {
    echo -e "${CYAN}═══ Status ═══${NC}"
    echo ""

    # Docker Services
    echo -e "${YELLOW}Docker Compose:${NC}"
    cd infrastructure/docker
    docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "   Nicht gestartet"
    cd "$PROJECT_DIR"
    echo ""

    # Background Prozesse
    echo -e "${YELLOW}Background Prozesse:${NC}"
    for pidfile in "$PID_DIR"/*.pid; do
        if [[ -f "$pidfile" ]]; then
            local name=$(basename "$pidfile" .pid)
            local pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "   $name: ${GREEN}RUNNING${NC} (PID: $pid)"
            else
                echo -e "   $name: ${RED}STOPPED${NC}"
            fi
        fi
    done

    if [[ -z "$(ls -A $PID_DIR 2>/dev/null)" ]]; then
        echo "   Keine Prozesse gestartet"
    fi
    echo ""

    # URLs
    echo -e "${YELLOW}URLs:${NC}"
    echo "   Panel GUI:    http://localhost:5006/app"
    echo "   Spark UI:     http://localhost:8080"
    echo "   Kafka UI:     http://localhost:8088"
}

# ============================================================
# Hauptlogik
# ============================================================

COMMAND="${1:-all}"

case "$COMMAND" in
    all)
        echo -e "${GREEN}╔══════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  🚀 Stock Platform - Lokaler Start          ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════╝${NC}"
        echo ""

        check_conda
        check_env_file
        check_docker

        echo ""
        echo -e "   Ticker-Modus: ${CYAN}$(get_ticker_description)${NC}"
        echo ""
        start_infra
        echo ""
        init_db
        echo ""
        start_producer
        echo ""
        start_spark
        echo ""
        start_gui
        echo ""

        echo -e "${GREEN}╔══════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  ✅ Alle Services gestartet!                ║${NC}"
        echo -e "${GREEN}╠══════════════════════════════════════════════╣${NC}"
        echo -e "${GREEN}║  Ticker:   $(printf '%-33s' "$(get_ticker_description)")║${NC}"
        echo -e "${GREEN}║  GUI:      http://localhost:5006/app         ║${NC}"
        echo -e "${GREEN}║  Spark:    http://localhost:8080              ║${NC}"
        echo -e "${GREEN}║  Kafka UI: http://localhost:8088              ║${NC}"
        echo -e "${GREEN}║                                              ║${NC}"
        echo -e "${GREEN}║  Stoppen: ./scripts/run_local.sh stop        ║${NC}"
        echo -e "${GREEN}║  Status:  ./scripts/run_local.sh status      ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════╝${NC}"
        ;;

    infra)
        check_docker
        start_infra
        ;;

    init)
        check_conda
        init_db
        ;;

    producer)
        check_conda
        check_env_file
        parse_ticker_args "${@:2}"
        start_producer
        ;;

    spark)
        check_conda
        start_spark
        ;;

    gui)
        check_conda
        start_gui
        ;;

    stop)
        stop_all
        ;;

    stop-all)
        stop_all
        stop_infra
        ;;

    status)
        show_status
        ;;

    logs)
        SERVICE="${2:-all}"
        case "$SERVICE" in
            producer) tail -f /tmp/stock-platform-ws-producer.log ;;
            spark)    tail -f /tmp/stock-platform-spark-second.log ;;
            gui)      tail -f /tmp/stock-platform-panel.log ;;
            all)      tail -f /tmp/stock-platform-*.log ;;
            *)        echo "Usage: $0 logs {producer|spark|gui|all}" ;;
        esac
        ;;

    *)
        echo -e "${CYAN}Usage: $0 {all|infra|init|producer|spark|gui|stop|stop-all|status|logs}${NC}"
        echo ""
        echo "  all       Alles starten (Default: Top 10 Ticker)"
        echo "  infra     Nur Docker Compose (Kafka, TimescaleDB, Spark)"
        echo "  init      Datenbank initialisieren + Tickers seeden"
        echo "  producer  Nur WebSocket Producer"
        echo "  spark     Nur Spark Streaming Jobs"
        echo "  gui       Nur Panel GUI"
        echo "  stop      Background-Prozesse stoppen"
        echo "  stop-all  Alles stoppen (inkl. Docker Compose)"
        echo "  status    Status anzeigen"
        echo "  logs      Logs anzeigen (producer|spark|gui|all)"
        echo ""
        echo -e "${YELLOW}Ticker-Optionen (nach dem Kommando):${NC}"
        echo "  --top10              Top 10 nach Marktkapitalisierung (Default)"
        echo "  --all                Alle S&P 500 Ticker (~502)"
        echo "  --symbols=AAPL,MSFT  Bestimmte Ticker"
        echo ""
        echo -e "${YELLOW}Beispiele:${NC}"
        echo "  $0 all --top10       Alles mit Top 10"
        echo "  $0 all --all         Alles mit allen S&P 500"
        echo "  $0 producer --all    Nur Producer mit allen Tickern"
        echo "  $0 producer --symbols=AAPL,MSFT,GOOGL"
        exit 1
        ;;
esac
