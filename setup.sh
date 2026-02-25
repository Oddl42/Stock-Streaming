#!/bin/bash
set -e

# ============================================
# Stock Streaming Platform - Setup Script
# ============================================

CONDA_ENV_NAME="stock-streaming"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINICONDA_DIR="$HOME/miniconda3"

echo "============================================"
echo "  Stock Streaming Platform - Setup"
echo "============================================"

# --- 1. Check if Conda is installed, otherwise install Miniconda ---
if ! command -v conda &> /dev/null; then
    echo "⚠️  Conda ist nicht installiert."
    read -p "Möchtest du Miniconda jetzt automatisch installieren? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Abbruch. Bitte installiere Anaconda/Miniconda manuell:"
        echo "  https://docs.conda.io/en/latest/miniconda.html"
        exit 1
    fi

    echo "📥 Installiere Miniconda..."

    # Betriebssystem und Architektur erkennen
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "${OS}" in
        Linux)
            case "${ARCH}" in
                x86_64)  MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" ;;
                aarch64) MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh" ;;
                *)       echo "❌ Nicht unterstützte Architektur: ${ARCH}"; exit 1 ;;
            esac
            ;;
        Darwin)
            case "${ARCH}" in
                x86_64)  MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh" ;;
                arm64)   MINICONDA_URL="https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh" ;;
                *)       echo "❌ Nicht unterstützte Architektur: ${ARCH}"; exit 1 ;;
            esac
            ;;
        *)
            echo "❌ Nicht unterstütztes Betriebssystem: ${OS}"
            exit 1
            ;;
    esac

    echo "  OS: ${OS}, Arch: ${ARCH}"
    echo "  URL: ${MINICONDA_URL}"

    # Verzeichnis erstellen
    mkdir -p "${MINICONDA_DIR}"

    # Installer herunterladen
    if command -v wget &> /dev/null; then
        wget -q --show-progress "${MINICONDA_URL}" -O "${MINICONDA_DIR}/miniconda.sh"
    elif command -v curl &> /dev/null; then
        curl -fsSL "${MINICONDA_URL}" -o "${MINICONDA_DIR}/miniconda.sh"
    else
        echo "❌ Weder 'wget' noch 'curl' gefunden. Bitte eines davon installieren."
        exit 1
    fi

    # Installer ausführen (batch mode, update, prefix)
    bash "${MINICONDA_DIR}/miniconda.sh" -b -u -p "${MINICONDA_DIR}"

    # Installer aufräumen
    rm -f "${MINICONDA_DIR}/miniconda.sh"

    # Conda initialisieren für aktuelle Shell
    eval "$("${MINICONDA_DIR}/bin/conda" shell.bash hook)"

    # Conda init für bash und zsh dauerhaft einrichten
    "${MINICONDA_DIR}/bin/conda" init bash
    if [ -n "${ZSH_VERSION:-}" ] || [ -f "$HOME/.zshrc" ]; then
        "${MINICONDA_DIR}/bin/conda" init zsh
    fi

    echo "✅ Miniconda wurde erfolgreich installiert nach: ${MINICONDA_DIR}"
    echo ""
    echo "ℹ️  Hinweis: Damit 'conda' in neuen Terminals verfügbar ist,"
    echo "   starte dein Terminal nach dem Setup einmal neu."
    echo ""
fi

echo "✅ Conda gefunden: $(conda --version)"

# --- 2. Remove existing environment if present ---
if conda env list | grep -q "^${CONDA_ENV_NAME} "; then
    echo "⚠️  Environment '${CONDA_ENV_NAME}' existiert bereits."
    read -p "Möchtest du es neu erstellen? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Lösche bestehende Environment..."
        conda env remove -n "${CONDA_ENV_NAME}" -y
    else
        echo "Aktiviere bestehende Environment..."
        eval "$(conda shell.bash hook)"
        conda activate "${CONDA_ENV_NAME}"
        echo "✅ Environment '${CONDA_ENV_NAME}' aktiviert."
        exit 0
    fi
fi

# --- 3. Create Conda Environment ---
echo "📦 Erstelle Conda Environment '${CONDA_ENV_NAME}'..."
conda env create -f "${SCRIPT_DIR}/environment.yaml"

# --- 4. Activate Environment ---
eval "$(conda shell.bash hook)"
conda activate "${CONDA_ENV_NAME}"
echo "✅ Environment '${CONDA_ENV_NAME}' aktiviert."

# --- 5. Verify installations ---
echo ""
echo "🔍 Verifiziere Installationen..."
echo "  Python:     $(python --version)"
echo "  Panel:      $(python -c 'import panel; print(panel.__version__)')"
echo "  Bokeh:      $(python -c 'import bokeh; print(bokeh.__version__)')"
echo "  PySpark:    $(python -c 'import pyspark; print(pyspark.__version__)')"
echo "  SQLAlchemy: $(python -c 'import sqlalchemy; print(sqlalchemy.__version__)')"
echo "  Kafka:      $(python -c 'import kafka; print(kafka.__version__)')"
echo "  Pandas:     $(python -c 'import pandas; print(pandas.__version__)')"

# --- 6. Setup Java for Spark ---
echo ""
echo "☕ Prüfe Java Installation (benötigt für Spark)..."
if ! command -v java &> /dev/null; then
    echo "⚠️  Java nicht gefunden. Installiere OpenJDK 11 via conda..."
    conda install -n "${CONDA_ENV_NAME}" -c conda-forge openjdk=11 -y
else
    echo "✅ Java gefunden: $(java -version 2>&1 | head -n 1)"
fi

# --- 7. Create .env template ---
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    echo ""
    echo "📝 Erstelle .env Template..."
    cat > "${SCRIPT_DIR}/.env" << 'EOF'
# Massive.com API
MASSIVE_API_KEY=your_api_key_here
MASSIVE_WS_URL=wss://delayed.massive.com

# PostgreSQL / TimescaleDB
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_streaming
DB_USER=postgres
DB_PASSWORD=your_db_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_SECOND=stocks.aggregates.second
KAFKA_TOPIC_MINUTE=stocks.aggregates.minute

# Spark
SPARK_MASTER=local[*]
SPARK_APP_NAME=stock-streaming

# Panel GUI
PANEL_PORT=5006
PANEL_ADDRESS=0.0.0.0
EOF
    echo "✅ .env Template erstellt. Bitte API Keys eintragen!"
fi

# --- 8. Create project directories ---
echo ""
echo "📁 Erstelle Projektverzeichnisse..."
mkdir -p "${SCRIPT_DIR}"/{data,config,backend/{database/migrations,websocket_producer,spark_streaming,data_service,api},frontend/{layouts,components,callbacks,charts,styles},infrastructure/{ansible/{inventory,playbooks,roles},helm/stock-platform/templates/{kafka,timescaledb,spark,app,monitoring},docker},monitoring/{prometheus,grafana/dashboards,exporters},tests,scripts}

echo ""
echo "============================================"
echo "  ✅ Setup abgeschlossen!"
echo "============================================"
echo ""
echo "Nächste Schritte:"
echo "  1. conda activate ${CONDA_ENV_NAME}"
echo "  2. API Key in .env eintragen"
echo "  3. ./scripts/run_local.sh  (für lokales Testing)"
echo "  4. ansible-playbook infrastructure/ansible/playbooks/06_deploy_all.yaml"
echo ""
