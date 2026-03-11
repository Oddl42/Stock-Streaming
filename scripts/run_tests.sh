#!/bin/bash
set -e

# ============================================================
# Test Runner Script — Stock Streaming Platform
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "============================================"
echo "  🧪 Stock Streaming Platform - Tests"
echo "============================================"
echo ""

# Farben
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Argumente
TEST_TYPE="${1:-unit}"
VERBOSE="${2:-}"

case "$TEST_TYPE" in
    # ==========================================
    # Unit Tests (schnell, ohne externe Services)
    # ==========================================
    unit)
        echo -e "${GREEN}▶ Running Unit Tests...${NC}"
        python -m pytest tests/unit/ \
            -m "not spark" \
            -v \
            --tb=short \
            --cov=backend \
            --cov=frontend \
            --cov-config=.coveragerc \
            --cov-report=term-missing \
            --cov-report=html:reports/coverage-unit \
            -x \
            ${VERBOSE:+--verbose}
        ;;

    # ==========================================
    # Spark Tests (benötigen lokale SparkSession)
    # ==========================================
    spark)
        echo -e "${GREEN}▶ Running Spark Tests...${NC}"
        python -m pytest tests/unit/ \
            -m "spark" \
            -v \
            --tb=short \
            --timeout=120 \
            ${VERBOSE:+--verbose}
        ;;

    # ==========================================
    # Integration Tests (benötigen Docker Services)
    # ==========================================
    integration)
        echo -e "${YELLOW}▶ Running Integration Tests...${NC}"
        echo "  Stelle sicher, dass Docker Compose läuft:"
        echo "  docker compose -f infrastructure/docker/docker-compose.yaml up -d"
        echo ""

        python -m pytest tests/integration/ \
            -v \
            --tb=long \
            --timeout=120 \
            ${VERBOSE:+--verbose}
        ;;

    # ==========================================
    # Alle Tests
    # ==========================================
    all)
        echo -e "${GREEN}▶ Running ALL Tests...${NC}"
        python -m pytest tests/ \
            -v \
            --tb=short \
            --cov=backend \
            --cov=frontend \
            --cov-config=.coveragerc \
            --cov-report=term-missing \
            --cov-report=html:reports/coverage-all \
            --timeout=120 \
            ${VERBOSE:+--verbose}
        ;;

    # ==========================================
    # Nur schnelle Tests (CI/CD)
    # ==========================================
    fast)
        echo -e "${GREEN}▶ Running Fast Tests (CI/CD)...${NC}"
        python -m pytest tests/unit/ \
            -m "not spark and not slow" \
            --tb=short \
            -q \
            --cov=backend \
            --cov=frontend \
            --cov-config=.coveragerc \
            -x
        ;;

    *)
        echo -e "${RED}Usage: $0 {unit|spark|integration|all|fast}${NC}"
        echo ""
        echo "  unit          Unit Tests (schnell, ohne Docker)"
        echo "  spark         Spark-spezifische Tests"
        echo "  integration   Integration Tests (Docker Services nötig)"
        echo "  all           Alle Tests"
        echo "  fast          Schnelle Tests für CI/CD"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  ✅ Tests abgeschlossen!${NC}"
echo -e "${GREEN}============================================${NC}"
