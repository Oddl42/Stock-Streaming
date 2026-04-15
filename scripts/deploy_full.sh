#!/bin/bash
##############################################
# Stock-Streaming Ansible Deployment Wrapper #
##############################################

set -euo pipefail

# --- Konfiguration ---
PROJECT_DIR="$HOME/stock-streaming"
ANSIBLE_DIR="$PROJECT_DIR/infrastructure/ansible"
PLAYBOOK_DIR="$ANSIBLE_DIR/playbooks"
ENV_FILE="$PROJECT_DIR/.env"
INVENTORY="$ANSIBLE_DIR/inventory"

# --- Farben ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# --- Funktionen ---
print_header() {
    echo -e "\n${CYAN}============================================${NC}"
    echo -e "${CYAN}  Stock-Streaming Deployment Tool${NC}"
    echo -e "${CYAN}============================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

usage() {
    echo -e "${CYAN}Verwendung:${NC}"
    echo "  $(basename "$0") [OPTION] [ZUSÄTZLICHE ANSIBLE-ARGUMENTE]"
    echo ""
    echo -e "${CYAN}Optionen:${NC}"
    echo "  all, 0          Vollständiges Setup (00_full_setup)"
    echo "  prereq, 1       Prerequisites installieren"
    echo "  microk8s, 2     MicroK8s installieren"
    echo "  docker, 3       Docker konfigurieren"
    echo "  cluster, 4      Cluster initialisieren"
    echo "  build, 5        Docker Images bauen"
    echo "  infra, 6        Infrastruktur deployen"
    echo "  app, 7          Applikation deployen"
    echo "  monitoring, 8   Monitoring deployen"
    echo "  verify, 9       Deployment verifizieren"
    echo ""
    echo "  step             Schrittweise alle Playbooks (01-09) ausführen"
    echo "  custom <datei>   Eigenes Playbook ausführen"
    echo "  list             Alle verfügbaren Playbooks anzeigen"
    echo "  check <nr>       Dry-Run für ein Playbook (--check)"
    echo "  -h, --help       Diese Hilfe anzeigen"
    echo ""
    echo -e "${CYAN}Beispiele:${NC}"
    echo "  $(basename "$0") all"
    echo "  $(basename "$0") app"
    echo "  $(basename "$0") 7"
    echo "  $(basename "$0") check 4"
    echo "  $(basename "$0") app --tags deploy"
    echo "  $(basename "$0") all -v"
}

load_env() {
    if [[ -f "$ENV_FILE" ]]; then
        print_info "Lade Umgebungsvariablen aus: $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
        print_success ".env Datei geladen"
    else
        print_error ".env Datei nicht gefunden: $ENV_FILE"
        exit 1
    fi
}

# In das Ansible-Verzeichnis wechseln
cd "$ANSIBLE_DIR"
print_info "Arbeitsverzeichnis: $(pwd)"

check_prerequisites() {
    local missing=0

    if ! command -v ansible-playbook &> /dev/null; then
        print_error "ansible-playbook ist nicht installiert!"
        missing=1
    fi

    if [[ ! -d "$ANSIBLE_DIR" ]]; then
        print_error "Ansible-Verzeichnis nicht gefunden: $ANSIBLE_DIR"
        missing=1
    fi

    if [[ ! -d "$PLAYBOOK_DIR" ]]; then
        print_error "Playbook-Verzeichnis nicht gefunden: $PLAYBOOK_DIR"
        missing=1
    fi

    if [[ $missing -eq 1 ]]; then
        exit 1
    fi

    print_success "Alle Voraussetzungen erfüllt"
}

# Playbook anhand der Nummer oder des Namens auflösen
resolve_playbook() {
    local input="$1"
    local playbook=""

    case "$input" in
        all|0)        playbook="00_full_setup.yaml" ;;
        prereq|1)     playbook="01_prerequisites.yaml" ;;
        microk8s|2)   playbook="02_microk8s.yaml" ;;
        docker|3)     playbook="03_docker.yaml" ;;
        cluster|4)    playbook="04_cluster_init.yaml" ;;
        build|5)      playbook="05_build_images.yaml" ;;
        infra|6)      playbook="06_deploy_infra.yaml" ;;
        app|7)        playbook="07_deploy_app.yaml" ;;
        monitoring|8) playbook="08_deploy_monitoring.yaml" ;;
        verify|9)     playbook="09_verify.yaml" ;;
        *)
            print_error "Unbekannte Option: $input"
            usage
            exit 1
            ;;
    esac

    echo "$playbook"
}

run_playbook() {
    local playbook="$1"
    shift
    local extra_args=("$@")
    local playbook_path="$PLAYBOOK_DIR/$playbook"

    if [[ ! -f "$playbook_path" ]]; then
        print_error "Playbook nicht gefunden: $playbook_path"
        exit 1
    fi

    echo ""
    print_info "Starte Playbook: $playbook"
    print_info "Pfad: $playbook_path"
    if [[ ${#extra_args[@]} -gt 0 ]]; then
        print_info "Zusätzliche Argumente: ${extra_args[*]}"
    fi
    echo -e "${YELLOW}-------------------------------------------${NC}"

    ansible-playbook \
        -i "$INVENTORY" \
        -K \
        "$playbook_path" \
        "${extra_args[@]}"

    local exit_code=$?

    echo -e "${YELLOW}-------------------------------------------${NC}"
    if [[ $exit_code -eq 0 ]]; then
        print_success "Playbook '$playbook' erfolgreich abgeschlossen!"
    else
        print_error "Playbook '$playbook' fehlgeschlagen! (Exit-Code: $exit_code)"
        exit $exit_code
    fi
}

run_step_by_step() {
    local extra_args=("$@")
    local playbooks=(
        "01_prerequisites.yaml"
        "02_microk8s.yaml"
        "03_docker.yaml"
        "04_cluster_init.yaml"
        "05_build_images.yaml"
        "06_deploy_infra.yaml"
        "07_deploy_app.yaml"
        "08_deploy_monitoring.yaml"
        "09_verify.yaml"
    )

    local total=${#playbooks[@]}
    local current=0

    for playbook in "${playbooks[@]}"; do
        ((current++))
        echo ""
        echo -e "${CYAN}[$current/$total] ========================================${NC}"
        run_playbook "$playbook" "${extra_args[@]}"
    done

    echo ""
    print_success "🎉 Alle $total Playbooks erfolgreich abgeschlossen!"
}

list_playbooks() {
    print_info "Verfügbare Playbooks in: $PLAYBOOK_DIR"
    echo ""
    if [[ -d "$PLAYBOOK_DIR" ]]; then
        for f in "$PLAYBOOK_DIR"/*.yaml "$PLAYBOOK_DIR"/*.yml; do
            [[ -f "$f" ]] && echo -e "  ${GREEN}▸${NC} $(basename "$f")"
        done
    fi
}

# --- Hauptprogramm ---
print_header

# Keine Argumente → Hilfe anzeigen
if [[ $# -eq 0 ]]; then
    usage
    exit 0
fi

# Voraussetzungen prüfen
check_prerequisites

# .env laden
load_env

# Erstes Argument auswerten
COMMAND="$1"
shift

case "$COMMAND" in
    -h|--help)
        usage
        exit 0
        ;;
    list)
        list_playbooks
        exit 0
        ;;
    step)
        run_step_by_step "$@"
        ;;
    check)
        if [[ $# -lt 1 ]]; then
            print_error "'check' benötigt eine Playbook-Nummer oder einen Namen"
            exit 1
        fi
        PLAYBOOK=$(resolve_playbook "$1")
        shift
        run_playbook "$PLAYBOOK" --check "$@"
        ;;
    custom)
        if [[ $# -lt 1 ]]; then
            print_error "'custom' benötigt einen Dateinamen"
            exit 1
        fi
        CUSTOM_PLAYBOOK="$1"
        shift
        run_playbook "$CUSTOM_PLAYBOOK" "$@"
        ;;
    *)
        PLAYBOOK=$(resolve_playbook "$COMMAND")
        run_playbook "$PLAYBOOK" "$@"
        ;;
esac
