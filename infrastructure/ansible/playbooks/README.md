# ============================================================
# SCHRITT 1: Setup-Skript ausführen (Conda Env)
# ============================================================
chmod +x setup.sh
./setup.sh
conda activate stock-streaming

# ============================================================
# SCHRITT 2: Ansible Inventory prüfen
# ============================================================
cd infrastructure/ansible
ansible-inventory --list

# ============================================================
# SCHRITT 3: Vollständiges Setup ausführen
# ============================================================

# Option A: Alles auf einmal
ansible-playbook playbooks/00_full_setup.yaml

# Option B: Schrittweise
ansible-playbook playbooks/01_prerequisites.yaml
ansible-playbook playbooks/02_microk8s.yaml
ansible-playbook playbooks/03_docker.yaml
ansible-playbook playbooks/04_cluster_init.yaml
ansible-playbook playbooks/05_build_images.yaml
ansible-playbook playbooks/06_deploy_infra.yaml
ansible-playbook playbooks/07_deploy_app.yaml
ansible-playbook playbooks/08_deploy_monitoring.yaml
ansible-playbook playbooks/09_verify.yaml

# Option C: Nur bestimmte Teile (Tags)
ansible-playbook playbooks/00_full_setup.yaml --tags "kafka,timescaledb"
ansible-playbook playbooks/00_full_setup.yaml --tags "app"
ansible-playbook playbooks/00_full_setup.yaml --tags "monitoring"

# ============================================================
# SCHRITT 4: Lokales Testing (ohne K8s, via Docker Compose)
# ============================================================
cd infrastructure/docker
docker compose up -d
# Dann Spark-Jobs und Panel GUI lokal starten

# ============================================================
# SCHRITT 5: Aufräumen
# ============================================================
ansible-playbook playbooks/99_teardown.yaml
