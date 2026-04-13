#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# AV-02 SSA — Inicialização completa do ambiente Airflow
# Execute UMA VEZ antes de subir o docker-compose
# Uso: bash setup.sh
# ─────────────────────────────────────────────────────────────────

set -e

echo ""
echo "============================================="
echo "  AV-02 SSA — Setup do ambiente Airflow 3.x  "
echo "============================================="
echo ""

# 1. Gera .env com o UID do usuário atual (necessário para permissões dos volumes)
echo "→ Gerando .env com AIRFLOW_UID=$(id -u)..."
cat > .env <<EOF
AIRFLOW_UID=$(id -u)
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.8
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt_secret_av02
AIRFLOW__API_AUTH__JWT_ISSUER=airflow
EOF
echo "   .env criado."

# 2. Cria as pastas necessárias
echo "→ Criando pastas de trabalho..."
mkdir -p ./dags ./logs ./plugins ./config ./data
echo "   Pastas prontas."

# 3. Baixa o dataset de entrada
echo "→ Baixando dados-stream.csv para ./data/..."
curl -sSL \
  "https://raw.githubusercontent.com/esensato/ssa-2026-01/refs/heads/main/dados-stream.csv" \
  -o ./data/dados-stream.csv
echo "   dados-stream.csv baixado: $(wc -l < ./data/dados-stream.csv) linhas."

# 4. Instala dependências Python localmente (para validação offline, opcional)
# pip install -r requirements.txt --quiet

# 5. Inicializa o banco do Airflow
echo "→ Rodando airflow-init (migração do banco + criação do usuário admin)..."
echo "   (pode levar alguns minutos na primeira vez)"
docker compose up airflow-init
echo "   Init concluído."

echo ""
echo "============================================="
echo "  Setup finalizado!                          "
echo ""
echo "  Para subir o ambiente:                     "
echo "    docker compose up -d                     "
echo ""
echo "  Acesse em:  http://localhost:8080          "
echo "  Usuário:    admin                          "
echo "  Senha:      admin                          "
echo "============================================="
echo ""
