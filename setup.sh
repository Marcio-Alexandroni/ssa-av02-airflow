#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# AV-02 SSA — Script de inicialização completa do ambiente
# Execute UMA VEZ antes de subir o docker-compose
# Uso: bash setup.sh
# ─────────────────────────────────────────────────────────────────

set -e

echo ""
echo "=========================================="
echo "  AV-02 SSA — Setup do ambiente Airflow  "
echo "=========================================="
echo ""

# 1. Gera o .env com o UID correto do usuário atual
echo "→ Gerando .env com AIRFLOW_UID=$(id -u)..."
echo "AIRFLOW_UID=$(id -u)" > .env
echo "   .env criado."

# 2. Garante que as pastas existam com as permissões certas
echo "→ Criando pastas de trabalho..."
mkdir -p ./dags ./logs ./plugins ./config ./data
echo "   Pastas prontas."

# 3. Baixa o dataset de entrada
echo "→ Baixando dados-stream.csv para ./data/..."
curl -sSL \
  "https://raw.githubusercontent.com/esensato/ssa-2026-01/refs/heads/main/dados-stream.csv" \
  -o ./data/dados-stream.csv
echo "   dados-stream.csv baixado: $(wc -l < ./data/dados-stream.csv) linhas."

# 4. Inicializa o banco do Airflow (cria usuário admin e migra o schema)
echo "→ Iniciando airflow-init (pode levar alguns minutos na primeira vez)..."
docker compose up airflow-init
echo "   Init concluído."

echo ""
echo "=========================================="
echo "  Setup finalizado!                       "
echo ""
echo "  Suba o ambiente com:                    "
echo "    docker compose up -d                  "
echo ""
echo "  Acesse em:  http://localhost:8080        "
echo "  Usuário:    airflow                      "
echo "  Senha:      airflow                      "
echo "=========================================="
echo ""
