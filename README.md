# AV-02 SSA — Pipeline de Streaming Musical com Apache Airflow 3.1.8

Pipeline de análise de dados de um app de stream musical, implementado com **Apache Airflow 3.1.8** e **PostgreSQL 16**.

Ambiente idêntico ao utilizado pelo professor ([esensato/temp](https://github.com/esensato/temp)):
- `LocalExecutor` + `SimpleAuthManager`
- Fernet Key e `airflow.cfg` originais

---

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) + [Docker Compose](https://docs.docker.com/compose/install/) v2+
- Git
- [GitHub Codespaces](https://github.com/codespaces) (recomendado)

---

## Como rodar

### 1. Clone o repositório

```bash
git clone https://github.com/Marcio-Alexandroni/ssa-av02-airflow.git
cd ssa-av02-airflow
```

### 2. Execute o setup (apenas uma vez)

```bash
bash setup.sh
```

O script faz:
- Gera `.env` com `AIRFLOW_UID` correto
- Cria as pastas (`dags/`, `logs/`, `data/`, etc.)
- Baixa `dados-stream.csv` direto do repositório do professor
- Roda `airflow-init` (migra banco + cria usuário `admin`)

### 3. Suba o ambiente

```bash
docker compose up -d
```

### 4. Acesse a UI

[http://localhost:8080](http://localhost:8080)

| Campo | Valor |
|---|---|
| Usuário | `admin` |
| Senha | `admin` |

### 5. Configure a conexão com o PostgreSQL

Na UI: **Admin → Connections → +**

| Campo | Valor |
|---|---|
| Connection Id | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `airflow` |
| Login | `airflow` |
| Password | `airflow` |
| Port | `5432` |

> As tabelas `genero_musical` e `descartados` já são criadas automaticamente pelo `sql/init.sql` na primeira subida do PostgreSQL.

### 6. Execute a DAG

- Encontre `av02_stream_pipeline` na lista de DAGs
- Ative o toggle
- Clique em **Trigger DAG ▶**

---

## Fluxo do Pipeline

```
TASK-1  Copia dados-stream.csv → entrada.csv
  └► TASK-2  Normaliza datas (yyyy-mm-dd → dd/mm/aaaa) → task2.csv
        └► TASK-3  Remove linhas sem nome_musica → task3.csv  [xcom: descartados]
              ├► TASK-4  INSERT na tabela `descartados`
              └► TASK-5  SELECT tabela `genero_musical`
                    └► TASK-6  Enriquece com nome_genero → task4.csv
                          ├► TASK-7  Média de avaliação por música → media_avaliacao.csv  ─┐ paralelas
                          └► TASK-8  Total de músicas por artista → total_artista.csv      ─┘
                                                                                             ▼
                                                                     TASK-9  rm entrada.csv (ALL_DONE)
                                                                           └► TASK-10  Fim
```

---

## Arquivos gerados

| Arquivo | Conteúdo |
|---|---|
| `data/task2.csv` | Dataset com datas normalizadas |
| `data/task3.csv` | Sem registros com `nome_musica` vazio |
| `data/task4.csv` | Com coluna `nome_genero` |
| `data/media_avaliacao.csv` | Média de nota por música |
| `data/total_artista.csv` | Total de plays por artista |

---

## Estrutura do repositório

```
ssa-av02-airflow/
├── dags/
│   └── av02_stream_dag.py     ← DAG principal
├── sql/
│   └── init.sql               ← Cria tabelas e insere gêneros automaticamente
├── config/
│   └── airflow.cfg            ← Idêntico ao do professor (SimpleAuthManager, Fernet Key)
├── data/                      ← Gerado pelo setup.sh
├── logs/                      ← Gerado pelo Airflow
├── docker-compose.yaml        ← Airflow 3.1.8 + PostgreSQL 16
├── requirements.txt           ← Dependências Python
├── setup.sh                   ← Inicialização com um comando
└── .env                       ← Gerado pelo setup.sh
```

---

## Referências

- Ambiente do professor: [esensato/temp](https://github.com/esensato/temp)
- Enunciado: [AV-02-SSA-Airflow.md](https://github.com/esensato/ssa-2026-01/blob/main/AV-02-SSA-Airflow.md)
- Docs oficiais: [airflow.apache.org/docs/docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
