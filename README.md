# AV-02 SSA — Pipeline de Streaming Musical com Apache Airflow

Pipeline de análise de dados de um app de stream musical, implementado com **Apache Airflow 3.x** e **PostgreSQL**.

---

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) instalado
- [Docker Compose](https://docs.docker.com/compose/install/) v2+
- Git
- Acesso ao [GitHub Codespaces](https://github.com/codespaces) (recomendado pelo professor)

---

## Como rodar (passo a passo)

### 1. Clone o repositório

```bash
git clone https://github.com/Marcio-Alexandroni/ssa-av02-airflow.git
cd ssa-av02-airflow
```

### 2. Execute o setup inicial (apenas uma vez)

```bash
bash setup.sh
```

O script faz automaticamente:
- Gera o arquivo `.env` com o `AIRFLOW_UID` correto
- Cria as pastas necessárias (`dags/`, `logs/`, `data/`, etc.)
- Baixa o dataset `dados-stream.csv` para `./data/`
- Inicializa o banco de dados do Airflow

### 3. Suba o ambiente

```bash
docker compose up -d
```

### 4. Acesse o Airflow

Abra no navegador: [http://localhost:8080](http://localhost:8080)

| Campo | Valor |
|---|---|
| Usuário | `airflow` |
| Senha | `airflow` |

### 5. Execute a DAG

- Encontre a DAG `av02_stream_pipeline` na lista
- Clique no toggle para ativar
- Clique em **Trigger DAG** para executar manualmente

---

## Fluxo do Pipeline

```
TASK-1  Copia dados-stream.csv → entrada.csv
  └► TASK-2  Normaliza datas (yyyy-mm-dd → dd/mm/aaaa) → task2.csv
        └► TASK-3  Remove linhas sem nome_musica → task3.csv  [xcom: descartados]
              ├► TASK-4  INSERT na tabela `descartados` (qtd. de linhas removidas)
              └► TASK-5  SELECT da tabela `genero_musical`
                    └► TASK-6  Enriquece com nome_genero → task4.csv
                          ├► TASK-7  Média de avaliação por música → media_avaliacao.csv  ─┐
                          └► TASK-8  Total de músicas por artista → total_artista.csv      ─┤ paralelas
                                                                                             ▼
                                                                         TASK-9  Remove entrada.csv (ALL_DONE)
                                                                               └► TASK-10  Fim do pipeline
```

---

## Arquivos gerados pelo pipeline

| Arquivo | Conteúdo |
|---|---|
| `data/entrada.csv` | Cópia do dataset original (removida ao final) |
| `data/task2.csv` | Dataset com datas normalizadas |
| `data/task3.csv` | Dataset sem registros com `nome_musica` vazio |
| `data/task4.csv` | Dataset enriquecido com `nome_genero` |
| `data/media_avaliacao.csv` | Média de nota por música |
| `data/total_artista.csv` | Total de músicas ouvidas por artista |

---

## Estrutura do repositório

```
ssa-av02-airflow/
├── dags/
│   └── av02_stream_dag.py   ← DAG principal
├── sql/
│   └── init.sql             ← Cria tabelas e insere gêneros automaticamente
├── data/                    ← Gerado pelo setup.sh (não versionado)
├── logs/                    ← Gerado pelo Airflow (não versionado)
├── docker-compose.yaml      ← Ambiente completo
├── requirements.txt         ← Dependências Python
├── setup.sh                 ← Script de inicialização
└── .env.example             ← Modelo do .env
```

---

## Dependências Python

Todas declaradas em `requirements.txt` e instaladas automaticamente pelos containers:

| Pacote | Função |
|---|---|
| `pandas` | Leitura, transformação e escrita de CSVs |
| `psycopg2-binary` | Conector PostgreSQL |
| `apache-airflow-providers-common-sql` | `SQLExecuteQueryOperator` |
| `apache-airflow-providers-postgres` | Conexão `postgres_default` |
| `apache-airflow-providers-standard` | `BashOperator`, `PythonOperator` |
| `pendulum` | Timezone `America/Sao_Paulo` |

---

## Tabelas no banco de dados

### `genero_musical`
| id_genero | nome_genero |
|---|---|
| 001 | POP |
| 002 | ROCK |
| 003 | RAP |
| 004 | SOUL |
| 005 | OUTROS |

### `descartados`
Registra o total de linhas removidas na TASK-3 a cada execução.

---

## Referência

- Repositório do professor: [esensato/ssa-2026-01](https://github.com/esensato/ssa-2026-01)
- Enunciado da avaliação: [AV-02-SSA-Airflow.md](https://github.com/esensato/ssa-2026-01/blob/main/AV-02-SSA-Airflow.md)
