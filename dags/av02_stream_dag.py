"""
AV-02 SSA — Pipeline de análise de músicas em streaming
Apache Airflow 3.1.8

Fluxo:
  TASK-1  → TASK-2 → TASK-3 ─┬─ TASK-4 (INSERT descartados)
                               └─ TASK-5 (SELECT genero_musical)
                                     └─ TASK-6 (enriquece com nome_genero)
                                           ├─ TASK-7 (média avaliação)  ─┐ paralelas
                                           └─ TASK-8 (total artista)    ─┘
                                                                           ↓
                                                               TASK-9 (rm entrada.csv — ALL_DONE)
                                                                     └─ TASK-10 (fim)
"""

import pendulum
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────
# Caminho base dos dados dentro do container
# ─────────────────────────────────────────────
DATA = "/opt/airflow/data"


# ─────────────────────────────────────────────
# Funções das tasks Python
# ─────────────────────────────────────────────

def tratar_datas():
    """TASK-2: converte datas yyyy-mm-dd → dd/mm/aaaa e salva task2.csv."""
    df = pd.read_csv(f"{DATA}/entrada.csv", sep=";")

    def normalizar(data):
        data = str(data).strip()
        # formato americano: yyyy-mm-dd (5º char é '-')
        if len(data) == 10 and data[4] == "-":
            ano, mes, dia = data.split("-")
            return f"{dia}/{mes}/{ano}"
        return data

    df["data_execucao"] = df["data_execucao"].apply(normalizar)
    df.to_csv(f"{DATA}/task2.csv", sep=";", index=False)
    print(f"[TASK-2] OK — {len(df)} registros gravados em task2.csv")


def remover_musicas_vazias():
    """TASK-3: remove linhas com nome_musica vazio, salva task3.csv e publica
    a quantidade de descartados via xcom para a TASK-4."""
    df = pd.read_csv(f"{DATA}/task2.csv", sep=";")
    total_antes = len(df)

    df_limpo = df[df["nome_musica"].notna() & (df["nome_musica"].str.strip() != "")]
    descartados = total_antes - len(df_limpo)

    df_limpo.to_csv(f"{DATA}/task3.csv", sep=";", index=False)

    get_current_context()["ti"].xcom_push(key="descartados", value=descartados)
    print(f"[TASK-3] OK — antes: {total_antes} | descartados: {descartados} | restantes: {len(df_limpo)}")


def enriquecer_com_genero():
    """TASK-6: lê o resultado do SELECT da TASK-5 (lista de tuplas) e cria
    a coluna nome_genero em task4.csv."""
    ti = get_current_context()["ti"]

    resultado_sql = ti.xcom_pull(task_ids="task_5_select_genero")
    if not resultado_sql:
        raise ValueError("[TASK-6] Resultado da TASK-5 está vazio!")

    # resultado_sql = [(id_genero, nome_genero), ...]
    mapa_genero = {str(row[0]).strip().zfill(3): str(row[1]).strip() for row in resultado_sql}
    print(f"[TASK-6] Mapa de gêneros: {mapa_genero}")

    df = pd.read_csv(f"{DATA}/task3.csv", sep=";")
    df["id_genero"] = df["id_genero"].astype(str).str.strip().str.zfill(3)
    df["nome_genero"] = df["id_genero"].map(mapa_genero).fillna("DESCONHECIDO")

    df.to_csv(f"{DATA}/task4.csv", sep=";", index=False)
    print(f"[TASK-6] OK — task4.csv com {len(df)} registros")


def media_avaliacao_por_musica():
    """TASK-7: calcula a média de nota por música → media_avaliacao.csv."""
    df = pd.read_csv(f"{DATA}/task4.csv", sep=";")
    resultado = (
        df.groupby("nome_musica")["nota"]
        .mean()
        .reset_index()
        .rename(columns={"nota": "media_avaliacao"})
        .sort_values("media_avaliacao", ascending=False)
    )
    resultado["media_avaliacao"] = resultado["media_avaliacao"].round(2)
    resultado.to_csv(f"{DATA}/media_avaliacao.csv", sep=";", index=False)
    print(f"[TASK-7] OK — {len(resultado)} músicas em media_avaliacao.csv")


def total_musicas_por_artista():
    """TASK-8: conta o total de músicas ouvidas por artista → total_artista.csv."""
    df = pd.read_csv(f"{DATA}/task4.csv", sep=";")
    resultado = (
        df.groupby("nome_artista")
        .size()
        .reset_index(name="total_musicas")
        .sort_values("total_musicas", ascending=False)
    )
    resultado.to_csv(f"{DATA}/total_artista.csv", sep=";", index=False)
    print(f"[TASK-8] OK — {len(resultado)} artistas em total_artista.csv")


# ─────────────────────────────────────────────
# Definição da DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="av02_stream_pipeline",
    description="AV-02 SSA — Pipeline de análise de streaming musical",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["av02", "ssa", "stream"],
) as dag:

    # ------------------------------------------------------------------
    # TASK-1: copia dados-stream.csv → entrada.csv
    # ------------------------------------------------------------------
    task_1 = BashOperator(
        task_id="task_1_copiar_arquivo",
        bash_command=f"cp {DATA}/dados-stream.csv {DATA}/entrada.csv && echo '[TASK-1] OK'",
    )

    # ------------------------------------------------------------------
    # TASK-2: normaliza datas → task2.csv
    # ------------------------------------------------------------------
    task_2 = PythonOperator(
        task_id="task_2_tratar_datas",
        python_callable=tratar_datas,
    )

    # ------------------------------------------------------------------
    # TASK-3: remove nome_musica vazio → task3.csv  +  xcom: descartados
    # ------------------------------------------------------------------
    task_3 = PythonOperator(
        task_id="task_3_remover_vazios",
        python_callable=remover_musicas_vazias,
    )

    # ------------------------------------------------------------------
    # TASK-4: INSERT do total de descartados na tabela `descartados`
    # O valor vem do xcom da task_3 via template Jinja
    # ------------------------------------------------------------------
    task_4 = SQLExecuteQueryOperator(
        task_id="task_4_insert_descartados",
        conn_id="postgres_default",
        sql="""
            INSERT INTO descartados (total)
            VALUES ({{ ti.xcom_pull(task_ids='task_3_remover_vazios', key='descartados') }});
        """,
    )

    # ------------------------------------------------------------------
    # TASK-5: SELECT da tabela genero_musical
    # Resultado fica automaticamente no xcom como lista de tuplas
    # ------------------------------------------------------------------
    task_5 = SQLExecuteQueryOperator(
        task_id="task_5_select_genero",
        conn_id="postgres_default",
        sql="SELECT id_genero, nome_genero FROM genero_musical ORDER BY id_genero;",
    )

    # ------------------------------------------------------------------
    # TASK-6: enriquece task3.csv com nome_genero → task4.csv
    # ------------------------------------------------------------------
    task_6 = PythonOperator(
        task_id="task_6_enriquecer_genero",
        python_callable=enriquecer_com_genero,
    )

    # ------------------------------------------------------------------
    # TASK-7: média de avaliação por música → media_avaliacao.csv
    # ------------------------------------------------------------------
    task_7 = PythonOperator(
        task_id="task_7_media_avaliacao",
        python_callable=media_avaliacao_por_musica,
    )

    # ------------------------------------------------------------------
    # TASK-8: total de músicas por artista → total_artista.csv
    # Executa EM PARALELO com TASK-7
    # ------------------------------------------------------------------
    task_8 = PythonOperator(
        task_id="task_8_total_artista",
        python_callable=total_musicas_por_artista,
    )

    # ------------------------------------------------------------------
    # TASK-9: remove entrada.csv independente do resultado das tasks anteriores
    # trigger_rule=ALL_DONE garante execução mesmo com falha em TASK-7 ou TASK-8
    # ------------------------------------------------------------------
    task_9 = BashOperator(
        task_id="task_9_remover_entrada",
        bash_command=f"rm -f {DATA}/entrada.csv && echo '[TASK-9] entrada.csv removido'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ------------------------------------------------------------------
    # TASK-10: marca o fim do pipeline (sem operação)
    # ------------------------------------------------------------------
    task_10 = BashOperator(
        task_id="task_10_fim_pipeline",
        bash_command="echo '[TASK-10] Pipeline finalizado com sucesso.'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ─────────────────────────────────────────────
    # Fluxo de dependências
    # ─────────────────────────────────────────────
    task_1 >> task_2 >> task_3 >> [task_4, task_5]
    task_5 >> task_6 >> [task_7, task_8]
    [task_7, task_8] >> task_9 >> task_10
