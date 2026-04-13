import pendulum
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import get_current_context
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import BaseOperator

# ──────────────────────────────────────────────
# Caminhos base
# ──────────────────────────────────────────────
DATA = "/opt/airflow/data"

# ──────────────────────────────────────────────
# Funções das tasks Python
# ──────────────────────────────────────────────

def tratar_datas():
    """TASK-2: normaliza datas para dd/mm/aaaa e salva task2.csv."""
    df = pd.read_csv(f"{DATA}/entrada.csv", sep=";")

    def normalizar(data):
        data = str(data).strip()
        # formato americano yyyy-mm-dd → dd/mm/aaaa
        if len(data) == 10 and data[4] == "-":
            partes = data.split("-")
            return f"{partes[2]}/{partes[1]}/{partes[0]}"
        return data

    df["data_execucao"] = df["data_execucao"].apply(normalizar)
    df.to_csv(f"{DATA}/task2.csv", sep=";", index=False)
    print(f"[TASK-2] Datas normalizadas. Total de registros: {len(df)}")


def remover_musicas_vazias():
    """TASK-3: remove linhas com nome_musica vazio e empurra qtd descartados via xcom."""
    df = pd.read_csv(f"{DATA}/task2.csv", sep=";")
    total_antes = len(df)

    df_limpo = df[df["nome_musica"].notna() & (df["nome_musica"].str.strip() != "")]
    descartados = total_antes - len(df_limpo)

    df_limpo.to_csv(f"{DATA}/task3.csv", sep=";", index=False)

    # Passa qtd de descartados para a task seguinte via xcom
    get_current_context()["ti"].xcom_push(key="descartados", value=descartados)
    print(f"[TASK-3] Registros antes: {total_antes} | Descartados: {descartados} | Restantes: {len(df_limpo)}")


def enriquecer_com_genero():
    """TASK-6: usa resultado do SELECT da TASK-5 para criar coluna nome_genero em task4.csv."""
    ti = get_current_context()["ti"]

    # Resultado do SQLExecuteQueryOperator é lista de tuplas [(id, nome), ...]
    resultado_sql = ti.xcom_pull(task_ids="task_5_select_genero")
    if not resultado_sql:
        raise ValueError("[TASK-6] Nenhum resultado recebido da TASK-5!")

    # Monta dicionário id_genero → nome_genero
    mapa_genero = {str(row[0]).strip(): str(row[1]).strip() for row in resultado_sql}
    print(f"[TASK-6] Mapa de gêneros recebido: {mapa_genero}")

    df = pd.read_csv(f"{DATA}/task3.csv", sep=";")
    df["id_genero"] = df["id_genero"].astype(str).str.strip().str.zfill(3)
    df["nome_genero"] = df["id_genero"].map(mapa_genero).fillna("DESCONHECIDO")

    df.to_csv(f"{DATA}/task4.csv", sep=";", index=False)
    print(f"[TASK-6] task4.csv gerado com {len(df)} registros.")


def media_avaliacao_por_musica():
    """TASK-7: calcula média de nota por música e salva media_avaliacao.csv."""
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
    print(f"[TASK-7] media_avaliacao.csv gerado com {len(resultado)} músicas.")


def total_musicas_por_artista():
    """TASK-8: calcula total de músicas ouvidas por artista e salva total_artista.csv."""
    df = pd.read_csv(f"{DATA}/task4.csv", sep=";")
    resultado = (
        df.groupby("nome_artista")
        .size()
        .reset_index(name="total_musicas")
        .sort_values("total_musicas", ascending=False)
    )
    resultado.to_csv(f"{DATA}/total_artista.csv", sep=";", index=False)
    print(f"[TASK-8] total_artista.csv gerado com {len(resultado)} artistas.")


# ──────────────────────────────────────────────
# Definição da DAG
# ──────────────────────────────────────────────
with DAG(
    dag_id="av02_stream_pipeline",
    description="AV-02 SSA — Pipeline de análise de músicas em streaming",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["av02", "stream", "airflow"],
) as dag:

    # ------------------------------------------------------------------
    # TASK-1: copia dados-stream.csv → entrada.csv
    # ------------------------------------------------------------------
    task_1_copiar = BashOperator(
        task_id="task_1_copiar_arquivo",
        bash_command=f"cp {DATA}/dados-stream.csv {DATA}/entrada.csv && echo '[TASK-1] Arquivo copiado.'",
    )

    # ------------------------------------------------------------------
    # TASK-2: normaliza datas → task2.csv
    # ------------------------------------------------------------------
    task_2_datas = PythonOperator(
        task_id="task_2_tratar_datas",
        python_callable=tratar_datas,
    )

    # ------------------------------------------------------------------
    # TASK-3: remove nome_musica vazio → task3.csv + xcom(descartados)
    # ------------------------------------------------------------------
    task_3_remover = PythonOperator(
        task_id="task_3_remover_vazios",
        python_callable=remover_musicas_vazias,
    )

    # ------------------------------------------------------------------
    # TASK-4: INSERT de descartados no banco
    # O valor é puxado via xcom da task_3 usando template Jinja
    # ------------------------------------------------------------------
    task_4_insert_descartados = SQLExecuteQueryOperator(
        task_id="task_4_insert_descartados",
        conn_id="postgres_default",
        sql="""
            INSERT INTO descartados (total)
            VALUES ({{ ti.xcom_pull(task_ids='task_3_remover_vazios', key='descartados') }});
        """,
    )

    # ------------------------------------------------------------------
    # TASK-5: SELECT na tabela genero_musical
    # Resultado fica no xcom automaticamente (lista de tuplas)
    # ------------------------------------------------------------------
    task_5_select_genero = SQLExecuteQueryOperator(
        task_id="task_5_select_genero",
        conn_id="postgres_default",
        sql="SELECT id_genero, nome_genero FROM genero_musical ORDER BY id_genero;",
    )

    # ------------------------------------------------------------------
    # TASK-6: enriquece task3.csv com nome_genero → task4.csv
    # ------------------------------------------------------------------
    task_6_enriquecer = PythonOperator(
        task_id="task_6_enriquecer_genero",
        python_callable=enriquecer_com_genero,
    )

    # ------------------------------------------------------------------
    # TASK-7: média de avaliação por música → media_avaliacao.csv
    # ------------------------------------------------------------------
    task_7_media = PythonOperator(
        task_id="task_7_media_avaliacao",
        python_callable=media_avaliacao_por_musica,
    )

    # ------------------------------------------------------------------
    # TASK-8: total de músicas por artista → total_artista.csv
    # (executa em paralelo com TASK-7)
    # ------------------------------------------------------------------
    task_8_total_artista = PythonOperator(
        task_id="task_8_total_artista",
        python_callable=total_musicas_por_artista,
    )

    # ------------------------------------------------------------------
    # TASK-9: remove entrada.csv independente do resultado das tasks anteriores
    # ------------------------------------------------------------------
    task_9_remover_entrada = BashOperator(
        task_id="task_9_remover_entrada",
        bash_command=f"rm -f {DATA}/entrada.csv && echo '[TASK-9] entrada.csv removido.'",
        trigger_rule=TriggerRule.ALL_DONE,  # executa mesmo se TASK-7 ou TASK-8 falharem
    )

    # ------------------------------------------------------------------
    # TASK-10: marca o fim do pipeline (sem operação)
    # ------------------------------------------------------------------
    task_10_fim = BashOperator(
        task_id="task_10_fim_processamento",
        bash_command="echo '[TASK-10] Pipeline finalizado.'",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────
    # Fluxo de dependências
    #
    #  task_1
    #    └─► task_2
    #          └─► task_3
    #                ├─► task_4  (INSERT descartados)
    #                └─► task_5  (SELECT genero_musical)
    #                      └─► task_6  (enriquece com genero)
    #                            ├─► task_7  (média avaliação)  ─┐
    #                            └─► task_8  (total artista)    ─┤
    #                                                             ▼
    #                                                          task_9  (rm entrada.csv)
    #                                                             └─► task_10 (fim)
    # ──────────────────────────────────────────────
    (
        task_1_copiar
        >> task_2_datas
        >> task_3_remover
        >> [task_4_insert_descartados, task_5_select_genero]
    )

    task_5_select_genero >> task_6_enriquecer >> [task_7_media, task_8_total_artista]

    [task_7_media, task_8_total_artista] >> task_9_remover_entrada >> task_10_fim
