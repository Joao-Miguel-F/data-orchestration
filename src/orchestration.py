"""
Pipeline PNCP orquestrado com Prefect.

Fluxo:
  API PNCP
    -> MongoDB raw   (task_coleta)
    -> Spark silver  (task_silver)  flatten + ramo_mei + dedup -> CSV

Resiliencia na coleta:
  - Erros 5xx: retry com backoff (10s, 20s)
  - Erros 4xx: falha imediata
  - Erros de rede: retry com backoff (5s, 10s)
"""

import os
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger

from src.ingestion import fetch_all_pages
from src.database import inserir_raw, contar, buscar_raw_por_periodo
from src.processing import (
    create_spark_session,
    load_from_records,
    flatten_and_select,
    classify_ramo_mei,
    deduplicate,
    add_data_coleta,
    save_as_csv,
)
from src.config import OUTPUT_DIR


@task(name="Coleta API PNCP", retries=3, retry_delay_seconds=10, log_prints=True)
def task_coleta(data_inicial: str, data_final: str, modalidade: int, uf: str, tamanho: int, max_paginas: int | None = None) -> None:
    """Busca todos os registros da API e persiste no MongoDB raw."""
    logger = get_run_logger()
    logger.info(f"Iniciando coleta | {data_inicial} -> {data_final} | UF: {uf} | modalidade: {modalidade}")

    registros = fetch_all_pages(
        data_inicial=data_inicial,
        data_final=data_final,
        modalidade=modalidade,
        uf=uf,
        tamanho=tamanho,
        max_paginas=max_paginas,
    )

    if not registros:
        raise ValueError("Nenhum registro retornado pela API.")

    logger.info(f"{len(registros)} registros coletados.")
    inserir_raw(registros)
    logger.info(f"MongoDB raw: {contar('contratacoes_raw')} documentos no total.")


@task(name="Processamento Silver", log_prints=True, cache_policy=NO_CACHE)
def task_silver(data_inicial: str, data_final: str, uf: str) -> None:
    """Le do MongoDB raw, transforma com Spark e salva como CSV na camada silver."""
    logger = get_run_logger()

    registros = buscar_raw_por_periodo(data_inicial, data_final, uf)
    if not registros:
        raise ValueError("Nenhum registro encontrado no MongoDB raw para o periodo informado.")
    logger.info(f"Lidos {len(registros)} registros do MongoDB raw.")

    spark = create_spark_session()
    df_raw = load_from_records(spark, registros)

    df = (
        df_raw
        .transform(flatten_and_select)
        .transform(classify_ramo_mei)
        .transform(deduplicate)
        .transform(add_data_coleta)
    )

    csv_path = os.path.join(OUTPUT_DIR, f"silver_{uf}_{data_inicial}_{data_final}.csv")
    save_as_csv(df, csv_path)
    logger.info(f"CSV salvo: {csv_path}")


@flow(name="Pipeline PNCP", log_prints=True)
def run_pipeline(
    data_inicial: str,
    data_final: str,
    modalidade: int = 8,
    uf: str = "PE",
    tamanho_pagina: int = 20,
    max_paginas: int | None = None,
) -> None:
    """
    Flow principal — coordena coleta, silver e gold.

    Args:
        data_inicial: formato AAAAMMDD (ex: '20261201')
        data_final:   formato AAAAMMDD (ex: '20261231')
        modalidade:   codigo da modalidade (padrao 8 = Dispensa)
        uf:           sigla do estado (padrao 'PE')
        tamanho_pagina: registros por pagina da API (max 500)
        exibir_resumo: imprime estatisticas basicas no console
    """
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info(f"PIPELINE PNCP | {data_inicial} -> {data_final} | UF: {uf}")
    logger.info("=" * 50)

    task_coleta(data_inicial, data_final, modalidade, uf, tamanho_pagina, max_paginas)
    task_silver(data_inicial, data_final, uf)

    logger.info("Pipeline concluido.")
