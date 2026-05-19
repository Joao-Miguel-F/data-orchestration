"""
Camada de acesso ao MongoDB.

Colecoes:
  - contratacoes_raw : dados brutos vindos da API

Upsert garante que rodar o pipeline duas vezes nao duplica registros.
"""

from pymongo import MongoClient, ASCENDING
from src.config import MONGO_URI, MONGO_DB, COLECAO_RAW


def _get_db():
    client = MongoClient(MONGO_URI, tlsInsecure=True)
    return client[MONGO_DB]


def inserir_raw(registros: list[dict]) -> dict:
    """Salva registros brutos da API. Atualiza se o numeroControlePNCP ja existe."""
    colecao = _get_db()[COLECAO_RAW]
    colecao.create_index([("numeroControlePNCP", ASCENDING)], unique=True)

    inseridos = 0
    atualizados = 0
    for reg in registros:
        resultado = colecao.update_one(
            {"numeroControlePNCP": reg.get("numeroControlePNCP")},
            {"$set": reg},
            upsert=True,
        )
        if resultado.upserted_id:
            inseridos += 1
        elif resultado.modified_count:
            atualizados += 1

    print(f"MongoDB raw -> inseridos: {inseridos} | atualizados: {atualizados}")
    return {"inseridos": inseridos, "atualizados": atualizados}


def contar(colecao: str) -> int:
    return _get_db()[colecao].count_documents({})


def buscar_raw_por_periodo(data_inicial: str, data_final: str, uf: str) -> list[dict]:
    """Busca registros brutos no MongoDB filtrando por periodo e UF."""
    di = f"{data_inicial[:4]}-{data_inicial[4:6]}-{data_inicial[6:]}"
    df = f"{data_final[:4]}-{data_final[4:6]}-{data_final[6:]}"
    colecao = _get_db()[COLECAO_RAW]
    return list(colecao.find(
        {
            "dataPublicacaoPncp": {"$gte": di, "$lte": df + "T23:59:59"},
            "unidadeOrgao.ufSigla": uf.upper(),
        },
        {"_id": 0},
    ))


