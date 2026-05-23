import certifi
from pymongo import MongoClient, ASCENDING
from src.config import MONGO_URI, MONGO_DB

_client: MongoClient | None = None


def _get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return _client[MONGO_DB]


def inserir_raw(registros: list[dict]) -> dict:
    colecao = _get_db()["contratacoes_raw"]
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


def inserir_processados(registros: list[dict]) -> dict:
    colecao = _get_db()["contratacoes_processadas"]
    colecao.create_index([("numero_controle_pncp", ASCENDING)], unique=True)

    inseridos = 0
    atualizados = 0
    for reg in registros:
        reg = {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in reg.items()}
        resultado = colecao.update_one(
            {"numero_controle_pncp": reg.get("numero_controle_pncp")},
            {"$set": reg},
            upsert=True,
        )
        if resultado.upserted_id:
            inseridos += 1
        elif resultado.modified_count:
            atualizados += 1

    print(f"MongoDB processados -> inseridos: {inseridos} | atualizados: {atualizados}")
    return {"inseridos": inseridos, "atualizados": atualizados}


def contar(colecao: str) -> int:
    return _get_db()[colecao].count_documents({})


def _fmt(data: str) -> str:
    return f"{data[:4]}-{data[4:6]}-{data[6:]}"


def buscar_raw_por_periodo(data_inicial: str, data_final: str, uf: str | None = None) -> list[dict]:
    filtro = {"dataPublicacaoPncp": {"$gte": _fmt(data_inicial), "$lte": _fmt(data_final) + "T23:59:59"}}
    if uf:
        filtro["unidadeOrgao.ufSigla"] = uf.upper()
    return list(_get_db()["contratacoes_raw"].find(filtro, {"_id": 0}))


def buscar_processados_por_periodo(data_inicial: str, data_final: str) -> list[dict]:
    filtro = {"data_publicacao_pncp": {"$gte": _fmt(data_inicial), "$lte": _fmt(data_final) + "T23:59:59"}}
    return list(_get_db()["contratacoes_processadas"].find(filtro, {"_id": 0}))


def salvar_gold(colecao: str, registros: list[dict], chave: list[str]) -> dict:
    col = _get_db()[colecao]
    col.create_index([(campo, ASCENDING) for campo in chave], unique=True)

    inseridos = 0
    atualizados = 0
    for reg in registros:
        filtro = {k: reg[k] for k in chave}
        resultado = col.update_one(filtro, {"$set": reg}, upsert=True)
        if resultado.upserted_id:
            inseridos += 1
        elif resultado.modified_count:
            atualizados += 1

    print(f"MongoDB {colecao} -> inseridos: {inseridos} | atualizados: {atualizados}")
    return {"inseridos": inseridos, "atualizados": atualizados}
