import os

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

DEFAULT_PARAMS = {
    "codigoModalidadeContratacao": 8,   # 8 = dispensa de licitacao
    "tamanhoPagina": 20,
}

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://larissa:engbigdata%40@cluster-bi-data.fzpirih.mongodb.net/?appName=cluster-bi-data")
MONGO_DB  = os.getenv("MONGO_DB", "pncp")
