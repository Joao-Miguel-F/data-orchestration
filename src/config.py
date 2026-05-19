BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

# parametros de coleta
DEFAULT_PARAMS = {
    "codigoModalidadeContratacao": 8,   # 8 = dispensa de licitacao
    "uf": "PE",
    "tamanhoPagina": 20,
}

# modalidades de contratação
MODALIDADES = {
    1: "Leilao - Lei 14.133/2021",
    2: "Dialogo Competitivo",
    3: "Concurso",
    4: "Concorrencia - Lei 14.133/2021",
    5: "Credenciamento",
    6: "Pregao Eletronico",
    7: "Pregao Presencial",
    8: "Dispensa de Licitacao",
    9: "Inexigibilidade",
    10: "Manifestacao de Interesse",
    11: "Pre-qualificacao",
    12: "Credenciamento",
    13: "Leilao - Lei 8.666/1993",
}

# categoria do processo
CATEGORIAS_PROCESSO = {
    1: "Alienacoes",
    2: "Compras",
    3: "Informatica (TIC)",
    4: "Internacional",
    5: "Locacoes",
    6: "Manutencao e Reparos",
    7: "Obras",
    8: "Servicos",
    9: "Servicos de Engenharia",
    10: "Servicos de Saude",
}

# tipos de documento
TIPOS_DOCUMENTO = {
    2: "Edital",
    4: "Termo de Referencia",
    7: "Estudo Tecnico Preliminar",
    12: "Contrato",
}

# mongo
MONGO_URI = "mongodb+srv://larissa:engbigdata%40@cluster-bi-data.fzpirih.mongodb.net/?appName=cluster-bi-data"
MONGO_DB  = "pncp"

COLECAO_RAW = "contratacoes_raw"

# saida CSV da camada silver
OUTPUT_DIR = "data/silver"
