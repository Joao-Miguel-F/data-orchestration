# Pipeline de Orquestração PNCP

Pipeline de coleta, transformação e armazenamento de dados de contratações públicas da [API do PNCP](https://pncp.gov.br), orquestrado via **Apache Airflow** com processamento distribuído via **Apache Spark**.

## Tecnologias

- **Python 3.12**
- **Apache Airflow 2.9** — orquestração e agendamento do pipeline
- **Apache Spark (PySpark 3.5)** — transformação e agregação dos dados
- **MongoDB Atlas** — persistência nas camadas Bronze, Silver e Gold
- **Docker** — ambiente de execução isolado e reproduzível

## Arquitetura

```
API PNCP → Bronze (raw) → Silver (processado) → Gold (agregado)
```

| Camada | Coleção MongoDB | Descrição |
|---|---|---|
| Bronze | `contratacoes_raw` | Dados brutos da API, sem transformação |
| Silver | `contratacoes_processadas` | Campos selecionados, padronizados e classificados |
| Gold | `gold_area_de_servico`, `gold_estado`, `gold_faixa_de_valor`, `gold_situacao`, `gold_por_mes` | Agregações analíticas prontas para consumo |

## Fluxo do pipeline

```
ingest_bronze → transform_silver → build_gold
```

1. **ingest_bronze** — coleta paginada da API do PNCP com retry automático, salva no MongoDB raw
2. **transform_silver** — lê do Bronze, aplica flatten, classifica por ramo MEI, deduplica e salva na Silver
3. **build_gold** — lê da Silver, executa 5 agregações por estado e salva nas coleções Gold

## Classificação por ramo MEI

Cada contratação é classificada automaticamente com base no `objeto_compra`:

| Ramo | Palavras-chave detectadas |
|---|---|
| Obras | obra, construção, reforma, pavimento... |
| TI | software, sistema, tecnologia, hardware... |
| Serviços | serviço, limpeza, vigilância, manutenção... |
| Compras | aquisição, fornecimento, material, equipamento... |
| Outros | demais casos |

## Estrutura do projeto

```
├── dags/
│   └── pncp_pipeline.py     # DAG do Airflow (3 tasks)
├── src/
│   ├── config.py            # URLs, parâmetros e variáveis de ambiente
│   ├── ingestion.py         # coleta paginada da API PNCP
│   ├── processing.py        # transformações e agregações Spark
│   └── loading.py           # acesso ao MongoDB Atlas
├── Dockerfile               # imagem Airflow + Java + dependências
├── docker-compose.yaml      # webserver, scheduler e postgres
├── requirements.txt
└── .env                     # credenciais (não versionado)
```

## Pré-requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

## Configuração

Crie um arquivo `.env` na raiz do projeto:

```env
MONGO_URI=mongodb+srv://<usuario>:<senha>@<cluster>.mongodb.net/?appName=<app>
MONGO_DB=pncp
```

## Como rodar

```bash
# Primeira vez — inicializa o banco do Airflow e cria o usuário admin
docker-compose up airflow-init

# Sobe o ambiente completo
docker-compose up
```

Acesse o dashboard em **http://localhost:8080** com `admin` / `admin`.

O DAG `pncp_pipeline` roda automaticamente todos os dias. Para disparar manualmente:

```bash
docker-compose exec scheduler airflow dags trigger pncp_pipeline
```

# 🏛️ PNCP Chatbot — MCP + Streamlit

Chatbot integrado ao pipeline ETL do PNCP que permite fazer perguntas em linguagem natural sobre os dados de contratações públicas por dispensa de licitação.

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│                    chatbot_app.py                        │
│                  (Interface Streamlit)                   │
│                                                         │
│  Usuário → Pergunta → Ollama LLM → Tool Calls           │
│                              ↓                          │
│              Chama mcp_server.py via stdio               │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│                    mcp_server.py                         │
│               (Servidor MCP — stdio)                     │
│                                                         │
│  Ferramentas expostas:                                  │
│  • contar_registros        • resumo_por_estado          │
│  • resumo_por_ramo_mei     • resumo_por_faixa_valor     │
│  • resumo_por_situacao     • evolucao_mensal            │
│  • buscar_contratacoes     • top_orgaos_contratantes    │
│  • estatisticas_gerais                                  │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│               MongoDB Atlas (PNCP)                       │
│                                                         │
│  Bronze: contratacoes_raw                               │
│  Silver: contratacoes_processadas                       │
│  Gold:   gold_estado, gold_area_de_servico,             │
│          gold_faixa_de_valor, gold_situacao,            │
│          gold_por_mes                                   │
└─────────────────────────────────────────────────────────┘
```

## 🚀 Instalação

### 1. Pré-requisitos

- Python 3.11+
- [Ollama](https://ollama.com) instalado

### 2. Instalar dependências do chatbot

```bash
# Na raiz do projeto data-orchestration
pip install mcp streamlit ollama
```

Ou adicione ao `requirements.txt` existente:
```
mcp>=1.0.0
streamlit>=1.35.0
ollama>=0.3.0
```

### 3. Instalar e configurar o Ollama

```bash
# Linux / macOS
curl -fsSL https://ollama.com/install.sh | sh

# Windows: baixe em https://ollama.com/download
```

Iniciar o servidor Ollama (deixe rodando em segundo plano):
```bash
ollama serve
```

Baixar o modelo (escolha UM — llama3.2 é recomendado por ser leve e bom):
```bash
# Opção 1: llama3.2 (2GB — recomendado, rápido e eficiente)
ollama pull llama3.2

# Opção 2: llama3.1 (4GB — mais capaz)
ollama pull llama3.1

# Opção 3: qwen2.5 (4GB — excelente em seguir instruções)
ollama pull qwen2.5

# Opção 4: mistral (4GB — alternativa sólida)
ollama pull mistral

# Opção 5: gemma3 (5GB — capacidade avançada de raciocínio)
ollama pull gemma3
```

### 4. Copiar os arquivos

Coloque `mcp_server.py` e `chatbot_app.py` na **raiz** do projeto `data-orchestration`:

```
data-orchestration/
├── dags/
├── src/
├── mcp_server.py       ← NOVO
├── chatbot_app.py      ← NOVO
├── requirements.txt
├── Dockerfile
└── docker-compose.yaml
```

### 5. Iniciar o chatbot

```bash
# Na raiz do projeto
streamlit run chatbot_app.py
```

Acesse: **http://localhost:8501**

---

## 🔧 Variáveis de ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `MONGO_URI` | URI do Atlas | Connection string do MongoDB |
| `MONGO_DB` | `pncp` | Nome do banco de dados |
| `OLLAMA_MODEL` | `llama3.2` | Modelo Ollama a usar |

Para usar variáveis customizadas:
```bash
OLLAMA_MODEL=qwen2.5 streamlit run chatbot_app.py
```

---

## 💬 Exemplos de perguntas

```
📊 Qual é o estado com mais contratações?
💰 Qual o valor total de contratações em SP?
🔧 Qual ramo MEI tem maior valor acumulado?
🏛️ Quais são os 5 órgãos que mais contratam em PE?
📈 Como evoluíram as contratações mês a mês em 2026?
📋 Quantos registros existem em cada camada do pipeline?
🔍 Busque contratações de limpeza publicadas em janeiro de 2026
📌 Quais são as situações mais comuns das contratações?
💡 Quais são as contratações de TI acima de R$80.000?
🗺️ Compare os estados do Nordeste em total de contratações
```

---

## 🛠️ Ferramentas MCP expostas

| Ferramenta | Descrição |
|-----------|-----------|
| `contar_registros` | Total de documentos em qualquer coleção |
| `resumo_por_estado` | Contratações agrupadas por UF |
| `resumo_por_ramo_mei` | Distribuição por categoria (Obras/TI/Serviços/Compras/Outros) |
| `resumo_por_faixa_valor` | Distribuição por faixa de valor monetário |
| `resumo_por_situacao` | Distribuição por situação da contratação |
| `evolucao_mensal` | Série temporal mensal por estado |
| `buscar_contratacoes` | Busca com filtros (UF, data, texto, ramo) |
| `top_orgaos_contratantes` | Ranking de órgãos por volume de contratações |
| `estatisticas_gerais` | Painel geral com totais de todas as camadas |

---

## 🐛 Solução de problemas

**"Ollama offline"**
```bash
# Certifique-se de que o Ollama está rodando
ollama serve
```

**"Modelo não encontrado"**
```bash
# Baixe o modelo configurado (padrão: llama3.2)
ollama pull llama3.2
# Ou liste os disponíveis:
ollama list
```

**"MCP Server erro"**
```bash
# Teste o servidor MCP isoladamente
python mcp_server.py
# Deve ficar aguardando input — Ctrl+C para sair
```

**"Sem dados nas consultas"**
- O pipeline Airflow precisa ter rodado ao menos uma vez para popular o MongoDB.
- Verifique via DAG `pncp_pipeline` no Airflow em `http://localhost:8080`.

---

## 📚 Tecnologias utilizadas

| Tecnologia | Uso | Licença |
|-----------|-----|---------|
| [Streamlit](https://streamlit.io) | Interface web | Apache 2.0 |
| [MCP (Model Context Protocol)](https://modelcontextprotocol.io) | Protocolo LLM ↔ Ferramentas | MIT |
| [Ollama](https://ollama.com) | LLM local open-source | MIT |
| [Llama 3.2](https://llama.meta.com) | Modelo de linguagem | Llama 3.2 Community License |
| [MongoDB](https://mongodb.com) | Banco de dados | SSPL |
| [Apache Airflow](https://airflow.apache.org) | Orquestração ETL | Apache 2.0 |
| [PySpark](https://spark.apache.org) | Processamento distribuído | Apache 2.0 |

**100% open-source e gratuito** — sem necessidade de chaves de API pagas.
