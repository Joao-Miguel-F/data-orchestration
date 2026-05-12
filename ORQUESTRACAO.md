# Pipeline PNCP

Pipeline de coleta, transformação e armazenamento de dados de contratações públicas da [API do PNCP](https://pncp.gov.br), orquestrado com Prefect e processado com Apache Spark.

## Tecnologias

- **Prefect** — orquestração e monitoramento das etapas
- **Apache Spark (PySpark)** — transformação e classificação dos dados
- **MongoDB Atlas** — persistência dos dados brutos e processados
- **Python 3.13**

## Pré-requisitos

- Python 3.13+
- Java JDK 21 instalado
- Conexão com internet (API PNCP + MongoDB Atlas)

## Configuração

Execute uma única vez para criar o ambiente virtual e instalar as dependências:

```powershell
.\setup_venv.ps1
```

## Como rodar

```powershell
.\rodar.ps1
```

Com datas e parâmetros personalizados:

```powershell
.\rodar.ps1 -DataInicial 20260101 -DataFinal 20260131 -UF SP -Modalidade 6
```

Ou diretamente pelo Python:

```powershell
python main.py --data-inicial 20260401 --data-final 20260430 --uf PE --modalidade 8
```

### Parâmetros disponíveis

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--data-inicial` | obrigatório | Formato AAAAMMDD |
| `--data-final` | obrigatório | Formato AAAAMMDD |
| `--uf` | `PE` | Sigla do estado |
| `--modalidade` | `8` | Código da modalidade (8 = Dispensa) |
| `--tamanho-pagina` | `20` | Registros por página (máx. 500) |
| `--sem-resumo` | — | Suprime o resumo estatístico no console |

## Fluxo do pipeline

```
API PNCP → MongoDB (raw) → Spark (flatten + classifica ramo MEI + deduplica) → MongoDB (processados)
```

1. **Coleta** — percorre todas as páginas da API com delay de 1.5s entre requisições
2. **Processamento** — Spark seleciona os campos relevantes e classifica cada contratação em um ramo do MEI
3. **Persistência** — dados brutos e processados são salvos no MongoDB Atlas com upsert por `numeroControlePNCP`

## Resiliência

- Erros **5xx** do servidor PNCP (instabilidade de banco): retry automático com backoff (10s, 20s)
- Erros **de rede**: retry com backoff (5s, 10s)
- A task de coleta tem até **3 retries** pelo Prefect

## Estrutura

```
├── main.py              # entrypoint CLI
├── rodar.ps1            # script de execução completa (Windows)
├── setup_venv.ps1       # configuração inicial do ambiente
├── requirements.txt
└── src/
    ├── config.py        # URLs, parâmetros e constantes
    ├── ingestion.py     # coleta paginada da API PNCP
    ├── processing.py    # transformações Spark
    ├── database.py      # acesso ao MongoDB
    └── orchestration.py # tasks e flow Prefect
```

## Coleções MongoDB

| Coleção | Conteúdo |
|---|---|
| `contratacoes_raw` | Dados brutos da API, sem transformação |
| `contratacoes_processadas` | Dados com `ramo_mei`, campos selecionados e deduplicados |

Ambas usam upsert por `numeroControlePNCP`, então rodar o pipeline duas vezes não duplica registros.
