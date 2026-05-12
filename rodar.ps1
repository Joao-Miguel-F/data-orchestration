# Pipeline PNCP - execucao completa
# Uso: .\rodar.ps1
# Uso com datas: .\rodar.ps1 -DataInicial 20260401 -DataFinal 20260430

param(
    [string]$DataInicial = "20260401",
    [string]$DataFinal   = "20260430",
    [string]$UF          = "PE",
    [int]$Modalidade     = 8
)

# --- configuracao do ambiente ---
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"

# O PySpark falha em caminhos com acentos (ex: "Area de Trabalho").
# Mapeamos uma letra de unidade temporaria sem caracteres especiais.
$ProjetoReal = Split-Path -Parent $MyInvocation.MyCommand.Path
$LetraTemp   = "P:"
subst $LetraTemp "$ProjetoReal" | Out-Null
$env:PYSPARK_PYTHON         = "$LetraTemp\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON  = "$LetraTemp\venv\Scripts\python.exe"
$env:SPARK_HOME             = "$LetraTemp\venv\Lib\site-packages\pyspark"
$env:SPARK_LOCAL_IP         = "127.0.0.1"
$env:HADOOP_HOME            = "$LetraTemp\venv\Lib\site-packages\pyspark"

& "$LetraTemp\venv\Scripts\Activate.ps1"

# --- painel Prefect em background (so abre se nao estiver rodando) ---
$prefectRodando = Get-NetTCPConnection -LocalPort 4200 -ErrorAction SilentlyContinue
if ($prefectRodando) {
    Write-Host "Painel Prefect ja esta rodando em http://localhost:4200" -ForegroundColor Yellow
} else {
    Write-Host "Iniciando painel Prefect em http://localhost:4200 ..." -ForegroundColor Cyan
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "subst $LetraTemp '$ProjetoReal'; & '$LetraTemp\venv\Scripts\Activate.ps1'; prefect server start"
    Start-Sleep -Seconds 5
}

# --- rodar pipeline a partir do caminho sem acentos ---
Write-Host "Rodando pipeline PNCP..." -ForegroundColor Cyan
Push-Location "$LetraTemp\"
try {
    python main.py --data-inicial $DataInicial --data-final $DataFinal --uf $UF --modalidade $Modalidade
} finally {
    Pop-Location
    subst "$LetraTemp" /D | Out-Null
}

Write-Host "Pronto! Veja os resultados em data/processed/ e no MongoDB Atlas." -ForegroundColor Green
