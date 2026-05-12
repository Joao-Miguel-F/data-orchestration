# Script de configuracao do ambiente virtual
Write-Host "Criando ambiente virtual..." -ForegroundColor Cyan
python -m venv venv

Write-Host "Ativando venv e instalando dependencias..." -ForegroundColor Cyan
& .\venv\Scripts\Activate.ps1
pip install -r requirements.txt

Write-Host "Ambiente pronto! Execute: python main.py --help" -ForegroundColor Green
