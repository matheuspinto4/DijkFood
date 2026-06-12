# ─────────────────────────────────────────────────────────────────────────────
# deploy_full.ps1 — Build, Push, Infra e Simulação completos
# Execute da raiz do projeto: .\deploy_full.ps1
# ─────────────────────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"

function Step($msg) {
    Write-Host "`n======================================" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan
}

# ── 1. Build e Push — Worker ──────────────────────────────────────────────────
Step "1/7  Build: Worker"
docker build -t wallita/dijkfood-worker:latest -f worker/Dockerfile worker/
if ($LASTEXITCODE -ne 0) { throw "Falha no build do Worker" }

Step "2/7  Push: Worker"
docker push wallita/dijkfood-worker:latest
if ($LASTEXITCODE -ne 0) { throw "Falha no push do Worker" }

# ── 2. Build e Push — API ─────────────────────────────────────────────────────
Step "3/7  Build: API"
docker build -t wallita/dijkfood-api:latest -f API/Dockerfile.api API/
if ($LASTEXITCODE -ne 0) { throw "Falha no build da API" }

Step "4/7  Push: API"
docker push wallita/dijkfood-api:latest
if ($LASTEXITCODE -ne 0) { throw "Falha no push da API" }

# ── 3. Infraestrutura ─────────────────────────────────────────────────────────
Step "5/7  Terraform: destroy + apply"
Set-Location terraform
wsl terraform destroy -auto-approve
if ($LASTEXITCODE -ne 0) { Set-Location ..; throw "Falha no terraform destroy" }
wsl terraform apply -auto-approve
if ($LASTEXITCODE -ne 0) { Set-Location ..; throw "Falha no terraform apply" }
Set-Location ..

# ── 4. Schema + Populate + Simulador ─────────────────────────────────────────
Step "6/7  Schema + Populate"
python run.py --step schema
if ($LASTEXITCODE -ne 0) { throw "Falha no schema" }
python run.py --step populate
if ($LASTEXITCODE -ne 0) { throw "Falha no populate" }

Step "7/7  Simulador"
python run.py --step simulator
