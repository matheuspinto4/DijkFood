# ─────────────────────────────────────────────────────────────────────────────
# deploy_full.ps1 — Build, Push, Infra e Simulação completos
# ─────────────────────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"

function Step($msg) {
    Write-Host "`n======================================" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan
}

# ── 1. Build e Push — Worker ──────────────────────────────────────────────────
Step "1/8  Build: Worker"
docker build --no-cache -t wallita/dijkfood-worker:latest -f worker/Dockerfile worker/
if ($LASTEXITCODE -ne 0) { throw "Falha no build do Worker" }

Step "2/8  Push: Worker"
docker push wallita/dijkfood-worker:latest
if ($LASTEXITCODE -ne 0) { throw "Falha no push do Worker" }

# ── 2. Build e Push — API ─────────────────────────────────────────────────────
Step "3/8  Build: API"
docker build --no-cache -t wallita/dijkfood-api:latest -f API/Dockerfile.api API/
if ($LASTEXITCODE -ne 0) { throw "Falha no build da API" }

Step "4/8  Push: API"
docker push wallita/dijkfood-api:latest
if ($LASTEXITCODE -ne 0) { throw "Falha no push da API" }

# ── 4. Infraestrutura ─────────────────────────────────────────────────────────
Step "5/8  Terraform: destroy + apply"
Set-Location terraform
wsl terraform destroy -auto-approve
if ($LASTEXITCODE -ne 0) { Set-Location ..; throw "Falha no terraform destroy" }
wsl terraform apply -auto-approve
if ($LASTEXITCODE -ne 0) { Set-Location ..; throw "Falha no terraform apply" }
Set-Location ..

# ── 5. Schema + Populate + Simulador ─────────────────────────────────────────
Step "6/8  Schema + Populate"
wsl bash -ic "python run.py --step schema"
if ($LASTEXITCODE -ne 0) { throw "Falha no schema" }
wsl bash -ic "python run.py --step populate"
if ($LASTEXITCODE -ne 0) { throw "Falha no populate" }

Step "7/8  Redshift"
wsl bash -ic "python analytics/redshift.py"
if ($LASTEXITCODE -ne 0) { throw "Falha no setup analítico" }

Step "8/8  Simulator"
wsl bash -ic "python run.py --step simulator"