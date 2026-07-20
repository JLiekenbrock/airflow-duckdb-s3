[CmdletBinding()]
param(
    [string]$Kubeconfig = $env:KUBECONFIG,
    [switch]$PublishImage,
    [switch]$SkipOpenShiftCheck
)

$ErrorActionPreference = 'Stop'
$projectDir = $PSScriptRoot
$envFile = Join-Path $projectDir '.env'

function Find-Docker {
    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }

    $perUserDocker = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $perUserDocker) { return $perUserDocker }

    $systemDocker = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
    if (Test-Path -LiteralPath $systemDocker) { return $systemDocker }

    throw 'Docker CLI was not found. Install and start Docker Desktop first.'
}

function Read-DotEnvValue([string]$Name) {
    $line = Get-Content -LiteralPath $envFile |
        Where-Object { $_ -match "^$([regex]::Escape($Name))=" } |
        Select-Object -Last 1
    if (-not $line) { return $null }
    return ($line -split '=', 2)[1].Trim()
}

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Missing $envFile. Copy .env.example to .env and configure it first."
}

if (-not $Kubeconfig) { $Kubeconfig = Read-DotEnvValue 'KUBECONFIG' }
if (-not $Kubeconfig) { $Kubeconfig = Join-Path $env:USERPROFILE '.kube\config' }
if (-not (Test-Path -LiteralPath $Kubeconfig)) {
    throw "Kubeconfig not found: $Kubeconfig. Run 'oc login' or pass -Kubeconfig."
}

$Kubeconfig = (Resolve-Path -LiteralPath $Kubeconfig).Path
$env:KUBECONFIG = $Kubeconfig
$docker = Find-Docker
& $docker version | Out-Null

$pipelineImage = Read-DotEnvValue 'PIPELINE_IMAGE'
if (-not $pipelineImage -or $pipelineImage -match 'registry\.example\.com') {
    throw 'Set PIPELINE_IMAGE in .env to an image registry reachable by OpenShift.'
}

if (-not $SkipOpenShiftCheck) {
    $oc = Get-Command oc -ErrorAction SilentlyContinue
    if (-not $oc) {
        throw "The 'oc' CLI was not found. Install it, or use -SkipOpenShiftCheck for only the local UI."
    }
    & $oc.Source whoami | Out-Null
    $namespace = Read-DotEnvValue 'OPENSHIFT_NAMESPACE'
    if (-not $namespace) { $namespace = 'data-jobs' }
    & $oc.Source get namespace $namespace | Out-Null
    Write-Host "OpenShift connection OK: $(& $oc.Source whoami --show-server) / $namespace"
}

if ($PublishImage) {
    Write-Host "Building pipeline image $pipelineImage"
    & $docker build --file (Join-Path $projectDir 'Dockerfile') --tag $pipelineImage $projectDir
    if ($LASTEXITCODE -ne 0) { throw 'Pipeline image build failed.' }
    & $docker push $pipelineImage
    if ($LASTEXITCODE -ne 0) { throw 'Pipeline image push failed.' }
}

Push-Location $projectDir
try {
    & $docker compose up --build --detach
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose startup failed.' }

    $airflowPort = Read-DotEnvValue 'AIRFLOW_PORT'
    if (-not $airflowPort) { $airflowPort = '8082' }
    $healthUrl = "http://localhost:$airflowPort/health"
    $ready = $false
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        try {
            $response = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 2
            if ($response.metadatabase.status -eq 'healthy' -and
                $response.scheduler.status -eq 'healthy') {
                $ready = $true
                break
            }
        } catch {
            # Airflow may still be initializing.
        }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) {
        & $docker compose logs --tail 100 airflow
        throw "Airflow did not become healthy at $healthUrl."
    }

    Write-Host "Airflow is ready: http://localhost:$airflowPort"
    Write-Host "DAG: http://localhost:$airflowPort/dags/duckdb_s3_openshift/grid"
    Write-Host 'Username: admin'
    & $docker compose logs airflow 2>&1 |
        Select-String -Pattern 'Login with username: admin' |
        Select-Object -Last 1
} finally {
    Pop-Location
}

