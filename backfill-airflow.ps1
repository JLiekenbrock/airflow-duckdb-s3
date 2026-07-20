[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidatePattern('^[A-Za-z0-9_-]{1,64}$')]
    [string]$Mandant,

    [Parameter(Mandatory)]
    [datetime]$From,

    [Parameter(Mandatory)]
    [datetime]$To,

    [ValidateSet('Daily', 'MonthEnd')]
    [string]$Frequency = 'Daily',

    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$projectDir = $PSScriptRoot

if ($From.Date -gt $To.Date) {
    throw '-From must be on or before -To.'
}

$dates = [System.Collections.Generic.List[string]]::new()
if ($Frequency -eq 'Daily') {
    for ($date = $From.Date; $date -le $To.Date; $date = $date.AddDays(1)) {
        $dates.Add($date.ToString('yyyy-MM-dd'))
    }
} else {
    $month = [datetime]::new($From.Year, $From.Month, 1)
    $lastMonth = [datetime]::new($To.Year, $To.Month, 1)
    while ($month -le $lastMonth) {
        $monthEnd = $month.AddMonths(1).AddDays(-1)
        if ($monthEnd -ge $From.Date -and $monthEnd -le $To.Date) {
            $dates.Add($monthEnd.ToString('yyyy-MM-dd'))
        }
        $month = $month.AddMonths(1)
    }
}

if ($dates.Count -eq 0) {
    throw 'The selected range produced no stichtage.'
}
if ($dates.Count -gt 366) {
    throw "The DAG accepts at most 366 stichtage; this range produced $($dates.Count)."
}

$conf = @{
    mandant = $Mandant
    stichtage = @($dates)
} | ConvertTo-Json -Compress

Write-Host "Mandant: $Mandant"
Write-Host "Stichtage: $($dates.Count) ($($dates[0]) through $($dates[$dates.Count - 1]))"
Write-Host "DAG configuration: $conf"
if ($DryRun) { return }

$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCommand) {
    $docker = $dockerCommand.Source
} else {
    $docker = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
    if (-not (Test-Path -LiteralPath $docker)) {
        throw 'Docker CLI was not found. Start the local Airflow environment first.'
    }
}

Push-Location $projectDir
try {
    & $docker compose exec -T airflow airflow dags trigger --conf $conf duckdb_s3_openshift
    if ($LASTEXITCODE -ne 0) { throw 'Airflow backfill trigger failed.' }
} finally {
    Pop-Location
}
