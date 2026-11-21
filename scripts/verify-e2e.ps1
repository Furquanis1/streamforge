$ErrorActionPreference = "Stop"

$edgeUrl = "http://localhost:8083"
$fogUrl = "http://localhost:8082"
$cloudUrl = "http://localhost:8081"

function Invoke-JsonGet {
    param([string]$Url)
    return Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 5
}

function Wait-Healthy {
    Write-Host "Waiting for Edge, Fog, and Cloud APIs..."
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            $edge = Invoke-JsonGet "$edgeUrl/api/edge/stats"
            $fog = Invoke-JsonGet "$fogUrl/api/fog/stats"
            $cloud = Invoke-JsonGet "$cloudUrl/api/cloud/stats"
            if ($null -ne $edge -and $null -ne $fog -and $null -ne $cloud) {
                Write-Host "All tiers healthy."
                return
            }
        } catch {}
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for the StreamForge APIs."
}

Wait-Healthy

$edge = Invoke-RestMethod -Uri "$edgeUrl/api/edge/control" -Method Post `
    -ContentType "application/json" -Body '{"anomalyBurst":true,"anomalyBurstCount":50}'
Start-Sleep -Seconds 3
$fog = Invoke-JsonGet "$fogUrl/api/fog/stats"
Write-Host ("Edge reduction: {0:N2}% | Anomaly overrides: {1}" -f $edge.reductionPercent, $edge.anomaliesDetected)
Write-Host ("Fog compression ratio: {0:N3}x | Codec: {1}" -f $fog.cumulativeCompressionRatio, $fog.activeEncodingCompression)

$outageResponse = Invoke-RestMethod -Uri "$cloudUrl/api/cloud/chaos" -Method Post `
    -ContentType "application/json" -Body '{"failMode":true,"latencyMs":0}'
if ($outageResponse.failMode -ne $true) {
    throw "Cloud outage mode did not enable."
}

Write-Host "Cloud outage enabled. Waiting 8 seconds for circuit and WAL failover..."
Start-Sleep -Seconds 8
$fogOutage = Invoke-JsonGet "$fogUrl/api/fog/stats"
$state = [string]$fogOutage.circuitBreakerState
$walSegments = [long]$fogOutage.pendingWalSegmentCount
$walBytes = [long]$fogOutage.pendingWalBytes
Write-Host "Circuit breaker: $state | WAL: $walSegments segments / $walBytes bytes"

if ($state -ne "OPEN") {
    throw "Resilience4j circuit breaker did not open; observed '$state'."
}
if ($walSegments -le 0 -or $walBytes -le 0) {
    throw "Disk WAL did not accumulate buffered data."
}

$cloudBeforeRecovery = Invoke-JsonGet "$cloudUrl/api/cloud/stats"
$batchesBeforeRecovery = [long]$cloudBeforeRecovery.batchesIngested

Invoke-RestMethod -Uri "$cloudUrl/api/cloud/chaos" -Method Post `
    -ContentType "application/json" -Body '{"failMode":false,"latencyMs":0}' | Out-Null

Write-Host "Cloud outage disabled. Waiting 8 seconds for WAL replay..."
Start-Sleep -Seconds 8
$fogRecovered = Invoke-JsonGet "$fogUrl/api/fog/stats"
$cloudRecovered = Invoke-JsonGet "$cloudUrl/api/cloud/stats"
$recoveredSegments = [long]$fogRecovered.pendingWalSegmentCount
$recoveredBytes = [long]$fogRecovered.pendingWalBytes
$batchesRecovered = [long]$cloudRecovered.batchesIngested
$checksumErrors = [long]$cloudRecovered.checksumErrors

Write-Host "Circuit breaker: $($fogRecovered.circuitBreakerState) | WAL: $recoveredSegments segments / $recoveredBytes bytes"
Write-Host "Persisted batches: $batchesRecovered | CRC32 errors: $checksumErrors"

if ($recoveredSegments -ne 0 -or $recoveredBytes -ne 0) {
    throw "Disk WAL did not drain after cloud recovery."
}
if ($batchesRecovered -lt $batchesBeforeRecovery) {
    throw "Cloud persisted batch count decreased during recovery."
}
if ($checksumErrors -ne 0) {
    throw "CRC32 integrity verification reported checksum failures."
}

Write-Host "E2E chaos verification PASSED."
