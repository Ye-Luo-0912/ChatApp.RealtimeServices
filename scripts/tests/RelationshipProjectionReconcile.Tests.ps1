Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module `
    (Join-Path (Split-Path $PSScriptRoot -Parent) 'RelationshipProjectionReconcile.psm1') `
    -Force

function Assert-True {
    param(
        [Parameter(Mandatory)]
        [bool] $Condition,

        [Parameter(Mandatory)]
        [string] $Message
    )

    if (-not $Condition) {
        throw $Message
    }
}

function New-Response {
    param(
        [Parameter(Mandatory)]
        [int] $StatusCode,

        [Parameter(Mandatory)]
        [System.Collections.IDictionary] $Body
    )

    return [pscustomobject]@{
        StatusCode = $StatusCode
        Content = ConvertTo-Json -InputObject $Body -Compress -Depth 16
    }
}

function New-StableStatusResponse {
    param(
        [long] $PassNumber = 7
    )

    return New-Response 200 ([ordered]@{
        available = $true
        passNumber = $PassNumber
        stablePasses = 3
        passChanged = $false
        cursorOwnerUserId = $null
        cursorListType = $null
        leaseActive = $false
        lockedUntilMs = $null
        nextAttemptAtMs = 0
        lastError = $null
        versionStreamCount = 2
        snapshotBaselineStreamCount = 2
        streamsWithoutSnapshotBaselineCount = 0
        projectionItemCount = 2
        inboxEventCount = 2
        updatedAtMs = 10
        generatedAtMs = 11
    })
}

function New-CleanPageResponse {
    param(
        [Parameter(Mandatory)]
        [long] $OwnerUserId,

        [Parameter(Mandatory)]
        [bool] $HasMore
    )

    return New-Response 200 ([ordered]@{
        available = $true
        error = $null
        items = @([ordered]@{
            ownerUserId = $OwnerUserId
            listType = 1
            matches = $true
            issues = @()
            serverListedVersion = 1
            serverDigestVersion = 1
            serverItemCount = 1
            serverResourceHash = ('0' * 64)
            realtimeCurrentVersion = 1
            realtimeCurrentItemCount = 1
            realtimeSnapshotVersion = 1
            realtimeSnapshotItemCount = 1
            realtimeSnapshotResourceHash = ('0' * 64)
            realtimeLocallyContiguous = $true
        })
        matchedCount = 1
        mismatchCount = 0
        hasMore = $HasMore
        nextOwnerUserId = if ($HasMore) { $OwnerUserId } else { $null }
        nextListType = if ($HasMore) { 1 } else { $null }
        generatedAtMs = 12
    })
}

$temporaryRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
    'chatapp-reconcile-tests-' + [Guid]::NewGuid().ToString('N'))
[System.IO.Directory]::CreateDirectory($temporaryRoot) | Out-Null

try {
    $responses = [System.Collections.Generic.Queue[object]]::new()
    foreach ($pass in 1..2) {
        $responses.Enqueue((New-StableStatusResponse))
        $responses.Enqueue((New-CleanPageResponse 1 $true))
        $responses.Enqueue((New-CleanPageResponse 2 $false))
        $responses.Enqueue((New-StableStatusResponse))
    }
    $requestedUris = [System.Collections.Generic.List[string]]::new()
    $cleanInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        $requestedUris.Add($Uri.AbsoluteUri)
        return $responses.Dequeue()
    }.GetNewClosure()
    $cleanReportPath = Join-Path $temporaryRoot 'clean.json'

    $clean = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'must-not-be-written' `
        -OutputPath $cleanReportPath `
        -PageSize 1 `
        -RequestInvoker $cleanInvoker

    Assert-True $clean.GatePassed 'Expected two clean passes to pass the gate.'
    Assert-True ($clean.Passes.Count -eq 2) 'Expected exactly two clean passes.'
    Assert-True `
        ($requestedUris[2] -match 'afterOwnerUserId=1&afterListType=1') `
        'Expected the second page to use the compound continuation cursor.'
    Assert-True `
        (-not ([System.IO.File]::ReadAllText($cleanReportPath).Contains('must-not-be-written'))) `
        'The report must not contain the Ops API key.'

    $mismatchResponses = [System.Collections.Generic.Queue[object]]::new()
    $mismatchResponses.Enqueue((New-StableStatusResponse))
    $mismatchResponses.Enqueue((New-Response 409 ([ordered]@{
        available = $true
        error = $null
        items = @([ordered]@{
            ownerUserId = 3
            listType = 1
            matches = $false
            issues = @('current_version_mismatch')
        })
        matchedCount = 0
        mismatchCount = 1
        hasMore = $false
        nextOwnerUserId = $null
        nextListType = $null
        generatedAtMs = 12
    })))
    $mismatchInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        return $mismatchResponses.Dequeue()
    }.GetNewClosure()
    $mismatch = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'test' `
        -OutputPath (Join-Path $temporaryRoot 'mismatch.json') `
        -RequestInvoker $mismatchInvoker

    Assert-True (-not $mismatch.GatePassed) 'Expected a 409 page to fail the gate.'
    Assert-True `
        ($mismatch.FailureReason -eq 'reconciliation_mismatch') `
        'Expected a stable mismatch failure reason.'
    Assert-True `
        ($mismatch.Passes[0].Mismatches[0].issues[0] -eq 'current_version_mismatch') `
        'Expected the report to retain the stable mismatch reason.'

    $unavailableResponses = [System.Collections.Generic.Queue[object]]::new()
    $unavailableResponses.Enqueue((New-StableStatusResponse))
    $unavailableResponses.Enqueue((New-Response 503 ([ordered]@{
        available = $false
        error = 'server_projection_source_unavailable'
        items = @()
        matchedCount = 0
        mismatchCount = 0
        hasMore = $false
        nextOwnerUserId = $null
        nextListType = $null
        generatedAtMs = 12
    })))
    $unavailableInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        return $unavailableResponses.Dequeue()
    }.GetNewClosure()
    $unavailable = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'test' `
        -OutputPath (Join-Path $temporaryRoot 'unavailable.json') `
        -RequestInvoker $unavailableInvoker

    Assert-True (-not $unavailable.GatePassed) 'Expected a 503 page to fail the gate.'
    Assert-True `
        ($unavailable.FailureReason -eq 'reconciliation_unavailable') `
        'Expected a stable unavailable failure reason.'
    Assert-True `
        ($unavailable.Passes[0].Pages[0].error -eq 'server_projection_source_unavailable') `
        'Expected the report to retain the upstream unavailable reason.'

    $changingResponses = [System.Collections.Generic.Queue[object]]::new()
    $changingResponses.Enqueue((New-StableStatusResponse 7))
    $changingResponses.Enqueue((New-CleanPageResponse 1 $false))
    $changingResponses.Enqueue((New-StableStatusResponse 8))
    $changingInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        return $changingResponses.Dequeue()
    }.GetNewClosure()
    $changing = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'test' `
        -OutputPath (Join-Path $temporaryRoot 'changing.json') `
        -RequestInvoker $changingInvoker

    Assert-True (-not $changing.GatePassed) 'Expected a changing Rebuilder status to fail the gate.'
    Assert-True `
        ($changing.FailureReason -eq 'projection_status_changed_during_pass') `
        'Expected the status-change failure reason.'
    Assert-True `
        ($changing.Passes[0].statusTokenBefore -ne $changing.Passes[0].statusTokenAfter) `
        'Expected the report to retain both status tokens.'

    $stalledResponses = [System.Collections.Generic.Queue[object]]::new()
    $stalledResponses.Enqueue((New-StableStatusResponse))
    $stalledResponses.Enqueue((New-CleanPageResponse 1 $true))
    $stalledResponses.Enqueue((New-CleanPageResponse 1 $true))
    $stalledInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        return $stalledResponses.Dequeue()
    }.GetNewClosure()
    $stalled = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'test' `
        -OutputPath (Join-Path $temporaryRoot 'stalled.json') `
        -PageSize 1 `
        -RequestInvoker $stalledInvoker

    Assert-True (-not $stalled.GatePassed) 'Expected a stalled cursor to fail the gate.'
    Assert-True `
        ($stalled.FailureReason -eq 'reconciliation_cursor_not_advanced') `
        'Expected the cursor-stall failure reason.'

    $driftingResponses = [System.Collections.Generic.Queue[object]]::new()
    $driftingResponses.Enqueue((New-StableStatusResponse))
    $driftingResponses.Enqueue((New-CleanPageResponse 1 $false))
    $driftingResponses.Enqueue((New-StableStatusResponse))
    $driftingResponses.Enqueue((New-StableStatusResponse))
    $driftingResponses.Enqueue((New-CleanPageResponse 2 $false))
    $driftingResponses.Enqueue((New-StableStatusResponse))
    $driftingInvoker = {
        param($Uri, $Headers, $TimeoutSeconds)
        return $driftingResponses.Dequeue()
    }.GetNewClosure()
    $drifting = Invoke-RelationshipProjectionReconcileGate `
        -BaseUri 'https://realtime.test' `
        -ApiKey 'test' `
        -OutputPath (Join-Path $temporaryRoot 'drifting.json') `
        -RequestInvoker $driftingInvoker

    Assert-True (-not $drifting.GatePassed) 'Expected different full-pass fingerprints to fail the gate.'
    Assert-True `
        ($drifting.FailureReason -eq 'reconciliation_changed_between_passes') `
        'Expected the cross-pass drift failure reason.'
    Assert-True `
        ($drifting.Passes[0].fingerprintSha256 -ne $drifting.Passes[1].fingerprintSha256) `
        'Expected the report to retain both different fingerprints.'

    $httpRejected = $false
    try {
        [void] (Invoke-RelationshipProjectionReconcileGate `
            -BaseUri 'http://realtime.test' `
            -ApiKey 'must-use-tls' `
            -OutputPath (Join-Path $temporaryRoot 'insecure.json') `
            -RequestInvoker { throw 'must not be called' })
    }
    catch [System.ArgumentException] {
        $httpRejected = $true
    }
    Assert-True $httpRejected 'Expected an Ops API key over HTTP to be rejected before the request.'

    Write-Output 'Relationship projection reconcile script tests passed: 7/7.'
}
finally {
    if ([System.IO.Directory]::Exists($temporaryRoot)) {
        [System.IO.Directory]::Delete($temporaryRoot, $true)
    }
}
