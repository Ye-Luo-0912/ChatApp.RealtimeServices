Set-StrictMode -Version Latest

function New-GateException {
    param(
        [Parameter(Mandatory)]
        [string] $Reason
    )

    $exception = [System.InvalidOperationException]::new($Reason)
    $exception.Data['GateReason'] = $Reason
    return $exception
}

function Get-RequiredValue {
    param(
        [Parameter(Mandatory)]
        [System.Collections.IDictionary] $Map,

        [Parameter(Mandatory)]
        [string] $Name
    )

    if (-not $Map.Contains($Name)) {
        throw (New-GateException 'invalid_reconciliation_response')
    }

    return $Map[$Name]
}

function ConvertFrom-ReconciliationJson {
    param(
        [Parameter(Mandatory)]
        [string] $Content
    )

    try {
        $value = ConvertFrom-Json -InputObject $Content -AsHashtable -Depth 32
    }
    catch {
        throw (New-GateException 'invalid_reconciliation_response')
    }

    if ($value -isnot [System.Collections.IDictionary]) {
        throw (New-GateException 'invalid_reconciliation_response')
    }

    return $value
}

function Invoke-DefaultReconciliationRequest {
    param(
        [Parameter(Mandatory)]
        [uri] $Uri,

        [Parameter(Mandatory)]
        [hashtable] $Headers,

        [Parameter(Mandatory)]
        [int] $TimeoutSeconds
    )

    $response = Invoke-WebRequest `
        -Uri $Uri `
        -Method Get `
        -Headers $Headers `
        -SkipHttpErrorCheck `
        -MaximumRedirection 0 `
        -TimeoutSec $TimeoutSeconds

    return [pscustomobject]@{
        StatusCode = [int] $response.StatusCode
        Content = [string] $response.Content
    }
}

function Invoke-ReconciliationRequest {
    param(
        [Parameter(Mandatory)]
        [scriptblock] $RequestInvoker,

        [Parameter(Mandatory)]
        [uri] $Uri,

        [Parameter(Mandatory)]
        [hashtable] $Headers,

        [Parameter(Mandatory)]
        [int] $TimeoutSeconds
    )

    try {
        $response = & $RequestInvoker $Uri $Headers $TimeoutSeconds
    }
    catch {
        throw (New-GateException 'reconciliation_request_failed')
    }

    if ($null -eq $response -or
        $response.PSObject.Properties.Name -notcontains 'StatusCode' -or
        $response.PSObject.Properties.Name -notcontains 'Content') {
        throw (New-GateException 'invalid_reconciliation_response')
    }

    return $response
}

function Get-RelationshipProjectionStatus {
    param(
        [Parameter(Mandatory)]
        [string] $BaseAddress,

        [Parameter(Mandatory)]
        [hashtable] $Headers,

        [Parameter(Mandatory)]
        [scriptblock] $RequestInvoker,

        [Parameter(Mandatory)]
        [int] $TimeoutSeconds,

        [Parameter(Mandatory)]
        [int] $MinimumStablePasses
    )

    $uri = [uri] ($BaseAddress + '/ops/relationship-projection/status')
    $response = Invoke-ReconciliationRequest $RequestInvoker $uri $Headers $TimeoutSeconds
    if ([int] $response.StatusCode -ne 200) {
        throw (New-GateException 'projection_status_unavailable')
    }

    $body = ConvertFrom-ReconciliationJson ([string] $response.Content)
    $available = [bool] (Get-RequiredValue $body 'available')
    $stablePasses = [int] (Get-RequiredValue $body 'stablePasses')
    $passChanged = [bool] (Get-RequiredValue $body 'passChanged')
    $cursorOwner = Get-RequiredValue $body 'cursorOwnerUserId'
    $cursorList = Get-RequiredValue $body 'cursorListType'
    $lastError = Get-RequiredValue $body 'lastError'

    if (-not $available) {
        throw (New-GateException 'projection_status_unavailable')
    }
    if ($stablePasses -lt $MinimumStablePasses -or
        $passChanged -or
        $null -ne $cursorOwner -or
        $null -ne $cursorList -or
        -not [string]::IsNullOrWhiteSpace([string] $lastError)) {
        throw (New-GateException 'projection_not_stable')
    }

    $tokenFields = @(
        'passNumber',
        'stablePasses',
        'passChanged',
        'cursorOwnerUserId',
        'cursorListType',
        'lastError',
        'versionStreamCount',
        'snapshotBaselineStreamCount',
        'streamsWithoutSnapshotBaselineCount',
        'projectionItemCount',
        'inboxEventCount',
        'updatedAtMs'
    )
    $token = [System.Text.StringBuilder]::new(192)
    foreach ($field in $tokenFields) {
        $value = Get-RequiredValue $body $field
        [void] $token.Append($field).Append('=').Append([string] $value).Append(';')
    }

    return [pscustomobject]@{
        Body = $body
        Token = $token.ToString()
    }
}

function Invoke-RelationshipProjectionReconcilePass {
    param(
        [Parameter(Mandatory)]
        [string] $BaseAddress,

        [Parameter(Mandatory)]
        [hashtable] $Headers,

        [Parameter(Mandatory)]
        [scriptblock] $RequestInvoker,

        [Parameter(Mandatory)]
        [int] $TimeoutSeconds,

        [Parameter(Mandatory)]
        [int] $PageSize,

        [Parameter(Mandatory)]
        [int] $MaxPagesPerPass
    )

    $pageSummaries = [System.Collections.Generic.List[object]]::new()
    $mismatches = [System.Collections.Generic.List[object]]::new()
    $hasher = [System.Security.Cryptography.IncrementalHash]::CreateHash(
        [System.Security.Cryptography.HashAlgorithmName]::SHA256)
    $afterOwner = $null
    $afterList = $null
    $matchedCount = 0L
    $mismatchCount = 0L

    try {
        for ($pageNumber = 1; $pageNumber -le $MaxPagesPerPass; $pageNumber++) {
            $query = 'pageSize=' + $PageSize
            if ($null -ne $afterOwner) {
                $query += '&afterOwnerUserId=' + [uri]::EscapeDataString([string] $afterOwner)
                $query += '&afterListType=' + [uri]::EscapeDataString([string] $afterList)
            }

            $uri = [uri] ($BaseAddress + '/ops/relationship-projection/reconcile?' + $query)
            $response = Invoke-ReconciliationRequest $RequestInvoker $uri $Headers $TimeoutSeconds
            $statusCode = [int] $response.StatusCode
            if ($statusCode -notin @(200, 409, 503)) {
                throw (New-GateException 'unexpected_reconciliation_status')
            }

            $body = ConvertFrom-ReconciliationJson ([string] $response.Content)
            $available = [bool] (Get-RequiredValue $body 'available')
            $pageMatched = [int] (Get-RequiredValue $body 'matchedCount')
            $pageMismatched = [int] (Get-RequiredValue $body 'mismatchCount')
            $hasMore = [bool] (Get-RequiredValue $body 'hasMore')
            $nextOwner = Get-RequiredValue $body 'nextOwnerUserId'
            $nextList = Get-RequiredValue $body 'nextListType'
            $pageError = Get-RequiredValue $body 'error'
            $itemsValue = Get-RequiredValue $body 'items'
            $items = @($itemsValue)

            if (-not $available -or $statusCode -eq 503) {
                $pageSummaries.Add([ordered]@{
                    page = $pageNumber
                    statusCode = $statusCode
                    error = $pageError
                    matchedCount = $pageMatched
                    mismatchCount = $pageMismatched
                    hasMore = $hasMore
                    nextOwnerUserId = $nextOwner
                    nextListType = $nextList
                })
                return [pscustomobject]@{
                    Succeeded = $false
                    FailureReason = 'reconciliation_unavailable'
                    Pages = $pageSummaries.ToArray()
                    Mismatches = $mismatches.ToArray()
                    MatchedCount = $matchedCount
                    MismatchCount = $mismatchCount
                    Fingerprint = $null
                }
            }
            if (($statusCode -eq 200 -and $pageMismatched -ne 0) -or
                ($statusCode -eq 409 -and $pageMismatched -eq 0) -or
                $pageMatched -lt 0 -or
                $pageMismatched -lt 0 -or
                $pageMatched + $pageMismatched -ne $items.Count) {
                throw (New-GateException 'invalid_reconciliation_response')
            }

            foreach ($item in $items) {
                $itemJson = ConvertTo-Json -InputObject $item -Compress -Depth 16
                $itemBytes = [System.Text.Encoding]::UTF8.GetBytes($itemJson + "`n")
                $hasher.AppendData($itemBytes)
                if ($statusCode -eq 409) {
                    $mismatches.Add($item)
                }
            }

            $matchedCount += $pageMatched
            $mismatchCount += $pageMismatched
            $pageSummaries.Add([ordered]@{
                page = $pageNumber
                statusCode = $statusCode
                error = $pageError
                matchedCount = $pageMatched
                mismatchCount = $pageMismatched
                hasMore = $hasMore
                nextOwnerUserId = $nextOwner
                nextListType = $nextList
            })

            if ($statusCode -eq 409) {
                $fingerprint = [Convert]::ToHexString($hasher.GetHashAndReset())
                return [pscustomobject]@{
                    Succeeded = $false
                    FailureReason = 'reconciliation_mismatch'
                    Pages = $pageSummaries.ToArray()
                    Mismatches = $mismatches.ToArray()
                    MatchedCount = $matchedCount
                    MismatchCount = $mismatchCount
                    Fingerprint = $fingerprint
                }
            }
            if (-not $hasMore) {
                $fingerprint = [Convert]::ToHexString($hasher.GetHashAndReset())
                return [pscustomobject]@{
                    Succeeded = $true
                    FailureReason = $null
                    Pages = $pageSummaries.ToArray()
                    Mismatches = $mismatches.ToArray()
                    MatchedCount = $matchedCount
                    MismatchCount = $mismatchCount
                    Fingerprint = $fingerprint
                }
            }
            if ($null -eq $nextOwner -or $null -eq $nextList) {
                throw (New-GateException 'reconciliation_cursor_missing')
            }
            if ($afterOwner -eq $nextOwner -and $afterList -eq $nextList) {
                throw (New-GateException 'reconciliation_cursor_not_advanced')
            }

            $afterOwner = $nextOwner
            $afterList = $nextList
        }

        throw (New-GateException 'reconciliation_page_limit_exceeded')
    }
    finally {
        $hasher.Dispose()
    }
}

function Invoke-RelationshipProjectionReconcileGate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [uri] $BaseUri,

        [string] $ApiKey,

        [Parameter(Mandatory)]
        [string] $OutputPath,

        [ValidateRange(1, 100)]
        [int] $PageSize = 50,

        [ValidateRange(2, 10)]
        [int] $RequiredCleanPasses = 2,

        [ValidateRange(2, 10)]
        [int] $MinimumRebuilderStablePasses = 2,

        [ValidateRange(1, 10000)]
        [int] $MaxPagesPerPass = 10000,

        [ValidateRange(1, 300)]
        [int] $TimeoutSeconds = 30,

        [switch] $AllowInsecureHttp,

        [scriptblock] $RequestInvoker = ${function:Invoke-DefaultReconciliationRequest}
    )

    if ($BaseUri.Scheme -notin @('http', 'https') -or
        -not [string]::IsNullOrEmpty($BaseUri.UserInfo) -or
        -not [string]::IsNullOrEmpty($BaseUri.Query) -or
        -not [string]::IsNullOrEmpty($BaseUri.Fragment)) {
        throw [System.ArgumentException]::new(
            'BaseUri must be HTTP(S) and must not contain user information, a query, or a fragment.')
    }
    if ($BaseUri.Scheme -eq 'http' -and
        -not [string]::IsNullOrWhiteSpace($ApiKey) -and
        -not $AllowInsecureHttp) {
        throw [System.ArgumentException]::new(
            'Refusing to send an Ops API key over HTTP. Use HTTPS or explicitly allow insecure HTTP for an isolated test.')
    }

    $baseAddress = $BaseUri.AbsoluteUri.TrimEnd('/')
    $headers = @{}
    if (-not [string]::IsNullOrWhiteSpace($ApiKey)) {
        $headers['X-Ops-Api-Key'] = $ApiKey
    }

    $startedAt = [DateTimeOffset]::UtcNow
    $passes = [System.Collections.Generic.List[object]]::new()
    $gatePassed = $false
    $failureReason = $null
    $previousFingerprint = $null

    try {
        for ($passNumber = 1; $passNumber -le $RequiredCleanPasses; $passNumber++) {
            $statusBefore = Get-RelationshipProjectionStatus `
                $baseAddress $headers $RequestInvoker $TimeoutSeconds $MinimumRebuilderStablePasses
            $pass = Invoke-RelationshipProjectionReconcilePass `
                $baseAddress $headers $RequestInvoker $TimeoutSeconds $PageSize $MaxPagesPerPass
            if (-not $pass.Succeeded) {
                $passes.Add([ordered]@{
                    pass = $passNumber
                    statusToken = $statusBefore.Token
                    fingerprintSha256 = $pass.Fingerprint
                    matchedCount = $pass.MatchedCount
                    mismatchCount = $pass.MismatchCount
                    pages = $pass.Pages
                    mismatches = $pass.Mismatches
                })
                throw (New-GateException $pass.FailureReason)
            }
            $statusAfter = Get-RelationshipProjectionStatus `
                $baseAddress $headers $RequestInvoker $TimeoutSeconds $MinimumRebuilderStablePasses

            $passes.Add([ordered]@{
                pass = $passNumber
                statusTokenBefore = $statusBefore.Token
                statusTokenAfter = $statusAfter.Token
                fingerprintSha256 = $pass.Fingerprint
                matchedCount = $pass.MatchedCount
                mismatchCount = $pass.MismatchCount
                pages = $pass.Pages
                mismatches = $pass.Mismatches
            })

            if ($statusBefore.Token -ne $statusAfter.Token) {
                throw (New-GateException 'projection_status_changed_during_pass')
            }
            if ($null -ne $previousFingerprint -and $previousFingerprint -ne $pass.Fingerprint) {
                throw (New-GateException 'reconciliation_changed_between_passes')
            }

            $previousFingerprint = $pass.Fingerprint
        }

        $gatePassed = $true
    }
    catch {
        if ($_.Exception.Data.Contains('GateReason')) {
            $failureReason = [string] $_.Exception.Data['GateReason']
        }
        else {
            $failureReason = 'reconciliation_gate_failed'
        }
    }

    $report = [ordered]@{
        schemaVersion = 1
        startedAtUtc = $startedAt.ToString('O')
        completedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
        baseUri = $baseAddress
        pageSize = $PageSize
        requiredCleanPasses = $RequiredCleanPasses
        minimumRebuilderStablePasses = $MinimumRebuilderStablePasses
        gatePassed = $gatePassed
        failureReason = $failureReason
        passes = $passes.ToArray()
    }

    $resolvedOutput = [System.IO.Path]::GetFullPath($OutputPath)
    $outputDirectory = [System.IO.Path]::GetDirectoryName($resolvedOutput)
    if (-not [string]::IsNullOrEmpty($outputDirectory)) {
        [System.IO.Directory]::CreateDirectory($outputDirectory) | Out-Null
    }
    $json = ConvertTo-Json -InputObject $report -Depth 24
    [System.IO.File]::WriteAllText(
        $resolvedOutput,
        $json + [Environment]::NewLine,
        [System.Text.UTF8Encoding]::new($false))

    return [pscustomobject] $report
}

Export-ModuleMember -Function Invoke-RelationshipProjectionReconcileGate
