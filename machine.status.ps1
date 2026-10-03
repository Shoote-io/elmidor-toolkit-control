# =========================================================
# ELMIDOR NEXUS — DISCOVERY WORKER
# SIGNAL-AWARE RUNTIME DISCOVERY ENGINE
# =========================================================

param(
    [string]$CommandFile,
    [string]$ExecutionId
)

$ErrorActionPreference = "Stop"

# =========================================================
# CONTEXT
# =========================================================

$ContextPath = $env:NEXUS_CONTEXT_PATH

if ([string]::IsNullOrWhiteSpace($ContextPath)) {
    throw "MISSING_NEXUS_CONTEXT_PATH"
}

if (!(Test-Path $ContextPath)) {
    throw "CONTEXT_NOT_FOUND"
}

$Context = Get-Content `
    -Raw `
    $ContextPath |
    ConvertFrom-Json

$Root = $Context.PATHS.ROOT

# =========================================================
# COMMAND
# =========================================================

if ([string]::IsNullOrWhiteSpace($CommandFile)) {
    throw "MISSING_COMMAND_FILE"
}

if (!(Test-Path $CommandFile)) {
    throw "COMMAND_FILE_NOT_FOUND"
}

$Command = Get-Content `
    -Raw `
    $CommandFile |
    ConvertFrom-Json

if ([string]::IsNullOrWhiteSpace($Command.command_id)) {
    throw "MISSING_COMMAND_ID"
}

$CommandId = $Command.command_id

# =========================================================
# PATHS
# =========================================================

$BusSignalsPath = Join-Path `
    $Root `
    "RUNTIME\BUS\signals"

$StatePath = Join-Path `
    $Root `
    "RUNTIME\STATE"

$LogPath = Join-Path `
    $Root `
    "RUNTIME\LOGS\discovery.log"

# =========================================================
# INIT
# =========================================================

@(
    $BusSignalsPath,
    $StatePath,
    (Split-Path $LogPath)
) | ForEach-Object {

    New-Item `
        -ItemType Directory `
        -Path $_ `
        -Force | Out-Null
}

# =========================================================
# LOGGING
# =========================================================

function Log {

    param(
        [string]$Level,
        [string]$Operation,
        [string]$Message
    )

    $line = "[$(Get-Date -Format o)][$Level][DISCOVERY][$Operation] $Message"

    Write-Host $line

    Add-Content `
        -Path $LogPath `
        -Value $line
}

# =========================================================
# WORKER SIGNAL
# =========================================================

function Write-Signal {

    param(
        [string]$Status,
        [int]$Progress = 0,
        [object]$Result = $null,
        [object]$Note = $null
    )

    try {

        $payload = [ordered]@{

            command_id = $CommandId

            phase      = "worker"
            engine     = "discovery"

            status     = $Status
            progress   = $Progress

            note       = $Note
            result     = $Result

            timestamp  = (
                Get-Date
            ).ToUniversalTime().ToString("o")
        }

        $signalFile = Join-Path `
            $BusSignalsPath `
            "$ExecutionId.worker.json"

        $payload |
            ConvertTo-Json -Depth 50 |
            Set-Content `
                -Path $signalFile `
                -Encoding UTF8

        Log `
            "INFO" `
            "SIGNAL" `
            "$CommandId => $Status"
    }
    catch {

        Log `
            "ERROR" `
            "SIGNAL_WRITE_FAIL" `
            $_.Exception.Message
    }
}

# =========================================================
# TOOL CHECK
# =========================================================

function Test-Tool {

    param(
        [string]$Name,
        [string]$Command
    )

    try {

        $null = Invoke-Expression `
            $Command 2>$null

        return [ordered]@{
            tool      = $Name
            available = $true
        }
    }
    catch {

        return [ordered]@{
            tool      = $Name
            available = $false
        }
    }
}

# =========================================================
# START
# =========================================================

Log `
    "INFO" `
    "START" `
    "Discovery Worker Started"

Write-Signal `
    -Status "running" `
    -Progress 5 `
    -Note @{
        message = "Discovery worker initialized."
        suggestion = "Runtime environment scan started."
    }

# =========================================================
# EXECUTION
# =========================================================

try {

    # =====================================================
    # TOOL DISCOVERY
    # =====================================================

    Write-Signal `
        -Status "running" `
        -Progress 20 `
        -Note @{
            message = "Scanning system tools."
        }

    $tools = @()

    $tools += Test-Tool `
        -Name "python" `
        -Command "Get-Command python"

    $tools += Test-Tool `
        -Name "git" `
        -Command "Get-Command git"

    $tools += Test-Tool `
        -Name "node" `
        -Command "Get-Command node"

    $tools += Test-Tool `
        -Name "ffmpeg" `
        -Command "Get-Command ffmpeg"

    # =====================================================
    # ENGINE CHECK
    # =====================================================

    Write-Signal `
        -Status "running" `
        -Progress 50 `
        -Note @{
            message = "Checking runtime engines."
        }

    $engines = @(
        "queue.engine.ps1",
        "update.engine.ps1",
        "listener.engine.ps1"
    )

    $runtime = @()

    foreach ($engine in $engines) {

        try {

            $running = Get-CimInstance `
                Win32_Process `
                -ErrorAction SilentlyContinue |
                Where-Object {
                    $_.CommandLine -like "*$engine*"
                }

            $runtime += [ordered]@{
                engine  = $engine
                running = ($null -ne $running)
            }
        }
        catch {

            $runtime += [ordered]@{
                engine  = $engine
                running = $false
            }
        }
    }

    # =====================================================
    # BUILD REPORT
    # =====================================================

    Write-Signal `
        -Status "running" `
        -Progress 80 `
        -Note @{
            message = "Generating discovery report."
        }

    $availableTools = (
        $tools |
        Where-Object {
            $_.available -eq $true
        }
    ).Count

    $runningEngines = (
        $runtime |
        Where-Object {
            $_.running -eq $true
        }
    ).Count

    $report = [ordered]@{

        timestamp = (
            Get-Date
        ).ToUniversalTime().ToString("o")

        machine = $env:COMPUTERNAME

        tools = $tools

        runtime = $runtime

        summary = [ordered]@{

            tools_total = $tools.Count
            tools_available = $availableTools

            engines_total = $runtime.Count
            engines_running = $runningEngines
        }
    }

    $reportFile = Join-Path `
        $StatePath `
        "discovery.json"

    $report |
        ConvertTo-Json -Depth 50 |
        Set-Content `
            -Path $reportFile `
            -Encoding UTF8

    # =====================================================
    # FINAL STATUS
    # =====================================================

    $nextSuggestion = @()

    if ($availableTools -lt $tools.Count) {

        $nextSuggestion +=
            "Install missing runtime dependencies."
    }

    if ($runningEngines -lt $runtime.Count) {

        $nextSuggestion +=
            "Restart inactive orchestration engines."
    }

    if ($nextSuggestion.Count -eq 0) {

        $nextSuggestion +=
            "Runtime environment healthy and ready for orchestration."
    }

    # =====================================================
    # COMPLETE
    # =====================================================

    Write-Signal `
        -Status "completed" `
        -Progress 100 `
        -Result $report `
        -Note @{
            message = "Discovery completed successfully."
            suggestion = $nextSuggestion
        }

    Log `
        "INFO" `
        "COMPLETED" `
        $CommandId
}
catch {

    Write-Signal `
        -Status "failed" `
        -Progress 100 `
        -Note @{
            message = "Discovery worker execution failed."
            reason = $_.Exception.Message
            suggestion = "Inspect worker logs and runtime dependencies."
        }

    Log `
        "ERROR" `
        "FAILED" `
        $_.Exception.Message
}
