
[CmdletBinding()]
param(
    [string]$KayaRoot = 'C:\KAYA',
    [string]$CommandFile = '',
    [string]$ExecutionId = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ============================================================
# 1. WORKER IDENTITY
# ============================================================

$WorkerName        = 'run.personal-os'
$WorkerVersion     = '1.1.0'
$ArchitectureVersion = '1.1.0'
$SchemaVersion     = '1.0'

$StartedAt = (Get-Date).ToUniversalTime().ToString('o')

$CommandId = ''

# ============================================================
# 2. PATH DISCOVERY
# ============================================================

$ProfileRoot = Join-Path $KayaRoot 'ENTITY\PROFILE'

$PersonalOSRoot = Join-Path `
    $ProfileRoot `
    'PERSONAL-OS'

$RuntimeRoot = Join-Path $KayaRoot 'RUNTIME'

$LogRoot = Join-Path $RuntimeRoot 'LOGS'

$SignalBusRoot = Join-Path `
    $RuntimeRoot `
    'BUS\signals'

$LogPath = Join-Path `
    $LogRoot `
    'bootstrap.personal-os.log'

$SignalPath = $null

# ============================================================
# 3. LOGGING
# ============================================================

function Write-WorkerLog {

    param(
        [ValidateSet('INFO','WARN','ERROR')]
        [string]$Level = 'INFO',

        [string]$Message
    )

    $Timestamp = (
        Get-Date
    ).ToUniversalTime().ToString('o')

    $Line = '[{0}][{1}][PERSONAL_OS] {2}' -f `
        $Timestamp,
        $Level,
        $Message

    Add-Content `
        -Path $LogPath `
        -Value $Line `
        -Encoding UTF8
}

# ============================================================
# 4. JSON HELPERS
# ============================================================

function Write-JsonFile {

    param(
        [Parameter(Mandatory)]
        [string]$Path,

        [Parameter(Mandatory)]
        [object]$Object
    )

    $Directory = Split-Path $Path -Parent

    if (-not (Test-Path $Directory)) {

        New-Item `
            -ItemType Directory `
            -Path $Directory `
            -Force |
            Out-Null
    }

    $Json = ConvertTo-Json `
        -InputObject $Object `
        -Depth 30

    Set-Content `
        -Path $Path `
        -Value $Json `
        -Encoding UTF8
}

function Write-JsonIfMissing {

    param(
        [string]$Path,
        [object]$Object
    )

    if (-not (Test-Path $Path)) {

        Write-JsonFile `
            -Path $Path `
            -Object $Object
    }
}

function Write-TextIfMissing {

    param(
        [string]$Path,
        [string]$Content
    )

    if (-not (Test-Path $Path)) {

        $Directory = Split-Path $Path -Parent

        if (-not (Test-Path $Directory)) {

            New-Item `
                -ItemType Directory `
                -Path $Directory `
                -Force |
                Out-Null
        }

        Set-Content `
            -Path $Path `
            -Value $Content `
            -Encoding UTF8
    }
}

# ============================================================
# 5. COMMAND CONTEXT
# ============================================================

function Import-CommandContext {

    param(
        [string]$Path
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }

    if (-not (Test-Path $Path -PathType Leaf)) {
        throw "COMMAND_FILE_NOT_FOUND: $Path"
    }

    $CommandData = Get-Content `
        -Path $Path `
        -Raw |
        ConvertFrom-Json

    if (
        [string]::IsNullOrWhiteSpace(
            [string]$CommandData.command_id
        )
    ) {
        throw 'COMMAND_ID_NOT_FOUND'
    }

    $script:CommandId = [string]$CommandData.command_id
}

# ============================================================
# 6. QUEUE WORKER SIGNAL
# ============================================================

function Write-WorkerSignal {

    param(
        [ValidateSet('running','completed','failed')]
        [string]$Status,

        [int]$Progress = 0,

        [object]$Result = $null,

        [object]$Note = $null
    )

    $Payload = [ordered]@{

        command_id = $CommandId

        phase = 'worker'

        engine = 'personal-os'

        status = $Status

        progress = $Progress

        note = $Note

        result = $Result

        timestamp = (
            Get-Date
        ).ToUniversalTime().ToString('o')
    }

    $TemporaryPath = Join-Path `
        $SignalBusRoot `
        "$ExecutionId.worker.tmp"

    $FinalPath = Join-Path `
        $SignalBusRoot `
        "$ExecutionId.worker.json"

    $Json = ConvertTo-Json `
        -InputObject $Payload `
        -Depth 50

    Set-Content `
        -Path $TemporaryPath `
        -Value $Json `
        -Encoding UTF8

    Move-Item `
        -Path $TemporaryPath `
        -Destination $FinalPath `
        -Force

    $script:SignalPath = $FinalPath
}

# ============================================================
# 7. PERSONAL OS DOMAIN STRUCTURE
# ============================================================

function Initialize-PersonalOSStructure {

    $Directories = @(
        'CORE',
        'BRIDGE',
        'LIFE',
        'SPACES',
        'LIBRARY',
        'CONNECTIONS',
        'ACTIVITY',
        'MEMORY',
        'STATE',
        'MODULES',
        'launch'
    )

    foreach ($Directory in $Directories) {

        $Path = Join-Path `
            $PersonalOSRoot `
            $Directory

        New-Item `
            -ItemType Directory `
            -Path $Path `
            -Force |
            Out-Null
    }

    Write-WorkerLog `
        -Level INFO `
        -Message 'Personal OS domain structure verified.'
}

# ============================================================
# 8. CORE FILES
# ============================================================

function Initialize-PersonalOSCore {

    $IdentityPath = Join-Path `
        $PersonalOSRoot `
        'CORE\identity.json'

    $ManifestPath = Join-Path `
        $PersonalOSRoot `
        'CORE\manifest.json'

    $ConfigurationPath = Join-Path `
        $PersonalOSRoot `
        'CORE\configuration.json'

    $PermissionsPath = Join-Path `
        $PersonalOSRoot `
        'CORE\permissions.json'

    $PoliciesPath = Join-Path `
        $PersonalOSRoot `
        'CORE\policies.json'

    $PersonalOSId = 'pos-' + [guid]::NewGuid().ToString()

    Write-JsonIfMissing `
        -Path $IdentityPath `
        -Object ([ordered]@{

            schema_version = '1.0'

            personal_os_id = $PersonalOSId

            name = 'Personal OS'

            created_at = (
                Get-Date
            ).ToUniversalTime().ToString('o')

            status = 'initialized'
        })

    $Identity = Get-Content `
        -Path $IdentityPath `
        -Raw |
        ConvertFrom-Json

    Write-JsonIfMissing `
        -Path $ManifestPath `
        -Object ([ordered]@{

            schema_version = '1.0'

            architecture_version = $ArchitectureVersion

            worker_version = $WorkerVersion

            personal_os_id = $Identity.personal_os_id

            root = $PersonalOSRoot

            domains = @(
                'CORE',
                'BRIDGE',
                'LIFE',
                'SPACES',
                'LIBRARY',
                'CONNECTIONS',
                'ACTIVITY',
                'MEMORY',
                'STATE',
                'MODULES'
            )
        })

    Write-JsonIfMissing `
        -Path $ConfigurationPath `
        -Object ([ordered]@{

            schema_version = '1.0'

            personal_os_root = $PersonalOSRoot

            bridge_status = 'unconfigured'

            environment = 'local'

            initialized = $true
        })

    Write-JsonIfMissing `
        -Path $PermissionsPath `
        -Object ([ordered]@{

            schema_version = '1.0'

            permissions = @{}

            default_policy = 'deny'
        })

    Write-JsonIfMissing `
        -Path $PoliciesPath `
        -Object ([ordered]@{

            schema_version = '1.0'

            policies = @()

            enforcement = 'pending'
        })

    return $Identity
}

# ============================================================
# 9. DOMAIN DESCRIPTIONS
# ============================================================

function Initialize-PersonalOSDocumentation {

    $Documents = @{

        'README.md' = @'
# Personal OS

Personal environment and digital stewardship architecture.

This directory is managed by the Personal OS bootstrap worker.
'@

        'BRIDGE\README.md' = @'
# Nexus Bridge

Integration boundary between Personal OS and Nexus.
'@

        'LIFE\README.md' = @'
# Life

Personal life organization domain.
'@

        'SPACES\README.md' = @'
# Spaces

Personal spaces and organizational environments.
'@

        'LIBRARY\README.md' = @'
# Library

Personal knowledge and resource organization.
'@

        'CONNECTIONS\README.md' = @'
# Connections

External services and connection definitions.
'@

        'ACTIVITY\README.md' = @'
# Activity

Personal activity records and history.
'@

        'MEMORY\README.md' = @'
# Memory

Personal OS memory domain.
'@

        'MODULES\README.md' = @'
# Modules

Personal OS functional modules.
'@

        'launch\README.md' = @'
# Launch

Personal OS startup and entry definitions.
'@
    }

    foreach ($RelativePath in $Documents.Keys) {

        $Path = Join-Path `
            $PersonalOSRoot `
            $RelativePath

        Write-TextIfMissing `
            -Path $Path `
            -Content $Documents[$RelativePath]
    }
}

# ============================================================
# 10. STATE
# ============================================================

function Initialize-PersonalOSState {

    $StatePath = Join-Path `
        $PersonalOSRoot `
        'STATE\bootstrap.state.json'

    Write-JsonIfMissing `
        -Path $StatePath `
        -Object ([ordered]@{

            schema_version = '1.0'

            status = 'initialized'

            architecture_version = $ArchitectureVersion

            worker_version = $WorkerVersion

            initialized_at = (
                Get-Date
            ).ToUniversalTime().ToString('o')
        })
}

# ============================================================
# 11. MAIN EXECUTION
# ============================================================

$ExitCode = 0
$Result = $null

try {

    if (-not (Test-Path $KayaRoot -PathType Container)) {
        throw "KAYA_ROOT_NOT_FOUND: $KayaRoot"
    }

    New-Item `
        -ItemType Directory `
        -Path $LogRoot `
        -Force |
        Out-Null

    New-Item `
        -ItemType Directory `
        -Path $SignalBusRoot `
        -Force |
        Out-Null

    Write-WorkerLog `
        -Level INFO `
        -Message "Worker starting. command_id=$CommandId execution_id=$ExecutionId"

    Write-WorkerLog `
        -Level INFO `
        -Message "KAYA_ROOT=$KayaRoot"

    Write-WorkerLog `
        -Level INFO `
        -Message "PERSONAL_OS_ROOT=$PersonalOSRoot"

    # Read command context when Queue supplies it.
    Import-CommandContext -Path $CommandFile

    Write-WorkerLog `
        -Level INFO `
        -Message "COMMAND_ID=$CommandId"

    if ([string]::IsNullOrWhiteSpace($ExecutionId)) {
        throw 'MISSING_EXECUTION_ID'
    }

    # START
    Write-WorkerSignal `
        -Status running `
        -Progress 5 `
        -Note @{
            message = 'Personal OS bootstrap initialized.'
        }

    # DOMAIN STRUCTURE
    Write-WorkerSignal `
        -Status running `
        -Progress 20 `
        -Note @{
            message = 'Creating Personal OS domain structure.'
        }

    Initialize-PersonalOSStructure

    # CORE
    Write-WorkerSignal `
        -Status running `
        -Progress 45 `
        -Note @{
            message = 'Initializing Personal OS core.'
        }

    $Identity = Initialize-PersonalOSCore

    # DOCUMENTATION
    Write-WorkerSignal `
        -Status running `
        -Progress 65 `
        -Note @{
            message = 'Initializing Personal OS domain documentation.'
        }

    Initialize-PersonalOSDocumentation

    # STATE
    Write-WorkerSignal `
        -Status running `
        -Progress 80 `
        -Note @{
            message = 'Initializing Personal OS state.'
        }

    Initialize-PersonalOSState

    # VERIFICATION
    $RequiredPaths = @(
        $PersonalOSRoot,
        (Join-Path $PersonalOSRoot 'CORE'),
        (Join-Path $PersonalOSRoot 'BRIDGE'),
        (Join-Path $PersonalOSRoot 'LIFE'),
        (Join-Path $PersonalOSRoot 'SPACES'),
        (Join-Path $PersonalOSRoot 'LIBRARY'),
        (Join-Path $PersonalOSRoot 'CONNECTIONS'),
        (Join-Path $PersonalOSRoot 'ACTIVITY'),
        (Join-Path $PersonalOSRoot 'MEMORY'),
        (Join-Path $PersonalOSRoot 'STATE'),
        (Join-Path $PersonalOSRoot 'MODULES'),
        (Join-Path $PersonalOSRoot 'launch')
    )

    foreach ($Path in $RequiredPaths) {

        if (-not (Test-Path $Path)) {
            throw "PERSONAL_OS_STRUCTURE_INCOMPLETE: $Path"
        }
    }

    Write-WorkerLog `
        -Level INFO `
        -Message 'Personal OS verification completed successfully.'

    $Result = [ordered]@{

        schema_version = $SchemaVersion

        type = 'personal_os_bootstrap_result'

        architecture_version = $ArchitectureVersion

        worker_version = $WorkerVersion

        personal_os_id = $Identity.personal_os_id

        personal_os_root = $PersonalOSRoot

        bridge_status = 'unconfigured'

        command_id = $CommandId

        execution_id = $ExecutionId

        status = 'completed'

        signal_bus = $SignalBusRoot

        log_path = $LogPath

        created_at = (
            Get-Date
        ).ToUniversalTime().ToString('o')
    }

    # COMPLETION SIGNAL
    Write-WorkerSignal `
        -Status completed `
        -Progress 100 `
        -Result $Result `
        -Note @{
            message = 'Personal OS bootstrap completed successfully.'
        }

    Write-WorkerLog `
        -Level INFO `
        -Message "Worker completed successfully. execution_id=$ExecutionId"
}
catch {

    $ExitCode = 1

    $ErrorMessage = $_.Exception.Message

    try {

        Write-WorkerLog `
            -Level ERROR `
            -Message "Worker failed: $ErrorMessage"

        $FailureResult = [ordered]@{

            schema_version = $SchemaVersion

            type = 'personal_os_bootstrap_result'

            command_id = $CommandId

            execution_id = $ExecutionId

            status = 'failed'

            error = $ErrorMessage

            created_at = (
                Get-Date
            ).ToUniversalTime().ToString('o')
        }

        if (
            (Test-Path $SignalBusRoot) -and
            -not [string]::IsNullOrWhiteSpace($ExecutionId)
        ) {

            Write-WorkerSignal `
                -Status failed `
                -Progress 100 `
                -Result $FailureResult `
                -Note @{
                    message = 'Personal OS bootstrap failed.'
                    reason = $ErrorMessage
                }
        }
    }
    catch {
        # Preserve the original worker failure.
    }

    $Result = $FailureResult
}

# ============================================================
# 12. FINAL OUTPUT
# ============================================================

if ($null -ne $Result) {

    $Result |
        ConvertTo-Json -Depth 30 |
        Write-Output
}

exit $ExitCode
