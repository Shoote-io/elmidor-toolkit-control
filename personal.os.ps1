
# =====================================================================
# KAYA NEXUS PERSONAL OS BOOTSTRAP
# RESPONSIBILITIES \ SAFETY WORKER
# =====================================================================

[CmdletBinding()]
param(
    [string]$KayaRoot = 'C:\KAYA',
    [string]$CommandFile = '',
    [string]$ExecutionId = ''
)

$ErrorActionPreference = 'Stop'

# =====================================================================
# 1. VERSION AND ENVIRONMENT
# =====================================================================

$WorkerVersion = '1.0.0'
$ArchitectureVersion = '1.0.0'

$PersonalOSRoot = Join-Path $KayaRoot 'ENTITY\PROFILE\PERSONAL-OS'

$RuntimeRoot = Join-Path $KayaRoot 'RUNTIME'
$LogRoot = Join-Path $RuntimeRoot 'LOGS'
$SignalRoot = Join-Path $RuntimeRoot 'BUS\signals'

$LogPath = Join-Path $LogRoot 'bootstrap.personal-os.log'

$CommandId = ''
$PersonalOSId = ''

$StartedAt = (Get-Date).ToUniversalTime().ToString('o')

# =====================================================================
# 2. LOGGING
# =====================================================================

function Write-WorkerLog {
    param(
        [string]$Level,
        [string]$Message
    )

    $Timestamp = (Get-Date).ToUniversalTime().ToString('o')

    $Line = "[$Timestamp][$Level][PERSONAL-OS] $Message"

    Add-Content -LiteralPath $LogPath -Value $Line -Encoding UTF8
}

# =====================================================================
# 3. COMMAND CONTEXT
# =====================================================================

function Import-CommandContext {

    if ([string]::IsNullOrWhiteSpace($CommandFile)) {
        return
    }

    if (!(Test-Path -LiteralPath $CommandFile -PathType Leaf)) {
        throw "COMMAND_FILE_NOT_FOUND: $CommandFile"
    }

    $Command = Get-Content `
        -LiteralPath $CommandFile `
        -Raw `
        -Encoding UTF8 | ConvertFrom-Json

    if ($Command.command_id) {
        $script:CommandId = [string]$Command.command_id
    }
}

# =====================================================================
# 4. DIRECTORY INITIALIZATION
# =====================================================================

function Ensure-Directory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (!(Test-Path -LiteralPath $Path -PathType Container)) {

        New-Item `
            -ItemType Directory `
            -Path $Path `
            -Force | Out-Null

        Write-WorkerLog 'INFO' "DIRECTORY_CREATED $Path"
    }
}

# =====================================================================
# 5. SAFE JSON INITIALIZATION
# =====================================================================

function Ensure-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [object]$Data
    )

    if (Test-Path -LiteralPath $Path -PathType Leaf) {
        Write-WorkerLog 'INFO' "EXISTING_FILE_PRESERVED $Path"
        return
    }

    $Directory = Split-Path -Parent $Path

    Ensure-Directory -Path $Directory

    $Json = ConvertTo-Json `
        -InputObject $Data `
        -Depth 20

    $TempPath = "$Path.tmp"

    Set-Content `
        -LiteralPath $TempPath `
        -Value $Json `
        -Encoding UTF8

    Move-Item `
        -LiteralPath $TempPath `
        -Destination $Path

    Write-WorkerLog 'INFO' "FILE_CREATED $Path"
}

# =====================================================================
# 6. NEXUS WORKER SIGNAL
# =====================================================================

function Write-WorkerSignal {
    param(
        [string]$Status,
        [int]$Progress,
        [string]$Note,
        [object]$Result = $null
    )

    if ([string]::IsNullOrWhiteSpace($ExecutionId)) {
        return
    }

    $Signal = [ordered]@{
        command_id  = $CommandId
        phase       = 'worker'
        engine      = 'personal-os'
        status      = $Status
        progress    = $Progress
        note        = $Note
        result      = $Result
        timestamp   = (Get-Date).ToUniversalTime().ToString('o')
    }

    $SignalPath = Join-Path `
        $SignalRoot `
        "$ExecutionId.worker.json"

    $TempPath = "$SignalPath.tmp"

    $Json = ConvertTo-Json `
        -InputObject $Signal `
        -Depth 20

    Set-Content `
        -LiteralPath $TempPath `
        -Value $Json `
        -Encoding UTF8

    Move-Item `
        -LiteralPath $TempPath `
        -Destination $SignalPath `
        -Force
}

# =====================================================================
# 7. PERSONAL OS DIRECTORY MAP
# =====================================================================

function Initialize-DirectoryStructure {

    $Directories = @(

        # Personal OS domains
        'CORE'
        'BRIDGE'
        'LIFE'
        'SPACES'
        'LIBRARY'
        'CONNECTIONS'
        'ACTIVITY'
        'MEMORY'
        'STATE'

        # Connection domains
        'CONNECTIONS\accounts'
        'CONNECTIONS\contacts'
        'CONNECTIONS\services'
        'CONNECTIONS\integrations'

    )

    foreach ($RelativePath in $Directories) {

        $FullPath = Join-Path `
            $PersonalOSRoot `
            $RelativePath

        Ensure-Directory -Path $FullPath
    }
}

# =====================================================================
# 8. CORE INITIALIZATION
# =====================================================================

function Initialize-Core {

    $IdentityPath = Join-Path $PersonalOSRoot 'CORE\identity.json'

    $PersonalOSId = [guid]::NewGuid().ToString()

    if (Test-Path -LiteralPath $IdentityPath -PathType Leaf) {

        try {
            $ExistingIdentity = Get-Content `
                -LiteralPath $IdentityPath `
                -Raw `
                -Encoding UTF8 | ConvertFrom-Json

            if ($ExistingIdentity.personal_os_id) {
                $PersonalOSId = [string]$ExistingIdentity.personal_os_id
            }
        }
        catch {
            throw "IDENTITY_FILE_INVALID: $IdentityPath"
        }
    }

    $script:PersonalOSId = $PersonalOSId

    $Identity = [ordered]@{
        schema_version = '1.0.0'
        personal_os_id = $PersonalOSId
        entity_type = 'digital-human-environment'
        status = 'initialized'
        created_at = $StartedAt
    }

    $Manifest = [ordered]@{
        schema_version = '1.0.0'
        name = 'Personal OS'
        architecture_version = $ArchitectureVersion
        worker_version = $WorkerVersion
        parent_system = 'Elmidor Nexus'
        entity_root = $PersonalOSRoot

        domains = @(
            'CORE'
            'BRIDGE'
            'LIFE'
            'SPACES'
            'LIBRARY'
            'CONNECTIONS'
            'ACTIVITY'
            'MEMORY'
            'STATE'
        )

        capabilities = @(
            'general'
            'identity'
            'accounts'
            'personal-organization'
            'knowledge-management'
            'activity-management'
        )

        created_at = $StartedAt
    }

    $Configuration = [ordered]@{
        schema_version = '1.0.0'
        language = $null
        timezone = $null
        date_format = 'yyyy-MM-dd'
        time_format = '24h'
        default_space = 'personal'
        automatic_archival = $false
        created_at = $StartedAt
    }

    $Permissions = [ordered]@{
        schema_version = '1.0.0'
        default_access = 'restricted'
        external_connections = $false
        account_access = $false
        credential_access = $false
        filesystem_access = 'personal-os-root'
        require_authorization_for_sensitive_actions = $true
    }

    $Policies = [ordered]@{
        schema_version = '1.0.0'
        preserve_existing_data = $true
        create_optional_resources_on_demand = $true
        prevent_duplicate_records = $true
        protect_credentials = $true
        require_authorization_for_external_actions = $true
    }

    Ensure-JsonFile `
        -Path $IdentityPath `
        -Data $Identity

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'CORE\manifest.json') `
        -Data $Manifest

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'CORE\configuration.json') `
        -Data $Configuration

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'CORE\permissions.json') `
        -Data $Permissions

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'CORE\policies.json') `
        -Data $Policies
}

# =====================================================================
# 9. LIFE INITIALIZATION
# =====================================================================

function Initialize-Life {

    $Profile = [ordered]@{
        schema_version = '1.0.0'
        profile_id = [guid]::NewGuid().ToString()
        display_name = $null
        description = $null
        profile_type = @(
            'general'
        )
        created_at = $StartedAt
    }

    $Preferences = [ordered]@{
        schema_version = '1.0.0'
        communication = @{}
        organization = @{}
        productivity = @{}
        accessibility = @{}
        updated_at = $StartedAt
    }

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'LIFE\profile.json') `
        -Data $Profile

    Ensure-JsonFile `
        -Path (Join-Path $PersonalOSRoot 'LIFE\preferences.json') `
        -Data $Preferences
}

# =====================================================================
# 10. ACCOUNTS INITIALIZATION
# =====================================================================

function Initialize-Accounts {

    $AccountsRoot = Join-Path `
        $PersonalOSRoot `
        'CONNECTIONS\accounts'

    Ensure-Directory -Path $AccountsRoot

    $Registry = [ordered]@{
        schema_version = '1.0.0'
        registry_type = 'digital-account-registry'
        accounts = @()
        total_accounts = 0
        created_at = $StartedAt
        updated_at = $StartedAt
    }

    Ensure-JsonFile `
        -Path (Join-Path $AccountsRoot 'registry.json') `
        -Data $Registry
}

# =====================================================================
# 11. STATE INITIALIZATION
# =====================================================================

function Initialize-State {

    $BootstrapState = [ordered]@{
        schema_version = '1.0.0'
        personal_os_id = $PersonalOSId
        architecture_version = $ArchitectureVersion
        worker_version = $WorkerVersion
        bootstrap_status = 'completed'
        last_bootstrap_at = $StartedAt
        parent_runtime = 'Elmidor Nexus'
    }

    $StatePath = Join-Path `
        $PersonalOSRoot `
        'STATE\bootstrap.state.json'

    Ensure-JsonFile `
        -Path $StatePath `
        -Data $BootstrapState
}

# =====================================================================
# 12. MAIN EXECUTION
# =====================================================================

$ExitCode = 0

$Result = $null

try {

    Ensure-Directory -Path $LogRoot
    Ensure-Directory -Path $SignalRoot

    Write-WorkerLog 'INFO' "WORKER_START version=$WorkerVersion"
    Write-WorkerLog 'INFO' "KAYA_ROOT=$KayaRoot"
    Write-WorkerLog 'INFO' "PERSONAL_OS_ROOT=$PersonalOSRoot"

    Write-WorkerSignal `
        -Status 'running' `
        -Progress 5 `
        -Note 'Loading Personal OS context.'

    Import-CommandContext

    if (!(Test-Path -LiteralPath $KayaRoot -PathType Container)) {
        throw "KAYA_ROOT_NOT_FOUND: $KayaRoot"
    }

    Ensure-Directory -Path $PersonalOSRoot

    Write-WorkerSignal `
        -Status 'running' `
        -Progress 20 `
        -Note 'Initializing Personal OS domains.'

    Initialize-DirectoryStructure

    Write-WorkerSignal `
        -Status 'running' `
        -Progress 45 `
        -Note 'Initializing Core architecture.'

    Initialize-Core

    Write-WorkerSignal `
        -Status 'running' `
        -Progress 65 `
        -Note 'Initializing personal environment.'

    Initialize-Life

    Write-WorkerSignal `
        -Status 'running' `
        -Progress 80 `
        -Note 'Initializing digital account registry.'

    Initialize-Accounts

    Initialize-State

    $Result = [ordered]@{
        command_id = $CommandId
        execution_id = $ExecutionId
        personal_os_id = $PersonalOSId
        personal_os_root = $PersonalOSRoot
        architecture_version = $ArchitectureVersion
        worker_version = $WorkerVersion
        status = 'completed'
        initialized_domains = @(
            'CORE'
            'BRIDGE'
            'LIFE'
            'SPACES'
            'LIBRARY'
            'CONNECTIONS'
            'ACTIVITY'
            'MEMORY'
            'STATE'
        )
        account_registry = Join-Path `
            $PersonalOSRoot `
            'CONNECTIONS\accounts\registry.json'
        log_path = $LogPath
    }

    Write-WorkerLog 'INFO' 'PERSONAL_OS_INITIALIZATION_COMPLETED'

    Write-WorkerSignal `
        -Status 'completed' `
        -Progress 100 `
        -Note 'Personal OS initialized successfully.' `
        -Result $Result

}
catch {

    $ExitCode = 1

    $ErrorMessage = $_.Exception.Message

    $Result = [ordered]@{
        command_id = $CommandId
        execution_id = $ExecutionId
        personal_os_id = $PersonalOSId
        personal_os_root = $PersonalOSRoot
        status = 'failed'
        error = $ErrorMessage
        log_path = $LogPath
    }

    try {

        Write-WorkerLog 'ERROR' $ErrorMessage

        Write-WorkerSignal `
            -Status 'failed' `
            -Progress 100 `
            -Note $ErrorMessage `
            -Result $Result

    }
    catch {
        Write-Error "WORKER_FAILURE_SIGNAL_ERROR: $($_.Exception.Message)"
    }
}

# =====================================================================
# 13. FINAL OUTPUT
# =====================================================================

if ($null -ne $Result) {

    $Result | ConvertTo-Json -Depth 20
}

exit $ExitCode
