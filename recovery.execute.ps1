# =========================================================
# ELMIDOR NEXUS
# RECOVERY EXECUTE WORKER
# BLOCK 01
# BOOTSTRAP + CONTEXT + SIGNAL BUS
# =========================================================

param(
    [string]$CommandFile,
    [string]$ExecutionId
)

$ErrorActionPreference = "Stop"

# =========================================================
# WORKER METADATA
# =========================================================

$WorkerName    = "recovery.execute"
$WorkerVersion = "1.0.0"

# =========================================================
# LOAD CONTEXT
# =========================================================

$ContextPath =
    $env:NEXUS_CONTEXT_PATH

if (
    [string]::IsNullOrWhiteSpace(
        $ContextPath
    )
) {

    throw "MISSING_NEXUS_CONTEXT_PATH"
}

if (
    -not (
        Test-Path $ContextPath
    )
) {

    throw "CONTEXT_FILE_NOT_FOUND"
}

try {

    $Context =
        Get-Content `
            -Raw `
            $ContextPath |
            ConvertFrom-Json
}
catch {

    throw "INVALID_CONTEXT_JSON"
}


# =========================================================
# ROOT
# =========================================================

$Root =
    $Context.
        PATHS.
        ROOT

if (
    [string]::IsNullOrWhiteSpace(
        $Root
    )
) {

    throw "CONTEXT_ROOT_NOT_DEFINED"
}

# =========================================================
# COMMAND
# =========================================================

if (
    [string]::IsNullOrWhiteSpace(
        $CommandFile
    )
) {

    Write-Host `
        "[NEXUS] Development Mode"

    $Command =
        [ordered]@{

            command_id =
                "DEV_RECOVERY_EXECUTE"

            created_at =
                (
                    Get-Date
                ).
                    ToUniversalTime().
                    ToString("o")
        }

}
else {

    if (
        -not (
            Test-Path `
                $CommandFile
        )
    ) {

        throw "COMMAND_FILE_NOT_FOUND"
    }

    try {

        $Command =
            Get-Content `
                -Raw `
                $CommandFile |
            ConvertFrom-Json `
            
    }
    catch {

        throw "INVALID_COMMAND_JSON"
    }
}

$CommandId =
    $Command.command_id

if (
    [string]::IsNullOrWhiteSpace(
        $CommandId
    )
) {

    $CommandId =
        "UNKNOWN_COMMAND"
}

# =========================================================
# EXECUTION ID
# =========================================================

if (
    [string]::IsNullOrWhiteSpace(
        $ExecutionId
    )
) {

    $ExecutionId =
        [guid]::NewGuid().
            ToString()
}

# =========================================================
# PATHS
# =========================================================

$RuntimePath =
    Join-Path `
        $Root `
        "RUNTIME"

$BusPath =
    Join-Path `
        $RuntimePath `
        "BUS"

$SignalsPath =
    Join-Path `
        $BusPath `
        "signals"

$LogsPath =
    Join-Path `
        $RuntimePath `
        "LOGS"

$RegistryPath =
    Join-Path `
        $Root `
        "SYSTEM\REGISTRY"

$TasksPath =
    Join-Path `
        $RegistryPath `
        "tasks"

$RecoveryPath =
    Join-Path `
        $TasksPath `
        "recovery"

$LogPath =
    Join-Path `
        $LogsPath `
        "recovery.execute.log"

# =========================================================
# INITIALIZE DIRECTORIES
# =========================================================

@(
    $RuntimePath
    $BusPath
    $SignalsPath
    $LogsPath
    $RegistryPath
    $TasksPath
    $RecoveryPath
) |
ForEach-Object {

    if (
        -not (
            Test-Path $_
        )
    ) {

        New-Item `
            -ItemType Directory `
            -Path $_ `
            -Force |
        Out-Null
    }
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

    $Timestamp =
        (
            Get-Date
        ).
            ToUniversalTime().
            ToString("o")

    $Line =
        "[$Timestamp][$Level][$WorkerName][$Operation] $Message"

    Write-Host `
        $Line

    Add-Content `
        -Path $LogPath `
        -Value $Line
}

# =========================================================
# SIGNAL BUS
# =========================================================

function Write-Signal {

    param(
        [string]$Status,
        [int]$Progress = 0,
        [object]$Result = $null,
        [object]$Note = $null
    )

    try {

        $Payload =
            [ordered]@{

                execution_id =
                    $ExecutionId

                command_id =
                    $CommandId

                worker =
                    $WorkerName

                version =
                    $WorkerVersion

                status =
                    $Status

                progress =
                    $Progress

                note =
                    $Note

                result =
                    $Result

                timestamp =
                    (
                        Get-Date
                    ).
                        ToUniversalTime().
                        ToString("o")
            }

        $SignalFile =
            Join-Path `
                $SignalsPath `
                "$ExecutionId.worker.json"

        $Payload |
            ConvertTo-Json `
                 |
            Set-Content `
                -Path $SignalFile `
                -Encoding UTF8
    }
    catch {

        Write-Host `
            $_.Exception.Message
    }
}

# =========================================================
# WORKER START
# =========================================================

Log `
    "INFO" `
    "START" `
    "Recovery Execute Worker Started"

Write-Signal `
    -Status "running" `
    -Progress 5 `
    -Note @{
        stage   = "bootstrap"
        message = "Recovery execute worker initialized."
    }

# =========================================================
# RUNTIME REGISTRATION
# =========================================================

$Runtime =
    [ordered]@{

        WorkerName =
            $WorkerName

        WorkerVersion =
            $WorkerVersion

        ExecutionId =
            $ExecutionId

        CommandId =
            $CommandId

        Root =
            $Root

        Context =
            $Context

        RecoveryPath =
            $RecoveryPath

        TasksPath =
            $TasksPath

        StartedAt =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

Log `
    "INFO" `
    "BOOTSTRAP" `
    "Bootstrap completed successfully"

# =========================================================
# BLOCK 02
# LOAD RECOVERY PACKAGE
# =========================================================

Log `
    "INFO" `
    "PACKAGE" `
    "Recovery package loading started"

# =========================================================
# LOCATE PUBLICATION
# =========================================================

$PublicationFile =
    Get-ChildItem `
        -Path $RecoveryPath `
        -Filter "recovery.publication.json" `
        -Recurse `
        -File `
        -ErrorAction SilentlyContinue |
    Sort-Object `
        LastWriteTimeUtc `
        -Descending |
    Select-Object `
        -First 1

if (
    $null -eq $PublicationFile
) {

    throw `
        "RECOVERY_PUBLICATION_NOT_FOUND"
}

Log `
    "INFO" `
    "PACKAGE" `
    (
        "Publication Found: " +
        $PublicationFile.FullName
    )

# =========================================================
# LOAD PUBLICATION
# =========================================================

try {

    $Publication =
        Get-Content `
            $PublicationFile.FullName `
            -Raw |
        ConvertFrom-Json `
            
}
catch {

    throw `
        "INVALID_RECOVERY_PUBLICATION"
}

# =========================================================
# VALIDATE PUBLICATION
# =========================================================

if (
    $null -eq $Publication
) {

    throw `
        "PUBLICATION_EMPTY"
}

if (
    $Publication.publication_status -ne
    "PUBLISHED"
) {

    throw `
        "PUBLICATION_NOT_PUBLISHED"
}

if (
    $Publication.recovery_ready -ne $true
) {

    throw `
        "RECOVERY_NOT_READY"
}

if (
    $Publication.recovery_certified -ne $true
) {

    throw `
        "RECOVERY_NOT_CERTIFIED"
}

if (
    $Publication.execution_gate.execution_allowed -ne $true
) {

    throw `
        "RECOVERY_EXECUTION_NOT_ALLOWED"
}

# =========================================================
# RECOVERY MEDIA
# =========================================================

$RecoveryMedia =
    $Publication.recovery_media

if (
    $null -eq $RecoveryMedia
) {

    throw `
        "RECOVERY_MEDIA_MISSING"
}

# =========================================================
# PATHS
# =========================================================

$ManifestPath =
    $Publication.artifacts.recovery_manifest

$PlanPath =
    $Publication.artifacts.reconstruction_plan

$RuntimeFilePath =
    $Publication.artifacts.reconstruction_runtime

$ReadinessPath =
    $Publication.artifacts.final_readiness

# =========================================================
# VALIDATE ARTIFACTS
# =========================================================

$RequiredFiles =
    @(
        $ManifestPath
        $PlanPath
        $RuntimeFilePath
        $ReadinessPath
    )

foreach (
    $File
    in
    $RequiredFiles
) {

    if (
        -not (
            Test-Path $File
        )
    ) {

        throw (
            "REQUIRED_RECOVERY_FILE_MISSING: " +
            $File
        )
    }

    Log `
        "INFO" `
        "PACKAGE" `
        (
            "Validated File: " +
            $File
        )
}

# =========================================================
# LOAD ARTIFACTS
# =========================================================

$RecoveryManifest =
    Get-Content `
        $ManifestPath `
        -Raw |
    ConvertFrom-Json `
        

$ReconstructionPlan =
    Get-Content `
        $PlanPath `
        -Raw |
    ConvertFrom-Json `
        

$ReconstructionRuntime =
    Get-Content `
        $RuntimeFilePath `
        -Raw |
    ConvertFrom-Json `
        

$FinalReadiness =
    Get-Content `
        $ReadinessPath `
        -Raw |
    ConvertFrom-Json `
        

# =========================================================
# RUNTIME REGISTRATION
# =========================================================

$Runtime["Publication"] =
    $Publication

$Runtime["RecoveryManifest"] =
    $RecoveryManifest

$Runtime["ReconstructionPlan"] =
    $ReconstructionPlan

$Runtime["ReconstructionRuntime"] =
    $ReconstructionRuntime

$Runtime["FinalReadiness"] =
    $FinalReadiness

$Runtime["RecoveryMedia"] =
    $RecoveryMedia

$Runtime["RecoveryRootPath"] =
    $RecoveryMedia.root

$Runtime["RecoveryRuntimePath"] =
    $RecoveryMedia.runtime_path

$Runtime["RecoveryArchivesPath"] =
    $RecoveryMedia.archives_path

$Runtime["RecoveryReportsPath"] =
    $RecoveryMedia.reports_path

$Runtime["BackupExecutionId"] =
    $Publication.backup_execution_id

$Runtime["ReconstructionExecutionId"] =
    $Publication.reconstruction_execution_id

# =========================================================
# SUMMARY
# =========================================================

Log `
    "INFO" `
    "PACKAGE" `
    (
        "Backup Execution ID: " +
        $Publication.backup_execution_id
    )

Log `
    "INFO" `
    "PACKAGE" `
    (
        "Recovery Root: " +
        $RecoveryMedia.root
    )

Write-Signal `
    -Status "running" `
    -Progress 15 `
    -Note @{
        stage = "package_loading"
        message = "Recovery package loaded"
        recovery_ready = $true
        recovery_certified = $true
    }

Log `
    "INFO" `
    "PACKAGE" `
    "Recovery package loading completed"

# =========================================================
# BLOCK 03
# EXECUTION READINESS VALIDATION
# =========================================================

Log `
    "INFO" `
    "READINESS" `
    "Execution readiness validation started"

$Publication =
    $Runtime["Publication"]

$RecoveryManifest =
    $Runtime["RecoveryManifest"]

$ReconstructionPlan =
    $Runtime["ReconstructionPlan"]

$ReconstructionRuntime =
    $Runtime["ReconstructionRuntime"]

# =========================================================
# REQUIRED OBJECTS
# =========================================================

if (
    $null -eq $Publication
) {

    throw `
        "PUBLICATION_NOT_LOADED"
}

if (
    $null -eq $RecoveryManifest
) {

    throw `
        "MANIFEST_NOT_LOADED"
}

if (
    $null -eq $ReconstructionPlan
) {

    throw `
        "PLAN_NOT_LOADED"
}

if (
    $null -eq $ReconstructionRuntime
) {

    throw `
        "RUNTIME_NOT_LOADED"
}

# =========================================================
# VALIDATION RESULTS
# =========================================================

$Checks =
    @()

function Add-ReadinessCheck {

    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Details
    )

    $script:Checks +=
        [ordered]@{

            name =
                $Name

            passed =
                $Passed

            details =
                $Details
        }

    if ($Passed) {

        Log `
            "INFO" `
            "READINESS" `
            (
                "PASSED: " +
                $Name
            )
    }
    else {

        Log `
            "ERROR" `
            "READINESS" `
            (
                "FAILED: " +
                $Name
            )
    }
}

# =========================================================
# CHECK 01
# =========================================================

Add-ReadinessCheck `
    -Name "recovery_ready" `
    -Passed (
        $Publication.recovery_ready -eq $true
    ) `
    -Details (
        [string]$Publication.recovery_ready
    )

# =========================================================
# CHECK 02
# =========================================================

Add-ReadinessCheck `
    -Name "recovery_certified" `
    -Passed (
        $Publication.recovery_certified -eq $true
    ) `
    -Details (
        [string]$Publication.recovery_certified
    )

# =========================================================
# CHECK 03
# =========================================================

Add-ReadinessCheck `
    -Name "plan_valid" `
    -Passed (
        $null -ne $ReconstructionPlan
    ) `
    -Details "plan_loaded"

# =========================================================
# CHECK 04
# RUNTIME READY
# =========================================================

Add-ReadinessCheck `
    -Name "runtime_ready" `
    -Passed (
        $ReconstructionRuntime.
            runtime_status -eq "READY"
    ) `
    -Details (
        $ReconstructionRuntime.
            runtime_status
    )

# =========================================================
# CHECK 05
# =========================================================

$RecoveryMedia =
    $Runtime["RecoveryMedia"]

$ArchiveCount =
    @(
        Get-ChildItem `
            -Path $RecoveryMedia.archives_path `
            -File `
            -Filter "*.zip"
    ).Count

Add-ReadinessCheck `
    -Name "archives_present" `
    -Passed (
        $ArchiveCount -gt 0
    ) `
    -Details (
        [string]$ArchiveCount
    )

# =========================================================
# CHECK 06
# EXECUTION MODE
# =========================================================

$ExecutionMode =
    "WINRE_TRANSITION_TEST"

Add-ReadinessCheck `
    -Name "execution_mode" `
    -Passed $true `
    -Details $ExecutionMode

# =========================================================
# FINAL RESULT
# =========================================================

$FailedChecks =
    @(
        $Checks |
        Where-Object {
            $_.passed -eq $false
        }
    )

$ExecutionReady =
    (
        $FailedChecks.Count -eq 0
    )

$ReadinessValidation =
    [ordered]@{

        backup_execution_id =
            $Runtime.
                BackupExecutionId

        execution_ready =
            $ExecutionReady

        execution_mode =
            $ExecutionMode

        total_checks =
            $Checks.Count

        passed_checks =
            (
                @(
                    $Checks |
                    Where-Object {
                        $_.passed
                    }
                ).Count
            )

        failed_checks =
            $FailedChecks.Count

        checks =
            $Checks

        validated_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

# =========================================================
# REGISTER RUNTIME
# =========================================================

$Runtime["ReadinessValidation"] =
    $ReadinessValidation

# =========================================================
# FAILURE
# =========================================================

if (
    -not $ExecutionReady
) {

    Log `
        "ERROR" `
        "READINESS" `
        (
            "Failed Checks: " +
            $FailedChecks.Count
        )

    throw `
        "RECOVERY_EXECUTION_NOT_READY"
}

# =========================================================
# SIGNAL
# =========================================================

Write-Signal `
    -Status "running" `
    -Progress 25 `
    -Note @{
        stage = "readiness_validation"
        message = "Execution readiness validated."
        execution_ready =
            $ExecutionReady
        execution_mode =
            $ExecutionMode
    }

Log `
    "INFO" `
    "READINESS" `
    (
        "Checks Passed: " +
        $Checks.Count
    )

Log `
    "INFO" `
    "READINESS" `
    "Execution readiness validation completed"

# =========================================================
# BLOCK 04
# EXECUTION STRATEGY VALIDATION
# =========================================================

Log `
    "INFO" `
    "EXECUTION" `
    "Execution strategy validation started"

$Publication =
    $Runtime["Publication"]

$RecoveryManifest =
    $Runtime["RecoveryManifest"]

$ReconstructionPlan =
    $Runtime["ReconstructionPlan"]

$ReconstructionRuntime =
    $Runtime["ReconstructionRuntime"]

$ReadinessValidation =
    $Runtime["ReadinessValidation"]

# =========================================================
# REQUIRED OBJECTS
# =========================================================

if ($null -eq $Publication) {
    throw "PUBLICATION_NOT_AVAILABLE"
}

if ($null -eq $RecoveryManifest) {
    throw "RECOVERY_MANIFEST_NOT_AVAILABLE"
}

if ($null -eq $ReconstructionPlan) {
    throw "RECONSTRUCTION_PLAN_NOT_AVAILABLE"
}

if ($null -eq $ReconstructionRuntime) {
    throw "RECONSTRUCTION_RUNTIME_NOT_AVAILABLE"
}

# =========================================================
# EXECUTION GATE
# =========================================================

$ExecutionAllowed = $false

if (
    $null -ne $Publication.execution_gate
) {

    $ExecutionAllowed =
        $Publication.
            execution_gate.
            execution_allowed
}

if (
    -not $ExecutionAllowed
) {

    throw `
        "EXECUTION_NOT_ALLOWED"
}

# =========================================================
# EXECUTION MODE
# =========================================================

$ExecutionMode =
    "WINRE_TRANSITION_TEST"

# Future:
#
# WINRE_TRANSITION_TEST
# WINRE_RESET_TEST
# FULL_REBUILD
# USB_RECOVERY
# CLOUD_RECOVERY

# =========================================================
# STRATEGY OBJECT
# =========================================================

$ExecutionValidation =
    [ordered]@{

        execution_allowed =
            $ExecutionAllowed

        execution_mode =
            $ExecutionMode

        recovery_ready =
            $Publication.
                recovery_ready

        recovery_certified =
            $Publication.
                recovery_certified

        runtime_status =
            $ReconstructionRuntime.
                runtime_status

        source_worker =
            $Publication.worker

        backup_execution_id =
            $Publication.
                backup_execution_id

        reconstruction_execution_id =
            $Publication.
                reconstruction_execution_id

        validated_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

# =========================================================
# REGISTER RUNTIME
# =========================================================

$Runtime["ExecutionValidation"] =
    $ExecutionValidation

# =========================================================
# SIGNAL
# =========================================================

Write-Signal `
    -Status "running" `
    -Progress 40 `
    -Note @{
        stage = "execution_validation"
        message = "Execution strategy validated."
        execution_mode =
            $ExecutionMode
    }

# =========================================================
# LOGGING
# =========================================================

Log `
    "INFO" `
    "EXECUTION" `
    (
        "Execution Allowed: " +
        $ExecutionAllowed
    )

Log `
    "INFO" `
    "EXECUTION" `
    (
        "Execution Mode: " +
        $ExecutionMode
    )

Log `
    "INFO" `
    "EXECUTION" `
    (
        "Recovery Certified: " +
        $Publication.recovery_certified
    )

Log `
    "INFO" `
    "EXECUTION" `
    "Execution strategy validation completed"

# =========================================================
# BLOCK 05
# PREPARE EXECUTION TRANSITION
# =========================================================

Log `
    "INFO" `
    "TRANSITION" `
    "Execution transition preparation started"

$ExecutionValidation =
    $Runtime["ExecutionValidation"]

if (
    $null -eq $ExecutionValidation
) {

    throw `
        "EXECUTION_STRATEGY_NOT_AVAILABLE"
}

if (
    -not $ExecutionValidation.execution_allowed
) {

    throw `
        "EXECUTION_NOT_ALLOWED"
}

$RecoveryMedia =
    $Runtime["RecoveryMedia"]

$Publication =
    $Runtime["Publication"]

# =========================================================
# SESSION
# =========================================================

$RecoverySessionId =
    [guid]::NewGuid().
        ToString()

$TransitionPath =
    Join-Path `
        $env:ProgramData `
        "ElmidorNexus\Recovery"

if (
    -not (
        Test-Path `
            $TransitionPath
    )
) {

    New-Item `
        -ItemType Directory `
        -Path $TransitionPath `
        -Force |
    Out-Null
}

# =========================================================
# RECOVERY SESSION
# =========================================================

$RecoverySession =
    [ordered]@{

        recovery_session_id =
            $RecoverySessionId

        backup_execution_id =
            $Publication.
                backup_execution_id

        reconstruction_execution_id =
            $Publication.
                reconstruction_execution_id

        execution_mode =
            $ExecutionValidation.
                execution_mode

        recovery_media =
            $RecoveryMedia.root

        status =
            "PREPARED"

        reboot_required =
            $true

        wipe_enabled =
            $false

        reset_enabled =
            $false

        rebuild_enabled =
            $false

        prepared_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

# =========================================================
# SAVE SESSION
# =========================================================

$RecoverySessionPath =
    Join-Path `
        $TransitionPath `
        "recovery.session.json"

$RecoverySession |
    ConvertTo-Json  |
    Set-Content `
        -Path $RecoverySessionPath `
        -Encoding UTF8

# =========================================================
# TRANSITION PLAN
# =========================================================

$TransitionPlan =
    [ordered]@{

        recovery_session_id =
            $RecoverySessionId

        execution_mode =
            $ExecutionValidation.
                execution_mode

        transition_type =
            "WINRE_TRANSITION_TEST"

        reboot_required =
            $true

        next_action =
            "BOOT_TO_WINRE"

        wipe_enabled =
            $false

        reset_enabled =
            $false

        rebuild_enabled =
            $false

        phase =
            "PREPARED"

        prepared_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

$TransitionPlanPath =
    Join-Path `
        $TransitionPath `
        "transition.plan.json"

$TransitionPlan |
    ConvertTo-Json  |
    Set-Content `
        -Path $TransitionPlanPath `
        -Encoding UTF8

# =========================================================
# REGISTER
# =========================================================

$Runtime["RecoverySession"] =
    $RecoverySession

$Runtime["RecoverySessionPath"] =
    $RecoverySessionPath

$Runtime["TransitionPlan"] =
    $TransitionPlan

$Runtime["TransitionPlanPath"] =
    $TransitionPlanPath

# =========================================================
# SIGNAL
# =========================================================

Write-Signal `
    -Status "running" `
    -Progress 55 `
    -Note @{
        stage = "transition_preparation"
        message = "Execution transition prepared."
        recovery_session_id =
            $RecoverySessionId
    }

# =========================================================
# LOGGING
# =========================================================

Log `
    "INFO" `
    "TRANSITION" `
    (
        "Recovery Session: " +
        $RecoverySessionId
    )

Log `
    "INFO" `
    "TRANSITION" `
    (
        "Execution Mode: " +
        $ExecutionValidation.execution_mode
    )

Log `
    "INFO" `
    "TRANSITION" `
    (
        "Transition Plan: " +
        $TransitionPlanPath
    )

Log `
    "INFO" `
    "TRANSITION" `
    "Execution transition preparation completed"
# =========================================================
# BLOCK 06
# DECISION ENGINE
# =========================================================

Log `
    "INFO" `
    "DECISION" `
    "Recovery decision engine started"

$ExecutionValidation =
    $Runtime["ExecutionValidation"]

$TransitionPlan =
    $Runtime["TransitionPlan"]

$RecoverySession =
    $Runtime["RecoverySession"]

$Publication =
    $Runtime["Publication"]

# =========================================================
# REQUIRED OBJECTS
# =========================================================

if ($null -eq $ExecutionValidation) {

    throw `
        "EXECUTION_VALIDATION_NOT_AVAILABLE"
}

if ($null -eq $TransitionPlan) {

    throw `
        "TRANSITION_PLAN_NOT_AVAILABLE"
}

if ($null -eq $RecoverySession) {

    throw `
        "RECOVERY_SESSION_NOT_AVAILABLE"
}

if ($null -eq $Publication) {

    throw `
        "PUBLICATION_NOT_AVAILABLE"
}

# =========================================================
# DECISION FLAGS
# =========================================================

$RecoveryAllowed =
    $true

$DecisionReasons =
    @()

# =========================================================
# EXECUTION ALLOWED
# =========================================================

if (
    -not (
        $ExecutionValidation.
            execution_allowed
    )
) {

    $RecoveryAllowed =
        $false

    $DecisionReasons +=
        "Execution not allowed"
}

# =========================================================
# CERTIFICATION
# =========================================================

if (
    -not (
        $Publication.
            recovery_certified
    )
) {

    $RecoveryAllowed =
        $false

    $DecisionReasons +=
        "Recovery not certified"
}

# =========================================================
# SESSION STATUS
# =========================================================

if (
    $RecoverySession.
        status -ne "PREPARED"
) {

    $RecoveryAllowed =
        $false

    $DecisionReasons +=
        "Recovery session not prepared"
}

# =========================================================
# TRANSITION TYPE
# =========================================================

if (
    $TransitionPlan.
        transition_type -ne
        "WINRE_TRANSITION_TEST"
) {

    $RecoveryAllowed =
        $false

    $DecisionReasons +=
        "Invalid transition type"
}

# =========================================================
# DECISION MODEL
# =========================================================

$DecisionModel =
    [ordered]@{

        execution_id =
            $ExecutionId

        recovery_session_id =
            $RecoverySession.
                recovery_session_id

        execution_mode =
            "WINRE_VALIDATION"

        recovery_allowed =
            $RecoveryAllowed

        reasons =
            $DecisionReasons

        next_stage =
            if ($RecoveryAllowed) {

                "boot_to_winre"

            } else {

                "abort"
            }

        decided_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

# =========================================================
# REGISTER
# =========================================================

$Runtime["DecisionModel"] =
    $DecisionModel

# =========================================================
# LOGGING
# =========================================================

if ($RecoveryAllowed) {

    Log `
        "INFO" `
        "DECISION" `
        "Recovery approved for WinRE transition"
}
else {

    Log `
        "ERROR" `
        "DECISION" `
        (
            $DecisionReasons -join "; "
        )

    throw `
        "RECOVERY_DECISION_DENIED"
}

# =========================================================
# SIGNAL
# =========================================================

Write-Signal `
    -Status "running" `
    -Progress 75 `
    -Note @{
        stage = "decision"
        recovery_allowed =
            $RecoveryAllowed
        next_stage =
            "boot_to_winre"
    }

Log `
    "INFO" `
    "DECISION" `
    "Recovery decision engine completed"

# =========================================================
# BLOCK 07
# PREPARE WINRE BOOT
# =========================================================

Log `
    "INFO" `
    "BOOT_PREP" `
    "WinRE boot preparation started"

$DecisionModel =
    $Runtime["DecisionModel"]

if (
    $null -eq $DecisionModel
) {

    throw `
        "DECISION_MODEL_NOT_AVAILABLE"
}

if (
    -not (
        $DecisionModel.
            recovery_allowed
    )
) {

    throw `
        "RECOVERY_EXECUTION_NOT_APPROVED"
}

# =========================================================
# EXECUTION MODE
# =========================================================

if (
    $DecisionModel.
        execution_mode -ne
    "WINRE_VALIDATION"
) {

    throw `
        "INVALID_EXECUTION_MODE"
}

# =========================================================
# PREPARE BOOT TO WINRE
# =========================================================

try {

    $BootOutput =
        reagentc `
            /boottore 2>&1

    $BootPrepared =
        $true

    Log `
        "INFO" `
        "BOOT_PREP" `
        "WinRE boot sequence registered"
}
catch {

    $BootPrepared =
        $false

    Log `
        "ERROR" `
        "BOOT_PREP" `
        $_.Exception.Message

    throw `
        "WINRE_BOOT_PREPARATION_FAILED"
}

# =========================================================
# BUILD BOOT PACKAGE
# =========================================================

$BootPreparation =
    [ordered]@{

        execution_id =
            $ExecutionId

        recovery_execution_id =
            $Runtime[
                "RecoveryExecutionId"
            ]

        execution_mode =
            $DecisionModel.
                execution_mode

        boot_prepared =
            $BootPrepared

        reboot_required =
            $true

        target_environment =
            "Windows Recovery Environment"

        winre_transition_ready =
            $BootPrepared

        boot_response =
            (
                $BootOutput |
                Out-String
            )

        prepared_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

# =========================================================
# EXPORT FILE
# =========================================================
Write-Host ""
Write-Host "RecoveryRuntimePath:"
Write-Host $Runtime["RecoveryRuntimePath"]
Write-Host ""
$BootPreparationPath =
    Join-Path `
        $Runtime["RecoveryRuntimePath"] `
        "boot.preparation.json"

$BootPreparation |
    ConvertTo-Json `
         |
    Set-Content `
        -Path $BootPreparationPath `
        -Encoding UTF8

# =========================================================
# REGISTER
# =========================================================

$Runtime["BootPreparation"] =
    $BootPreparation

$Runtime["BootPreparationPath"] =
    $BootPreparationPath

# =========================================================
# LOGGING
# =========================================================

Log `
    "INFO" `
    "BOOT_PREP" `
    (
        "Boot Package: " +
        $BootPreparationPath
    )

Log `
    "INFO" `
    "BOOT_PREP" `
    "WinRE transition prepared"

# =========================================================
# SIGNAL
# =========================================================

Write-Signal `
    -Status "running" `
    -Progress 85 `
    -Note @{
        stage = "boot_preparation"
        reboot_required = $true
        target = "WinRE"
        prepared = $BootPrepared
    }

Log `
    "INFO" `
    "BOOT_PREP" `
    "WinRE boot preparation completed"

# =========================================================
# BLOCK 08
# EXECUTE WINRE TRANSITION
# VERSION 1 TEST
# =========================================================

Log `
    "INFO" `
    "EXECUTE" `
    "WinRE transition execution started"

# =========================================================
# LOAD OBJECTS
# =========================================================

$BootPreparation =
    $Runtime["BootPreparation"]

$BootPreparationPath =
    $Runtime["BootPreparationPath"]

$RecoverySession =
    $Runtime["RecoverySession"]

if (
    $null -eq $BootPreparation
) {

    throw `
        "BOOT_PREPARATION_NOT_AVAILABLE"
}

if (
    -not (
        Test-Path `
            $BootPreparationPath
    )
) {

    throw `
        "BOOT_PREPARATION_FILE_MISSING"
}

# =========================================================
# AUTHORIZATION
# =========================================================

$AuthorizationPath =
    Join-Path `
        $env:ProgramData `
        "ElmidorNexus\Recovery\recovery.authorized.json"

$Authorization =
    [ordered]@{

        execution_id =
            $ExecutionId

        recovery_session_id =
            $RecoverySession.
                recovery_session_id

        execution_mode =
            "WINRE_TRANSITION_TEST"

        action =
            "BOOT_TO_WINRE"

        authorized =
            $true

        authorized_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

$Authorization |
    ConvertTo-Json |
    Set-Content `
        -Path $AuthorizationPath `
        -Encoding UTF8

Log `
    "INFO" `
    "EXECUTE" `
    (
        "Authorization File: " +
        $AuthorizationPath
    )

# =========================================================
# EXECUTION REPORT
# =========================================================

$ExecutionReport =
    [ordered]@{

        execution_id =
            $ExecutionId

        recovery_session_id =
            $RecoverySession.
                recovery_session_id

        execution_mode =
            "WINRE_TRANSITION_TEST"

        phase =
            "BOOT_TO_WINRE"

        status =
            "READY_FOR_REBOOT"

        created_at =
            (
                Get-Date
            ).
                ToUniversalTime().
                ToString("o")
    }

$ExecutionReportPath =
    Join-Path `
        $Runtime["RecoveryRuntimePath"] `
        "winre.execution.json"

$ExecutionReport |
    ConvertTo-Json |
    Set-Content `
        -Path $ExecutionReportPath `
        -Encoding UTF8

Log `
    "INFO" `
    "EXECUTE" `
    (
        "Execution Report: " +
        $ExecutionReportPath
    )

# =========================================================
# BOOT TO WINRE
# =========================================================

Log `
    "INFO" `
    "EXECUTE" `
    "Registering next boot to WinRE"

reagentc /boottore

if (
    $LASTEXITCODE -ne 0
) {

    throw `
        "WINRE_BOOT_REGISTRATION_FAILED"
}

Log `
    "INFO" `
    "EXECUTE" `
    "WinRE boot registration successful"

# =========================================================
# FINAL SIGNAL
# =========================================================

Write-Signal `
    -Status "completed" `
    -Progress 100 `
    -Note @{
        stage = "execute"
        message = "System will reboot into WinRE."
        execution_mode = "WINRE_TRANSITION_TEST"
    }

# =========================================================
# FINAL LOG
# =========================================================

Log `
    "INFO" `
    "EXECUTE" `
    "System rebooting into Windows Recovery Environment"

Start-Sleep `
    -Seconds 5

# =========================================================
# REBOOT
# =========================================================

shutdown.exe `
    /r `
    /t 0 `
    /f
