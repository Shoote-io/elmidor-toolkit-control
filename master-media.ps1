# ==============================================
 # Media processing OS (Distributed worker system)
# ==============================================
param(
    $CommandType,
    $CommandId
)
$InputPath = "E:\elmidor_group_site\En-bassarts_deur\99_INBOX"
Write-Host "INPUT PATH =" $InputPath
$ErrorActionPreference = "Continue"

# ---------------- GLOBAL CONFIG ----------------
$POLL_INTERVAL = 15
$JOB_TIMEOUT   = 3600
$MAX_RETRIES   = 2

$ROOT = Get-Location
$SYS  = "00_SYSTEM"
$DEBUG_MODE = $true

$global:DepartmentState = @{
    "AUDIO" = "IDLE"
    "VIDEO" = "IDLE"
}

$Scripts = @{
    "AUDIO" = @{
        discovery     = ".\01_audio_discovery.ps1"
        analyzer      = ".\00_audio_analyzer.ps1"
        distribution  = ".\99_audio_distributor.ps1"
    }
    "VIDEO" = @{
        discovery     = ".\01_video_discovery.ps1"
        analyzer      = ".\00_video_analyzer.ps1"
        distribution  = ".\99_video_distributor.ps1"
    }
}

$LOG_DIR    = Join-Path $SYS "logs"

New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null
# ADDED: force absolute signal dir
$SIGNAL_DIR = Join-Path $PSScriptRoot "00_SYSTEM\signals"

# ADDED: ensure directory exists
if (-not (Test-Path $SIGNAL_DIR)) {
    New-Item -ItemType Directory -Path $SIGNAL_DIR -Force | Out-Null
}

Write-Host "LOCK PATH FULL: $SIGNAL_DIR"
# ---------------- LOGGING ----------------
function Write-Log($msg, $level="INFO") {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts][$level] $msg"
    Write-Host $line
    Add-Content (Join-Path $LOG_DIR "master_4life.md") $line
}

# ---------------- JOB REGISTRY ----------------
$global:RunningJobs = @{}

function New-JobId { 
    [guid]::NewGuid().ToString() 
}

# ------------- STAGE STATE CHECK ----------------
function Is-Stage-Running($Department, $Stage) {

    return ($global:RunningJobs.Values | Where-Object {
        $_.Department -eq $Department -and $_.Stage -eq $Stage
    }).Count -gt 0
}

# ---------------- LOCK HELPERS ----------------
function Get-LockPath($Department, $Stage) {
    return Join-Path $SIGNAL_DIR "$($Department)_$($Stage).lock"
}
function Try-AcquireLock($LockPath) {
    try {
        # ADDED: atomic lock (prevents race condition)
        $fs = [System.IO.File]::Open($LockPath, 'CreateNew')
        $fs.Close()
        return $true
    }
    catch {
        return $false
    }
}

function Release-Lock($LockPath) {
    if (Test-Path $LockPath) {
        Remove-Item $LockPath -ErrorAction SilentlyContinue
    }
}
function Cleanup-StaleLocks {

    $locks = Get-ChildItem $SIGNAL_DIR -Filter *.lock -ErrorAction SilentlyContinue

    foreach ($lock in $locks) {

        $inUse = $false

        foreach ($job in $global:RunningJobs.Values) {
            if ($job.LockPath -eq $lock.FullName) {
                $inUse = $true
                break
            }
        }

        if (-not $inUse) {
            Write-Log "REMOVING STALE LOCK: $($lock.Name)" "WARN"
            Remove-Item $lock.FullName -Force -ErrorAction SilentlyContinue
        }
    }
}

# ---------------- LAUNCH WORKER ----------------
function Launch-Worker {
    param(
        [string]$Department,
        [string]$Stage,
        [string]$Script,
        [string]$SignalFile
    )

    $lockPath = Get-LockPath $Department $Stage

    # ADDED: prevent duplicate launch (memory + file lock)
    if (Is-Stage-Running $Department $Stage) {
        Write-Log "SKIP [$Department::$Stage] already running (memory)"
        return
    }

    if (!(Try-AcquireLock $lockPath)) {

    # ADDED: attempt cleanup if lock invalid
    if (-not (Test-Path $lockPath)) {
        Write-Log "LOCK FAIL but file not found → retrying" "WARN"
        
        if (Try-AcquireLock $lockPath) {
            Write-Log "LOCK RECOVERED [$Department::$Stage]"
        } else {
            Write-Log "SKIP [$Department::$Stage] lock still failing"
            return
        }
    }
    else {
        Write-Log "SKIP [$Department::$Stage] already locked (file)"
        return
    }
}

    if (!(Test-Path $Script)) {
        Write-Log "Missing Script: $Script" "ERROR"
        Release-Lock $lockPath # ADDED: cleanup lock if fail
        return
    }

    $jobId = New-JobId

    try {
        if ($DEBUG_MODE) {

            # FIXED: proper argument handling
            $proc = Start-Process powershell `
                -ArgumentList @(
                    "-ExecutionPolicy", "Bypass",
                    "-File", $Script,
                    "-InputPath", $InputPath
                ) `
                -PassThru
        } 
        else {

            $proc = Start-Process powershell `
                -ArgumentList @(
                    "-ExecutionPolicy", "Bypass",
                    "-File", $Script,
                    "-InputPath", $InputPath
                ) `
                -WindowStyle Hidden `
                -PassThru
        }

        # ADDED: validate process actually started
        if (-not $proc -or -not $proc.Id) {
            throw "Process failed to start"
        }

        $global:RunningJobs[$jobId] = @{
            Department = $Department
            Stage      = $Stage
            Script     = $Script
            Signal     = $SignalFile
            ProcessId  = $proc.Id
            StartTime  = Get-Date
            Retries    = 0
            LockPath   = $lockPath # ADDED: track lock
        }

        Write-Log "LAUNCHED [$Department::$Stage] PID=$($proc.Id)"
    }
    catch {
        # ADDED: ensure lock is not left behind on failure
        Release-Lock $lockPath
        Write-Log "FAILED TO LAUNCH [$Department::$Stage] $_" "ERROR"
    }
}

# ------------ WORKER CHECK FLAGS ----------------
# ADDED: kept as-is but cleaner formatting
$global:WorkersChecked = @{
    "AUDIO" = $false
    "VIDEO" = $false
}
#-------------- RUN-DEPARTMENT -------------------
function Run-Department {
    param([string]$Dept)

    $state = $global:DepartmentState[$Dept]

    switch ($state) {

        "IDLE" {

            Launch-Worker $Dept "discovery" $Scripts[$Dept].discovery "$($Dept.ToLower())_discovery.done"

            # ADDED: only move state if launch actually happened
            if (Is-Stage-Running $Dept "discovery") {
                $global:DepartmentState[$Dept] = "DISCOVERY"
            }
        }

        "DISCOVERY_DONE" {

            Launch-Worker $Dept "analyzer" $Scripts[$Dept].analyzer "$($Dept.ToLower())_analyzer.done"

            # ADDED
            if (Is-Stage-Running $Dept "analyzer") {
                $global:DepartmentState[$Dept] = "ANALYZER"
            }
        }

        "ANALYZER_DONE" {

            Launch-Worker $Dept "distribution" $Scripts[$Dept].distribution "$($Dept.ToLower())_distribution.done"

            # ADDED
            if (Is-Stage-Running $Dept "distribution") {
                $global:DepartmentState[$Dept] = "DISTRIBUTION"
            }
        }

        "DISTRIBUTION_DONE" {

            $global:DepartmentState[$Dept] = "WORKERS"

            # ADDED: reset check flag clean
            $global:WorkersChecked[$Dept] = $false
        }

        "WORKERS" {

            if (Get-Command Is-Department-Idle -ErrorAction SilentlyContinue) {

                if (Is-Department-Idle $Dept) {
                    $global:DepartmentState[$Dept] = "IDLE"
                }
            }
        }
    }
}
# ---------------- CHECK WORKERS ----------------
function Check-Workers {

    foreach ($jobId in @($global:RunningJobs.Keys)) {

        # ADDED: safe access
        if (-not $global:RunningJobs.ContainsKey($jobId)) { continue }

        $job = $global:RunningJobs[$jobId]

        $signalPath = Join-Path $SIGNAL_DIR $job.Signal

        # ADDED: reliable process check
        $procAlive = $null
        if ($job.ProcessId) {
            try {
                $procAlive = Get-Process -Id $job.ProcessId -ErrorAction Stop
            }
            catch {
                $procAlive = $null
            }
        }

        if ($procAlive) {
            Write-Log "STILL RUNNING [$($job.Department)::$($job.Stage)] PID=$($job.ProcessId)"
        }

        # -------- DONE VALIDATION ----------------
        # ADDED: strict condition (NO MORE FALSE DONE)
        $isDone = (-not $procAlive) -and (Test-Path $signalPath)

        if ($isDone) {

            Write-Log "DONE [$($job.Department)::$($job.Stage)]"

            switch ($job.Stage) {

                "discovery" {
                    $global:DepartmentState[$job.Department] = "DISCOVERY_DONE"
                }

                "analyzer" {
                    $global:DepartmentState[$job.Department] = "ANALYZER_DONE"
                }

                "distribution" {
                    $global:DepartmentState[$job.Department] = "DISTRIBUTION_DONE"
                }

                "audit" {
                    $global:DepartmentState[$job.Department] = "IDLE"
                }

                default { }
            }

            # ADDED: cleanup signal AFTER confirmed done
            if (Test-Path $signalPath) {
                Remove-Item $signalPath -Force -ErrorAction SilentlyContinue
            }

            # ADDED: release lock linked to job
            if ($job.LockPath) {
                if (Test-Path $job.LockPath) {
                    Remove-Item $job.LockPath -Force -ErrorAction SilentlyContinue
                }
            }

            # ADDED: remove job safely
            if ($global:RunningJobs.ContainsKey($jobId)) {
                $global:RunningJobs.Remove($jobId)
            }

            continue
        }

        # -------- CRASH DETECTION ----------------
        # ADDED: process dead BUT no signal = crash
        if (-not $procAlive -and -not (Test-Path $signalPath)) {

            Write-Log "CRASH DETECTED [$($job.Department)::$($job.Stage)]" "ERROR"

            # ADDED: retry logic safe
            if ($job.Retries -lt 2) {

                $job.Retries++

                Write-Log "RETRY [$($job.Department)::$($job.Stage)] Attempt=$($job.Retries)"

                # ADDED: release old lock before retry
                if ($job.LockPath) {
                    Remove-Item $job.LockPath -Force -ErrorAction SilentlyContinue
                }

                # relaunch
                Launch-Worker $job.Department $job.Stage $job.Script $job.Signal
            }
            else {

                Write-Log "FAILED AFTER MAX RETRIES [$($job.Department)::$($job.Stage)]" "ERROR"

                # ADDED: cleanup lock even on failure
                if ($job.LockPath) {
                    Remove-Item $job.LockPath -Force -ErrorAction SilentlyContinue
                }

                if ($global:RunningJobs.ContainsKey($jobId)) {
                    $global:RunningJobs.Remove($jobId)
                }
            }

            continue
        }
  # ---------------- TIMEOUT ----------------

# ADDED: validate StartTime
if (-not $job["StartTime"] -or -not ($job["StartTime"] -is [datetime])) {
    Write-Log "INVALID StartTime [$($job.Department)::$($job.Stage)] - fixing" "WARN"
   $job["StartTime"] = Get-Date
}
        $elapsed = (Get-Date) - $job["StartTime"]

        if ($elapsed.TotalSeconds -gt $JOB_TIMEOUT) {

            Write-Log "TIMEOUT [$($job.Department)::$($job.Stage)]" "WARN"

            try {
                if ($job.ProcessId) {
                    Stop-Process -Id $job.ProcessId -Force -ErrorAction Stop
                }
            } catch {}

            # ADDED: release lock before retry/fail
            if ($job.LockPath) {
                Remove-Item $job.LockPath -Force -ErrorAction SilentlyContinue
            }

            # ADDED: retry logic aligned with crash system
            if ($job.Retries -lt $MAX_RETRIES) {

                $job.Retries++

                Write-Log "RETRY (TIMEOUT) [$($job.Department)::$($job.Stage)] Attempt=$($job.Retries)"

                # ADDED: relaunch safely via existing system
                Launch-Worker $job.Department $job.Stage $job.Script $job.Signal

                # ADDED: remove old job (new one will be tracked separately)
                if ($global:RunningJobs.ContainsKey($jobId)) {
                    $global:RunningJobs.Remove($jobId)
                }

                continue
            }
            else {

                Write-Log "FAILED AFTER MAX RETRIES (TIMEOUT) [$($job.Department)::$($job.Stage)]" "ERROR"

                # ADDED: cleanup lock
                if ($job.LockPath) {
                    Remove-Item $job.LockPath -Force -ErrorAction SilentlyContinue
                }

                # ADDED: safe remove
                if ($global:RunningJobs.ContainsKey($jobId)) {
                    $global:RunningJobs.Remove($jobId)
                }

                continue
            }
        }
        # ADDED: stale signal protection (signal exists but process still alive)
        if ($procAlive -and (Test-Path $signalPath)) {
            Write-Log "WARNING: STALE SIGNAL [$($job.Department)::$($job.Stage)] ignored"
        }
    }
}

# ------------ DEPARTMENT STATUS ----------------
function Is-Department-Idle($Department) {

    # ADDED: safe guard
    if (!$global:RunningJobs -or $global:RunningJobs.Count -eq 0) { return $true }

    $active = $global:RunningJobs.Values | Where-Object {
        $_.Department -eq $Department
    }

    return ($active.Count -eq 0)
}

# ---------------- AUDIO ENGINE ----------------
$global:WorkersChecked["AUDIO"] = $true  # ADDED

function Run-AudioWorkers {

    if ($global:DepartmentState["AUDIO"] -ne "WORKERS") { return }

    # ADDED: snapshot filesystem once (avoid race + cost)
    $compressFiles = @(Get-ChildItem "AUDIO_STAGE_COMPRESS" -File -ErrorAction SilentlyContinue)
    $repairFiles   = @(Get-ChildItem "AUDIO_STAG_REPAIR" -File -ErrorAction SilentlyContinue)

    # ADDED: mark that real work happened
if ($activityDetected) {
    $global:WorkersChecked["AUDIO"] = $true
}

    if ($compressFiles.Count -gt 0) {
        Launch-Worker "AUDIO" "compress" ".\02_audio_compress.ps1" "audio_compress.done"
        $activityDetected = $true
    }

    if ($repairFiles.Count -gt 0) {
        Launch-Worker "AUDIO" "repair" ".\03_audio_repair.ps1" "audio_repair.done"
        $activityDetected = $true
    }

    # ADDED: clean boolean logic (based on snapshot)
    $hasFiles = ($compressFiles.Count -gt 0) -or ($repairFiles.Count -gt 0)

    if (
        -not $hasFiles -and
        (Is-Department-Idle "AUDIO") -and
        $global:WorkersChecked["AUDIO"]
    ) {
        $global:DepartmentState["AUDIO"] = "IDLE"
    }
}

# ---------------- VIDEO ENGINE ----------------
$global:WorkersChecked["VIDEO"] = $true  # ADDED

function Run-VideoWorkers {

    if ($global:DepartmentState["VIDEO"] -ne "WORKERS") { return }

    # ADDED: snapshot filesystem once
    $compressFiles = @(Get-ChildItem "VIDEO_STAGE_COMPRESS" -Filter *.mp4 -File -ErrorAction SilentlyContinue)
    $speedFiles    = @(Get-ChildItem "VIDEO_STAGE_SPEED" -Filter *.mp4 -File -ErrorAction SilentlyContinue)
    $repairFiles   = @(Get-ChildItem "VIDEO_STAG_REPAIR" -Filter *.mp4 -File -ErrorAction SilentlyContinue)

    # ADDED: mark that real work happened
if ($activityDetected) {
    $global:WorkersChecked["AUDIO"] = $true
}

    if ($compressFiles.Count -gt 0) {
        Launch-Worker "VIDEO" "compress" ".\02_video_compress.ps1" "video_compress.done"
        $activityDetected = $true
    }

    if ($speedFiles.Count -gt 0) {
        Launch-Worker "VIDEO" "speed" ".\04_video_speed_up.ps1" "video_speed.done"
        $activityDetected = $true
    }

    if ($repairFiles.Count -gt 0) {
        Launch-Worker "VIDEO" "repair" ".\03_video_repair.ps1" "video_repair.done"
        $activityDetected = $true
    }

    # ADDED: clean boolean logic
    $hasFiles =
        ($compressFiles.Count -gt 0) -or
        ($speedFiles.Count -gt 0) -or
        ($repairFiles.Count -gt 0)

    if (
        -not $hasFiles -and
        (Is-Department-Idle "VIDEO") -and
        $global:WorkersChecked["VIDEO"]
    ) {
        $global:DepartmentState["VIDEO"] = "IDLE"
    }
}
function Run-AuditIfReady {

    # ---------------- AUDIO ----------------
    # ADDED: snapshot filesystem (avoid race)
    $audioCompress = @(Get-ChildItem "AUDIO_STAGE_COMPRESS" -File -ErrorAction SilentlyContinue)
    $audioRepair   = @(Get-ChildItem "AUDIO_STAGE_REPAIR" -File -ErrorAction SilentlyContinue)

if (
    (Is-Department-Idle "AUDIO") -and
    ($audioCompress.Count -eq 0) -and
    ($audioRepair.Count -eq 0) -and
    $global:WorkersChecked["AUDIO"]   #  ADDED
){
    if (
        (Is-Department-Idle "AUDIO") -and
        ($audioCompress.Count -eq 0) -and
        ($audioRepair.Count -eq 0)
    ) {
        Write-Log "AUDIO READY FOR AUDIT"

        # ADDED: prevent duplicate audit launch
        if (-not (Is-Stage-Running "AUDIO" "audit")) {
            Launch-Worker "AUDIO" "audit" ".\05_audio_audit.ps1" "audio_audit.done"

# ADDED: reset after audit
$global:WorkersChecked["AUDIO"] = $false
        }
    }
}
    # ---------------- VIDEO ----------------
    # ADDED: snapshot filesystem
    $videoCompress = @(Get-ChildItem "STAGE_COMPRESS" -File -ErrorAction SilentlyContinue)
    $videoSpeed    = @(Get-ChildItem "STAGE_SPEED" -File -ErrorAction SilentlyContinue)
    $videoRepair   = @(Get-ChildItem "STAGE_REPAIR" -File -ErrorAction SilentlyContinue)

if (
    (Is-Department-Idle "VIDEO") -and
    ($videoCompress.Count -eq 0) -and
    ($videoSpeed.Count -eq 0) -and
    ($videoRepair.Count -eq 0) -and
    $global:WorkersChecked["VIDEO"]   # 🔥 ADDED
)
{
    if (
        (Is-Department-Idle "VIDEO") -and
        ($videoCompress.Count -eq 0) -and
        ($videoSpeed.Count -eq 0) -and
        ($videoRepair.Count -eq 0)
    ) {
        Write-Log "VIDEO READY FOR AUDIT"

        # ADDED: prevent duplicate audit launch
        if (-not (Is-Stage-Running "VIDEO" "audit")) {
            Launch-Worker "VIDEO" "audit" ".\05_video_audit.ps1" "video_audit.done"

# ADDED: reset after audit
$global:WorkersChecked["VIDEO"] = $false
        }
    }
}
}
# ---------------- MAIN LOOP ----------------
Write-Log "MASTER V4 ENGINE STARTED"

while ($true) {

    Cleanup-StaleLocks  # ADDED
    Check-Workers

    Run-Department "AUDIO"
    Run-Department "VIDEO"

    Run-AudioWorkers
    Run-VideoWorkers

    Run-AuditIfReady

    Write-Log "Heartbeat | ActiveJobs=$($global:RunningJobs.Count)"

    Start-Sleep -Seconds $POLL_INTERVAL
}
