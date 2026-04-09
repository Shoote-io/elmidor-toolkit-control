# ==================================================
 # Master Media OS (Distributed workers module farm)
# ==================================================
$ErrorActionPreference = "Stop"

$POLL_INTERVAL = 15 
$JOB_TIMEOUT   = 3600 
$MAX_RETRIES   = 2

# === RESOLUTION (FROM tools → SYSTEM → ROOT) ===
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path        # tools
$SYS        = Split-Path -Parent $ScriptRoot                          # 00_SYSTEM
$ROOT       = Split-Path -Parent $SYS                                 # C:\ElmidorOS

# === CRITICAL PATHS ===
$WORKERS    = Join-Path $SYS "workers"
$LOG_DIR    = Join-Path $SYS "logs"

# === PROCESSING ZONE ===
$PROCESSING_ROOT = Join-Path $ROOT "05_MEDIA_PROCESSING"

# === SIGNAL DIR (INSIDE PROCESSING ZONE) ===
$SIGNAL_DIR = Join-Path $PROCESSING_ROOT "00_SYSTEM\signals"

# ensure dirs
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null
New-Item -ItemType Directory -Force -Path $SIGNAL_DIR | Out-Null

Write-Host "SYSTEM ROOT =" $SYS
Write-Host "PROCESSING ROOT =" $PROCESSING_ROOT
Write-Host "SIGNAL PATH =" $SIGNAL_DIR

$global:DepartmentState = @{
    "AUDIO" = "IDLE"
    "VIDEO" = "IDLE"
}

# === WORKER PATH RESOLUTION (ABSOLUTE) ===
$Scripts = @{
    "AUDIO" = @{
        discovery     = Join-Path $WORKERS "audio-discovery.ps1"
        analyzer      = Join-Path $WORKERS "00_audio_analyzer.ps1"
        distribution  = Join-Path $WORKERS "99_audio_distributor.ps1"
    }
    "VIDEO" = @{
        discovery     = Join-Path $WORKERS "video-discovery.ps1"
        analyzer      = Join-Path $WORKERS "00_video_analyzer.ps1"
        distribution  = Join-Path $WORKERS "99_video_distributor.ps1"
    }
}

# ---------------- LOGGING ----------------
function Write-Log($msg, $level="INFO") {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts][$level] $msg"
    Write-Host $line
    Add-Content (Join-Path $LOG_DIR "master_4life.md") $line
}

# === HEARTBEAT ===
function Write-Heartbeat {
    Write-Log "MASTER HEARTBEAT OK"
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

# === ENSURE ABSOLUTE PATH (FROM WORKERS DIR) ===
if (-not ([System.IO.Path]::IsPathRooted($Script))) {
    $Script = Join-Path $WORKERS (Split-Path $Script -Leaf)
}

if (!(Test-Path $Script)) {
    Write-Log "Missing Script: $Script" "ERROR"
    Release-Lock $lockPath
    return
}

$jobId = New-JobId

# === HEARTBEAT (LIGHT) ===
if ($DEBUG_MODE) {
    Write-Log "HEARTBEAT launch attempt [$Department::$Stage]"
}
if ($DEBUG_MODE) {
    Write-Log "ARGS: $($args -join ' ')"
}
try {
    # SAFE ARGUMENT BUILDER (NO NULL)
$args = @(
    "-NoExit",
    "-ExecutionPolicy", "Bypass",
    "-File", $Script
)

if ($InputPath -and $InputPath.Trim() -ne "") {
    $args += @("-InputPath", $InputPath)
}

if ($PROCESSING_ROOT -and $PROCESSING_ROOT.Trim() -ne "") {
    $args += @("-OutputPath", $PROCESSING_ROOT)
}

if ($SIGNAL_DIR -and $SIGNAL_DIR.Trim() -ne "") {
    $args += @("-SignalPath", $SIGNAL_DIR)
}

    if ($DEBUG_MODE) {

        $proc = Start-Process powershell `
            -ArgumentList $args `
            -PassThru
    } 
    else {

        $proc = Start-Process powershell `
            -ArgumentList $args `
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
        LockPath   = $lockPath
    }

    Write-Log "LAUNCHED [$Department::$Stage] PID=$($proc.Id)"
}
catch {
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
# === STATE HANDLER (SINGLE SOURCE OF TRUTH) ===

if (-not $job -or -not ($job -is [hashtable])) {
    Write-Log "INVALID JOB OBJECT detected, skipping..."
    continue
}

if (-not $job.ContainsKey("StartTime") -or -not ($job["StartTime"] -is [datetime])) {
    Write-Log "INVALID StartTime [$($job.Department)::$($job.Stage)] - fixing"
    $job["StartTime"] = Get-Date
}

$elapsed = (Get-Date) - $job["StartTime"]

# === CASE 1: PROCESS STILL RUNNING ===
if ($procAlive) {

    Write-Log "STILL RUNNING [$($job.Department)::$($job.Stage)] PID=$($job.ProcessId)"

    # STALE SIGNAL PROTECTION
    if (Test-Path $signalPath) {
        Write-Log "WARNING: Signal detected[$($job.Department)::$($job.Stage)] (waiting process exit)"
    }

    cintinue
}

# === CASE 2: PROCESS FINISHED ===
if (Test-Path $signalPath) {

    Write-Log "COMPLETED [$($job.Department)::$($job.Stage)] via signal"

    switch ($job.Stage) {
        "discovery"    { $global:DepartmentState[$job.Department] = "DISCOVERY_DONE" }
        "analyzer"     { $global:DepartmentState[$job.Department] = "ANALYZER_DONE" }
        "distribution" { $global:DepartmentState[$job.Department] = "DISTRIBUTION_DONE" }
        "audit"        { $global:DepartmentState[$job.Department] = "IDLE" }
    }

    Remove-Item $signalPath -Force -ErrorAction SilentlyContinue
    Release-Lock $job.LockPath

    if ($global:RunningJobs.ContainsKey($jobId)) {
        $global:RunningJobs.Remove($jobId)
    }

    continue
}

# === CASE 3: TIMEOUT ===
if ($elapsed.TotalSeconds -gt $JOB_TIMEOUT) {

    Write-Log "TIMEOUT [$($job.Department)::$($job.Stage)]" "WARN"

    try {
        if ($job.ProcessId) {
            Stop-Process -Id $job.ProcessId -Force -ErrorAction Stop
        }
    } catch {}

    if ($job.Retries -lt $MAX_RETRIES) {

        $job.Retries++
        Release-Lock $job.LockPath

        Launch-Worker `
            $job.Department `
            $job.Stage `
            $job.Script `
            $job.Signal

        if ($global:RunningJobs.ContainsKey($jobId)) {
            $global:RunningJobs.Remove($jobId)
        }

        continue
    }

    Write-Log "FAILED AFTER MAX RETRIES (TIMEOUT) [$($job.Department)::$($job.Stage)]" "ERROR"

    Release-Lock $job.LockPath

    if ($global:RunningJobs.ContainsKey($jobId)) {
        $global:RunningJobs.Remove($jobId)
    }

    continue
}

# === CASE 4: CRASH (PROCESS DEAD, NO SIGNAL) ===
Write-Log "CRASH DETECTED [$($job.Department)::$($job.Stage)]" "ERROR"

if ($job.Retries -lt $MAX_RETRIES) {

    $job.Retries++
    Release-Lock $job.LockPath

    Launch-Worker `
        $job.Department `
        $job.Stage `
        $job.Script `
        $job.Signal

    if ($global:RunningJobs.ContainsKey($jobId)) {
        $global:RunningJobs.Remove($jobId)
    }

    continue
}

Write-Log "FAILED AFTER MAX RETRIES [$($job.Department)::$($job.Stage)]" "ERROR"

Release-Lock $job.LockPath

if ($global:RunningJobs.ContainsKey($jobId)) {
    $global:RunningJobs.Remove($jobId)
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
$global:WorkersChecked["AUDIO"] = $false  # ADDED

function Run-AudioWorkers {

    if ($global:DepartmentState["AUDIO"] -ne "WORKERS") { return }

    $activityDetected = $false

    # SNAPSHOT (ABSOLUTE PATH)
    $compressPath = Join-Path $PROCESSING_ROOT "AUDIO_STAGE_COMPRESS"
    $repairPath   = Join-Path $PROCESSING_ROOT "AUDIO_STAG_REPAIR"

    $compressFiles = @(Get-ChildItem $compressPath -File -ErrorAction SilentlyContinue)
    $repairFiles   = @(Get-ChildItem $repairPath -File -ErrorAction SilentlyContinue)

    if ($compressFiles.Count -gt 0) {
        Launch-Worker "AUDIO" "compress" ".\02_audio_compress.ps1" "audio_compress.done"
        $activityDetected = $true
    }

    if ($repairFiles.Count -gt 0) {
        Launch-Worker "AUDIO" "repair" ".\03_audio_repair.ps1" "audio_repair.done"
        $activityDetected = $true
    }

    if ($activityDetected) {
        $global:WorkersChecked["AUDIO"] = $true
    }

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
$global:WorkersChecked["VIDEO"] = $false  # ADDED

function Run-VideoWorkers {

    if ($global:DepartmentState["VIDEO"] -ne "WORKERS") { return }

    $activityDetected = $false

    # SNAPSHOT (ABSOLUTE PATH)
    $compressPath = Join-Path $PROCESSING_ROOT "VIDEO_STAGE_COMPRESS"
    $speedPath    = Join-Path $PROCESSING_ROOT "VIDEO_STAGE_SPEED"
    $repairPath   = Join-Path $PROCESSING_ROOT "VIDEO_STAG_REPAIR"

    $compressFiles = @(Get-ChildItem $compressPath -Filter *.mp4 -File -ErrorAction SilentlyContinue)
    $speedFiles    = @(Get-ChildItem $speedPath -Filter *.mp4 -File -ErrorAction SilentlyContinue)
    $repairFiles   = @(Get-ChildItem $repairPath -Filter *.mp4 -File -ErrorAction SilentlyContinue)

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

    if ($activityDetected) {
        $global:WorkersChecked["VIDEO"] = $true
    }

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
    $audioCompressPath = Join-Path $PROCESSING_ROOT "AUDIO_STAGE_COMPRESS"
    $audioRepairPath   = Join-Path $PROCESSING_ROOT "AUDIO_STAGE_REPAIR"

    $audioCompress = @(Get-ChildItem $audioCompressPath -File -ErrorAction SilentlyContinue)
    $audioRepair   = @(Get-ChildItem $audioRepairPath -File -ErrorAction SilentlyContinue)

    $audioReady =
        (Is-Department-Idle "AUDIO") -and
        ($audioCompress.Count -eq 0) -and
        ($audioRepair.Count -eq 0) -and
        $global:WorkersChecked["AUDIO"] -and ($global:DepartmentState["AUDIO"] -eq "WORKERS")

    if ($audioReady) {

        Write-Log "AUDIO READY FOR AUDIT"

        if (-not (Is-Stage-Running "AUDIO" "audit")) {

            Launch-Worker "AUDIO" "audit" ".\05_audio_audit.ps1" "audio_audit.done"

            $global:WorkersChecked["AUDIO"] = $false
        }
    }

    # ---------------- VIDEO ----------------
    $videoCompressPath = Join-Path $PROCESSING_ROOT "VIDEO_STAGE_COMPRESS"
    $videoSpeedPath    = Join-Path $PROCESSING_ROOT "VIDEO_STAGE_SPEED"
    $videoRepairPath   = Join-Path $PROCESSING_ROOT "VIDEO_STAGE_REPAIR"

    $videoCompress = @(Get-ChildItem $videoCompressPath -File -ErrorAction SilentlyContinue)
    $videoSpeed    = @(Get-ChildItem $videoSpeedPath -File -ErrorAction SilentlyContinue)
    $videoRepair   = @(Get-ChildItem $videoRepairPath -File -ErrorAction SilentlyContinue)

    $videoReady =
        (Is-Department-Idle "VIDEO") -and
        ($videoCompress.Count -eq 0) -and
        ($videoSpeed.Count -eq 0) -and
        ($videoRepair.Count -eq 0) -and
        $global:WorkersChecked["VIDEO"] -and ($global:DepartmentState["VIDEO"] -eq "WORKERS")

    if ($videoReady) {

        Write-Log "VIDEO READY FOR AUDIT"

        if (-not (Is-Stage-Running "VIDEO" "audit")) {

            Launch-Worker "VIDEO" "audit" ".\05_video_audit.ps1" "video_audit.done"

            $global:WorkersChecked["VIDEO"] = $false
        }
    }
}
# ---------------- MAIN LOOP ----------------
Write-Log "MASTER V4 ENGINE STARTED"

while ($true) {

    Cleanup-StaleLocks
    Check-Workers

    Run-Department "AUDIO"
    Run-Department "VIDEO"

    Run-AudioWorkers
    Run-VideoWorkers

    Run-AuditIfReady

    # SMART HEARTBEAT (pa spam logs)
    if ((Get-Date).Second % 20 -eq 0) {
        Write-Log "Heartbeat | ActiveJobs=$($global:RunningJobs.Count)"
    }

    Start-Sleep -Seconds $POLL_INTERVAL
}
