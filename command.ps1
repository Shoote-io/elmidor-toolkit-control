
# ==========================================
# ELMIDOR OS — MASTER INSTALLER PRO
# ==========================================

param(
    [Parameter(Mandatory=$true)][string]$action,
    [string]$name,
    [string]$url,
    [string]$target
)

# ---------------- PATHS ----------------
$BasePath     = "C:\ElmidorOS"
$SystemPath   = "$BasePath\00_SYSTEM"

$ToolsPath    = "$SystemPath\tools"
$WorkersPath  = "$SystemPath\workers"
$InstallPath  = "$SystemPath\install"
$TempPath     = "$SystemPath\temp"
$SignalsPath  = "$SystemPath\signals"
$RegistryPath = "$SystemPath\registry"

$LogPath      = "$SystemPath\logs\installer.log"

# ---------------- INIT ----------------
function Ensure-Dir($path) {
    $dir = Split-Path -Parent $path
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

Ensure-Dir $LogPath

# ---------------- LOG ----------------
function Write-Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts - $msg" | Out-File -FilePath $LogPath -Append -Encoding UTF8
    Write-Host "[$ts] $msg"
}

# ---------------- DOWNLOAD ----------------
function Download-File {
    param($Url, $Destination)

    try {
        Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing
        Write-Log "Downloaded → $Destination"
        return $true
    }
    catch {
        Write-Log "Download FAILED → $Url"
        return $false
    }
}

# ---------------- TARGET ----------------
function Get-TargetPath {
    param($target, $name)

    switch ($target) {
        "tools"   { return "$ToolsPath\$name.ps1" }
        "workers" { return "$WorkersPath\$name.ps1" }
        "system"  { return "$SystemPath\$name.ps1" }
        default   { return "$BasePath\$name.ps1" }
    }
}

# ---------------- REGISTRY ----------------
function Update-Registry {
    param($name, $status)

    $regFile = "$RegistryPath\registry.json"

    if (!(Test-Path $regFile)) {
        "{}" | Out-File $regFile
    }

    $json = Get-Content $regFile | ConvertFrom-Json

    $json | Add-Member -NotePropertyName $name -NotePropertyValue @{
        status = $status
        updated = (Get-Date)
    } -Force

    $json | ConvertTo-Json -Depth 5 | Out-File $regFile
}

# ---------------- SAFE SELF UPDATE ----------------
function Invoke-SelfUpdate {
    param($newFile, $dest)

    $updater = "$TempPath\updater.ps1"

@"
Start-Sleep -Seconds 2
Copy-Item -Path '$newFile' -Destination '$dest' -Force
Start-Process powershell -ArgumentList '-ExecutionPolicy Bypass -File "$dest"'
"@ | Out-File $updater -Encoding UTF8

    Write-Log "Triggering SELF-UPDATE..."

    Start-Process powershell -ArgumentList "-ExecutionPolicy Bypass -File `"$updater`""
    exit
}

# ==========================================
# MAIN EXECUTION
# ==========================================

if (-not $name -or -not $url) {
    Write-Log "Missing parameters"
    exit
}

$dest    = Get-TargetPath $target $name
$tempNew = "$TempPath\$name.ps1.new"

Ensure-Dir $dest
Ensure-Dir $tempNew

# ---------------- DOWNLOAD TO TEMP ----------------
$ok = Download-File $url $tempNew

if (-not $ok) {
    Update-Registry $name "failed"
    exit
}

# ---------------- CHECK SELF UPDATE ----------------
$currentScript = $MyInvocation.MyCommand.Path

if ($dest -eq $currentScript) {
    Write-Log "Self-update detected for $name"
    Invoke-SelfUpdate $tempNew $dest
}

# ---------------- SAFE REPLACE ----------------
try {
    Copy-Item -Path $tempNew -Destination $dest -Force
    Remove-Item $tempNew -Force

    Write-Log "$action SUCCESS → $name"
    Update-Registry $name "ok"
}
catch {
    Write-Log "REPLACE FAILED → $name"
    Update-Registry $name "failed"
    exit
}

# ---------------- SIGNAL IF CRITICAL ----------------
if ($name -eq "master-chief" -or $name -eq "listener") {

    $signalFile = "$SignalsPath\restart_$name.signal"
    New-Item -ItemType File -Path $signalFile -Force | Out-Null

    Write-Log "Signal created → restart required for $name"
}
