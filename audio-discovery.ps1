# =====================================================
 # OS — AUDIO DISCOVERY ENGINE (Production Hardened)
# =====================================================
param (
    [string]$RootPath = "",
    [int]$BatchSize = 8
)
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'
$SCRIPT_ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $SCRIPT_ROOT) { $SCRIPT_ROOT = Get-Location }
Set-Location $PSScriptRoot
$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "AUDIO_OS Discovery Engine (PRO)" -ForegroundColor Cyan
Write-Host ""

# === BASE PATH (OS MODE) ===
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path   # workers
$SYS        = Split-Path -Parent $ScriptRoot                    # 00_SYSTEM
$ROOT       = Split-Path -Parent $SYS                           # C:\ElmidorOS

$PROCESSING_ROOT = Join-Path $ROOT "05_MEDIA_PROCESSING"

# === WORK CONTEXT ===
$SelfPath = $PROCESSING_ROOT

$StateDir  = Join-Path $PROCESSING_ROOT "00_SYSTEM"
$SignalDir = Join-Path $StateDir "signals"
$BatchDir  = Join-Path $StateDir "logs"
$L         = Join-Path $StateDir "meta"

$CursorFile   = Join-Path $L "audio_discovery_cursor.json"
$CommandsPath = Join-Path $BatchDir "audio_discovery.md"
$SignalPath   = Join-Path $SignalDir "audio_discovery.done"

$IMPORT_DIR = Join-Path $PROCESSING_ROOT "AUDIO_INPUT"
New-Item -ItemType Directory -Force -Path $IMPORT_DIR,$SignalDir,$BatchDir,$L | Out-Null

$AudioExt = @(
# Common formats
"*.mp3","*.wav","*.flac","*.aac","*.m4a","*.ogg","*.wma","*.aiff","*.opus",

# Lossless / studio
"*.alac","*.ape","*.wv","*.tta",

# Broadcast / pro
"*.bwf","*.rf64","*.caf",

# Tracker / module music
"*.mod","*.xm","*.it","*.s3m",

# Mobile / streaming
"*.amr","*.3ga","*.mka",

# Legacy
"*.au","*.snd","*.ra","*.ram"

)

# -------------------------
# SAFETY CHECKS
# -------------------------
# OS MODE: RootPath optional (multi-drive scan)
if ($RootPath -and (Test-Path -LiteralPath $RootPath)) {
    Write-Log "Custom root path detected: $RootPath"
}
else {
    Write-Log "OS MODE: scanning all drives"
}

# -------------------- LOGGING --------------------
function Write-Log {

    param(
        [string]$Message,
        [ValidateSet("INFO","SUCCESS","ERROR")]
        [string]$Level="INFO"
    )

    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $line = "[$ts][$Level] $Message"

    Add-Content -LiteralPath $CommandsPath -Value $line
    Write-Host $line
}
# -------------------------
# LOAD CURSOR (OS-GRADE)
# -------------------------
$CursorLastPath = $null
$CursorIndex    = 0

if (Test-Path -LiteralPath $CursorFile) {
    try {
        $raw = Get-Content $CursorFile -Raw | ConvertFrom-Json

        if ($raw.lastPath) {
            $CursorLastPath = $raw.lastPath
        }

        if ($raw.index -is [int]) {
            $CursorIndex = $raw.index
        }

        Write-Host "Resume → Path=$CursorLastPath | Index=$CursorIndex"
    }
    catch {
        Write-Host "Cursor corrupted → reset"
        $CursorLastPath = $null
        $CursorIndex = 0
    }
}

# -------------------------
# SAVE CURSOR (NEW)
# -------------------------
function Save-Cursor {
    param(
        [string]$Path,
        [int]$Index
    )

    try {
        @{
            lastPath = $Path
            index    = $Index
            updated  = (Get-Date)
        } | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $CursorFile -Encoding UTF8
    }
    catch {
        Write-Log "Failed to save cursor" "ERROR"
    }
}

function Normalize-FileName {
    param ([string]$name)

    if (-not $name) { return "file_" + [guid]::NewGuid().ToString().Substring(0,8) }

    # replace invalid chars
    $name = $name -replace '[\[\]]', '_'
    $name = $name -replace '[<>:"/\\|?*]', '_'

    # trim spaces
    $name = $name.Trim()

    # avoid empty or broken names
    if ([string]::IsNullOrWhiteSpace($name)) {
        $name = "file_" + [guid]::NewGuid().ToString().Substring(0,8)
    }

    return $name
}

# -------------------------
# BUILD DIRECTORY LIST (ALL DRIVES)
# -------------------------

$ExcludePatterns = @(
    "\Windows",
    "\Program Files",
    "\Program Files (x86)",
    "\ProgramData",
    "\$Recycle.Bin",
    "\System Volume Information"
)

$Drives = Get-PSDrive -PSProvider FileSystem | Where-Object {
    $_.Free -ne $null -and $_.Root -match "^[A-Z]:\\$"
}

$dirs = @()

foreach ($drive in $Drives) {

    Write-Log "Scanning drive: $($drive.Root)"

    try {

        $driveDirs = Get-ChildItem -Path $drive.Root -Directory -Recurse -ErrorAction SilentlyContinue |

            Where-Object {

                $path = $_.FullName

                # Exclude self
                if ($path -like "$SelfPath*") { return $false }

                # Exclude system dirs
                foreach ($pattern in $ExcludePatterns) {
                    if ($path -like "*$pattern*") { return $false }
                }

                return $true
            }

        $dirs += $driveDirs
    }
    catch {
        Write-Log "Access issue on drive: $($drive.Root)" "WARN"
    }
}

$dirs = $dirs | Sort-Object FullName -Unique

Write-Log "Dirs found (all drives): $($dirs.Count)"

# -------------------------
# REORDER (SOFT RESUME)
# -------------------------
if ($CursorIndex -gt 0 -and $CursorIndex -lt $dirs.Count) {

    Write-Log "Resuming from index $CursorIndex"

    $dirs = $dirs[$CursorIndex..($dirs.Count - 1)]
}
elseif ($CursorLastPath) {

    $matchIndex = $dirs.FindIndex({ $_.FullName -eq $CursorLastPath })

    if ($matchIndex -ge 0) {
        Write-Log "Resuming from path match at index $matchIndex"
        $dirs = $dirs[$matchIndex..($dirs.Count - 1)]
    }
}

# -------------------------
# DIRECTORY WALK (OPTIMIZED)
# -------------------------
$processed = 0
$Imported  = 0
$failed    = 0
$i         = 0

Write-Log "=== AUDIO DISCOVERY PIPELINE STARTED ==="

foreach ($dir in $dirs) {

    Write-Log "Scanning directory: $($dir.FullName)"

    # SINGLE PASS (FASTER)
    $files = Get-ChildItem -LiteralPath $dir.FullName -File -ErrorAction SilentlyContinue |
        Where-Object {
            $ext = $_.Extension.ToLower()
            $AudioExt -contains "*$ext"
        }

    Write-Log "Files found: $($files.Count)"

    foreach ($f in $files) {

        if ($Imported -ge $BatchSize) { break }

        if (-not $f -or -not $f.FullName) {
            Write-Log "Invalid file object detected"
            continue
        }

        if ($f.FullName -like "$SelfPath*") { continue }

        # RENAME SAFE
        $cleanName = Normalize-FileName $f.Name
if ($cleanName -ne $f.Name) {

            $newPath = Join-Path $f.DirectoryName $cleanName
if (Test-Path $newPath) {
    $base = [System.IO.Path]::GetFileNameWithoutExtension($cleanName)
    $ext  = [System.IO.Path]::GetExtension($cleanName)
    $cleanName = "$base" + "_" + (Get-Random -Minimum 1000 -Maximum 9999) + "$ext"
    $newPath = Join-Path $f.DirectoryName $cleanName
}
            Rename-Item -LiteralPath $f.FullName -NewName $cleanName -ErrorAction SilentlyContinue

            if (Test-Path $newPath) {
                $f = Get-Item $newPath
            }
        }

        $processed++

        $dest = Join-Path $IMPORT_DIR $f.Name

        if (Test-Path -LiteralPath $dest) {
            Write-Log "Skipped (exists): $($f.FullName)"
            continue
        }

        try {

            Move-Item -LiteralPath $f.FullName -Destination $dest -ErrorAction Stop

            if (Test-Path -LiteralPath $dest) {

                $Imported++

                Write-Log "IMPORTED: $($f.FullName) → $dest" "SUCCESS"
            }
        }
        catch {

            $failed++

            Write-Log "ERROR moving file: $($f.FullName) | $($_.Exception.Message)" "ERROR"
        }
    }

    # -------------------------
    # SAVE CURSOR (INDEX-BASED)
    # -------------------------
    Save-Cursor -Path $dir.FullName -Index $i

    if ($Imported -ge $BatchSize) { break }

    $i++
}

# -------------------------
# SIGNAL MASTER (ALWAYS)
# -------------------------
$status = "OK"

if ($failed -gt 0 -and $Imported -eq 0) {
    $status = "FAILED"
}
$result = @{
    timestamp = (Get-Date)
    processed = $processed
    success   = $Imported
    failed    = $failed
    status    = $status
} | ConvertTo-Json -Depth 2

$result | Out-File -LiteralPath $SignalPath -Encoding UTF8 -Force
Write-Log "Signal written: audio_discovery.done"

Write-Log "Processed: $processed"
Write-Log "Imported: $Imported"
Write-Log "Failed: $failed"

if ($failed -gt 0) {
    Write-Log "=== AUDIO DISCOVERY PIPELINE COMPLETED WITH ERRORS ===" "ERROR"

}
else {

    Write-Log "=== AUDIO DISCOVERY PIPELINE COMPLETED SUCCESSFUL ===" "SUCCESS"
}