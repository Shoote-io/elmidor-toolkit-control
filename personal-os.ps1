#requires -Version 5.1
$ErrorActionPreference = 'Stop'
$KayaRoot = "C:\KAYA"
$PersonalOS = Join-Path $KayaRoot 'ENTITY\PROFILE\PERSONAL-OS'
$Marker = Join-Path $PersonalOS '.installed'

function Write-Json($Path, $Object) {
    $dir = Split-Path $Path -Parent
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    $Object | ConvertTo-Json -Depth 20 | Set-Content -Path $Path -Encoding UTF8
}
function Write-Text($Path, $Text) {
    $dir = Split-Path $Path -Parent
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    Set-Content -Path $Path -Value $Text -Encoding UTF8
}

if (Test-Path $Marker) {
    Write-Host "Personal OS deja enstale: $PersonalOS" -ForegroundColor Yellow
    Write-Host "Bootstrap la pap rekreye li. Sèvi ak .\personal-os\launch\run.ps1"
    exit 0
}

$dirs = @(
'SYSTEM','SYSTEM\identity','SYSTEM\constitution','SYSTEM\configuration','SYSTEM\registry','SYSTEM\policies','SYSTEM\permissions','SYSTEM\contracts','SYSTEM\lifecycle','SYSTEM\governance','SYSTEM\routing','SYSTEM\orchestration','SYSTEM\diagnostics',
'DATA','DATA\files','DATA\documents','DATA\media','DATA\records','DATA\datasets','DATA\archives',
'KNOWLEDGE','KNOWLEDGE\user','KNOWLEDGE\domains','KNOWLEDGE\references','KNOWLEDGE\instructions','KNOWLEDGE\prompts','KNOWLEDGE\templates','KNOWLEDGE\learned',
'TOOLS','TOOLS\system','TOOLS\ai','TOOLS\web','TOOLS\file','TOOLS\media','TOOLS\communication','TOOLS\automation',
'ACTIONS','ACTIONS\create','ACTIONS\read','ACTIONS\update','ACTIONS\delete','ACTIONS\move','ACTIONS\transform','ACTIONS\publish','ACTIONS\send','ACTIONS\analyze','ACTIONS\execute',
'SERVICES','SERVICES\account','SERVICES\ai','SERVICES\media','SERVICES\search','SERVICES\notification','SERVICES\communication','SERVICES\scheduler','SERVICES\automation','SERVICES\synchronization',
'CONNECTIONS','CONNECTIONS\accounts','CONNECTIONS\providers','CONNECTIONS\integrations','CONNECTIONS\apis','CONNECTIONS\devices','CONNECTIONS\external-services',
'WORKSPACES','WORKSPACES\personal','WORKSPACES\work','WORKSPACES\projects','WORKSPACES\creation','WORKSPACES\media',
'STATE','STATE\runtime','STATE\sessions','STATE\jobs','STATE\queues','STATE\checkpoints','STATE\recovery',
'MEMORY','MEMORY\user','MEMORY\preferences','MEMORY\context','MEMORY\history','MEMORY\long-term',
'VALUE','VALUE\assets','VALUE\creations','VALUE\resources','VALUE\collections','VALUE\libraries',
'LOGS','LOGS\runtime','LOGS\services','LOGS\audit','LOGS\errors',
'TEMP','TEMP\work','TEMP\cache','launch'
)
foreach ($d in $dirs) { New-Item -ItemType Directory -Path (Join-Path $PersonalOS $d) -Force | Out-Null }

$id = [guid]::NewGuid().ToString()
$now = (Get-Date).ToUniversalTime().ToString('o')

Write-Json (Join-Path $PersonalOS 'SYSTEM\identity\identity.json') @{
 schema_version='1.0'; type='personal_os_identity'; id=$id; name='Personal OS'; owner=@{type='client';id=$null}; parent=@{system='KAYA';root=$KayaRoot}; created_at=$now; status='initialized'
}
Write-Json (Join-Path $PersonalOS 'SYSTEM\configuration\system.json') @{
 schema_version='1.0'; system='personal-os'; version='0.1.0'; parent_system='KAYA'; runtime=@{mode='local';command_dispatch='enabled';service_discovery='enabled';telemetry='enabled'}
}
Write-Json (Join-Path $PersonalOS 'SYSTEM\configuration\runtime-entrypoint.json') @{
 schema_version='1.0'; enabled=$false; command=$null; note='Configure vrè KAYA Runtime entrypoint la sèlman apre entrypoint aktyèl la konfime.'
}
Write-Json (Join-Path $PersonalOS 'SYSTEM\system.manifest.json') @{
 schema_version='1.0'; kind='personal_os'; id=$id; name='Personal OS'; version='0.1.0'; parent='KAYA'; state='initialized'; root=$PersonalOS;
 architecture=@{governance='SYSTEM';persistence='DATA';knowledge='KNOWLEDGE';tools='TOOLS';actions='ACTIONS';services='SERVICES';connections='CONNECTIONS';workspaces='WORKSPACES';state='STATE';memory='MEMORY';value='VALUE'}
}

Write-Json (Join-Path $PersonalOS 'SYSTEM\contracts\command.contract.json') @{schema_version='1.0';type='command';required=@('id','action','issued_at');optional=@('source','actor','target','arguments','context','permissions')}
Write-Json (Join-Path $PersonalOS 'SYSTEM\contracts\result.contract.json') @{schema_version='1.0';type='result';required=@('command_id','status','completed_at');optional=@('data','error','artifacts','telemetry')}
Write-Json (Join-Path $PersonalOS 'SYSTEM\contracts\service.contract.json') @{schema_version='1.0';type='service';required=@('id','name','version','entrypoint','status');optional=@('dependencies','permissions','capabilities')}
Write-Json (Join-Path $PersonalOS 'SYSTEM\contracts\event.contract.json') @{schema_version='1.0';type='event';required=@('id','type','occurred_at');optional=@('source','subject','data','correlation_id')}
Write-Json (Join-Path $PersonalOS 'SYSTEM\registry\services.json') @{schema_version='1.0';services=@()}
Write-Json (Join-Path $PersonalOS 'SYSTEM\registry\capabilities.json') @{schema_version='1.0';capabilities=@()}
Write-Json (Join-Path $PersonalOS 'SYSTEM\registry\connections.json') @{schema_version='1.0';connections=@()}
Write-Json (Join-Path $PersonalOS 'SYSTEM\registry\workflows.json') @{schema_version='1.0';workflows=@()}

@'
# PERSONAL OS — CONSTITUTION

Personal OS se espas pèsonèl dijital kliyan an andedan KAYA.

SYSTEM gouvène. DATA konsève. KNOWLEDGE sèvi pou konprann. TOOLS bay zouti.
ACTIONS reprezante aksyon. SERVICES bay fonksyon. CONNECTIONS jere relasyon ekstèn.
WORKSPACES òganize aktivite. STATE konsève eta operasyonèl. MEMORY kenbe kontinwite.
VALUE konsève assets/resous ki gen itilite oswa valè.

Règ:
- Pa mete secrets an plain text.
- Pa melanje runtime ak done kliyan.
- Nouvo sèvis dwe pase nan registry/contracts.
- Media se yon branch enpòtan, men Personal OS pa limite ak Media.
- Aksyon ki chanje eta dwe kapab obsève epi verifye.
'@ | Set-Content (Join-Path $PersonalOS 'SYSTEM\constitution\CONSTITUTION.md') -Encoding UTF8

@"
# KAYA Personal OS

ID: $id

Personal OS se espas lavi dijital kliyan an andedan KAYA.

SYSTEM       = tèt panse/gouvènans
DATA        = bagay pou konsève
KNOWLEDGE   = konesans, prompts, instructions, templates
TOOLS       = zouti pou itilize
ACTIONS     = bagay sistèm nan kapab fè
SERVICES    = fonksyon ki disponib
CONNECTIONS = kont, providers ak sistèm ekstèn
WORKSPACES  = pwojè ak aktivite
STATE       = eta operasyonèl
MEMORY      = kontinwite ak kontèks
VALUE       = assets, creations, resources, collections
LOGS        = obsèvasyon/audit
TEMP        = travay tanporè

Pa mete credentials oswa tokens an plain text.
"@ | Set-Content (Join-Path $PersonalOS 'README.md') -Encoding UTF8

@'
# Personal OS RUN
$ErrorActionPreference='Stop'
$Root=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Manifest=Join-Path $Root 'SYSTEM\system.manifest.json'
if (!(Test-Path $Manifest)) { throw "Personal OS manifest pa jwenn: $Manifest" }
$Hook=Join-Path $Root 'SYSTEM\configuration\runtime-entrypoint.json'
$cfg=Get-Content $Hook -Raw | ConvertFrom-Json
Write-Host "KAYA PERSONAL OS" -ForegroundColor Cyan
Write-Host "ROOT: $Root"
if ($cfg.enabled -and $cfg.command) {
  Write-Host 'Launching configured KAYA Runtime...'
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -Command $cfg.command
  exit $LASTEXITCODE
}
Write-Host 'Personal OS la pare; KAYA Runtime entrypoint poko configure.' -ForegroundColor Yellow
'@ | Set-Content (Join-Path $PersonalOS 'launch\run.ps1') -Encoding UTF8

@'
$Root=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Marker=Join-Path $Root '.installed'
$Manifest=Join-Path $Root 'SYSTEM\system.manifest.json'
Write-Host "KAYA PERSONAL OS STATUS" -ForegroundColor Cyan
Write-Host "ROOT: $Root"
Write-Host "INSTALLED: $([bool](Test-Path $Marker))"
Write-Host "MANIFEST: $([bool](Test-Path $Manifest))"
if(Test-Path $Manifest){$m=Get-Content $Manifest -Raw|ConvertFrom-Json;Write-Host "ID: $($m.id)";Write-Host "VERSION: $($m.version)";Write-Host "STATE: $($m.state)"}
'@ | Set-Content (Join-Path $PersonalOS 'launch\status.ps1') -Encoding UTF8

@'
$ErrorActionPreference='Stop'
$Root=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$required=@('SYSTEM','DATA','KNOWLEDGE','TOOLS','ACTIONS','SERVICES','CONNECTIONS','WORKSPACES','STATE','MEMORY','VALUE','LOGS','TEMP')
foreach($d in $required){$p=Join-Path $Root $d;if(!(Test-Path $p)){New-Item -ItemType Directory -Path $p -Force|Out-Null;Write-Host "Repaired: $d" -ForegroundColor Yellow}}
Write-Host 'Personal OS fondasyon verifye.' -ForegroundColor Green
'@ | Set-Content (Join-Path $PersonalOS 'launch\repair.ps1') -Encoding UTF8

Write-Json $Marker @{installed=$true;installed_at=$now;bootstrap='KAYA Personal OS Bootstrap';bootstrap_version='0.1.0';personal_os_id=$id;kaya_root=$KayaRoot}

$check=@('SYSTEM\system.manifest.json','SYSTEM\constitution\CONSTITUTION.md','SYSTEM\identity\identity.json','SYSTEM\registry\services.json','SYSTEM\contracts\command.contract.json','launch\run.ps1','launch\status.ps1','.installed')
$missing=$check|Where-Object{!(Test-Path (Join-Path $PersonalOS $_))}
if($missing){throw "INSTALLATION FAILED. Missing: $($missing -join ', ')"}
Write-Host ''
Write-Host 'KAYA PERSONAL OS — INSTALLATION COMPLETE' -ForegroundColor Green
Write-Host "Personal OS: $PersonalOS"
Write-Host "ID: $id"
Write-Host ''
Write-Host 'Status: .\personal-os\launch\status.ps1'
Write-Host 'Run:    .\personal-os\launch\run.ps1'
