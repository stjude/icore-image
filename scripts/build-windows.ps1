<#
.SYNOPSIS
    Build the iCore Windows desktop app (NSIS installer).

.DESCRIPTION
    Windows counterpart to the macOS Makefile. PyInstaller cannot cross-compile,
    so this must run on an x64 Windows machine (or CI runner). Native cargo /
    PyInstaller builds land in their default output dirs, which the spec and
    electron config already expect.

    x64 only: uv.lock has no win_arm64 wheels for numpy, pandas, cryptography,
    blis or sqlalchemy. Windows 11 on ARM runs the x64 build under emulation.

    Steps mirror `make all`:
      deps -> external-deps (dcmtk, rclone) -> dicom-deid-rs -> build-frontend
      -> build-django-app -> prepare-assets -> package (electron-builder --win)

.PARAMETER Publish
    electron-builder --publish value (never | onTag | onTagOrDraft | always).
    Default: never.

.EXAMPLE
    pwsh scripts/build-windows.ps1
#>
[CmdletBinding()]
param(
    [ValidateSet('never', 'onTag', 'onTagOrDraft', 'always')]
    [string]$Publish = 'never'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Repo root is the parent of this script's directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

# Set-StrictMode errors on $LASTEXITCODE before any native command has run.
$global:LASTEXITCODE = 0

$DcmtkVersion = '3.6.9'
$RcloneVersion = 'v1.68.2'

function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

# Run a command in a subdirectory and fail loudly. Preferred over npm's
# --prefix, which has cwd/lifecycle-script quirks on Windows.
function Invoke-In($dir, [scriptblock]$block) {
    Push-Location $dir
    try {
        & $block
        if ($LASTEXITCODE -ne 0) { throw "'$block' failed in $dir (exit $LASTEXITCODE)" }
    } finally {
        Pop-Location
    }
}

# --- Dependencies -----------------------------------------------------------
function Install-Deps {
    Write-Step 'Installing Python dependencies (uv sync)'
    uv sync
    if ($LASTEXITCODE -ne 0) { throw 'uv sync failed' }

    Write-Step 'Installing deid npm dependencies'
    Invoke-In 'deid' { npm install }

    Write-Step 'Installing QC viewer npm dependencies'
    Invoke-In 'deid/frontend' { npm install }

    Write-Step 'Installing electron npm dependencies'
    Invoke-In 'electron' { npm install }
}

# --- React QC viewer -------------------------------------------------------
function Build-Frontend {
    # Bundles the viewer into deid/static/qc-viewer/, which manage.spec ships as
    # part of `static`. Must run before Build-DjangoApp. Skipping it produces a
    # working installer whose QC page 404s on qc-viewer.js -- no build error.
    Write-Step 'Building React QC viewer'
    Invoke-In 'deid/frontend' { npm run build }
}

# --- DCMTK ------------------------------------------------------------------
function Install-Dcmtk {
    if (Test-Path 'dcmtk/bin/findscu.exe') {
        Write-Step 'DCMTK already present, skipping'
        return
    }
    Write-Step 'Downloading DCMTK (win64 dynamic)'
    $name = "dcmtk-$DcmtkVersion-win64-dynamic"
    $url = "https://dicom.offis.de/download/dcmtk/dcmtk369/bin/$name.zip"
    Invoke-WebRequest -Uri $url -OutFile 'dcmtk.zip'
    Expand-Archive -Path 'dcmtk.zip' -DestinationPath '.' -Force
    Remove-Item 'dcmtk.zip'
    if (-not (Test-Path $name)) {
        throw "Expected '$name/' inside dcmtk.zip; got: $((Get-ChildItem -Directory).Name -join ', ')"
    }
    if (Test-Path 'dcmtk') { Remove-Item 'dcmtk' -Recurse -Force }
    Rename-Item $name 'dcmtk'

    # Keep the four tools we invoke plus every DLL the dynamic build needs;
    # drop the other CLI executables to keep the bundle small.
    $keepExe = @('findscu.exe', 'movescu.exe', 'storescp.exe', 'echoscu.exe')
    Get-ChildItem 'dcmtk/bin' -Filter '*.exe' |
        Where-Object { $keepExe -notcontains $_.Name } |
        Remove-Item -Force

    # Headers and import libraries are build-time only; PyInstaller would
    # otherwise bundle them into every installer.
    foreach ($d in @('dcmtk/include', 'dcmtk/lib')) {
        if (Test-Path $d) { Remove-Item $d -Recurse -Force }
    }

    # dcmtk.py points DCMDICTPATH here. Unlike the macOS build (which compiles
    # the dictionary in and ships an empty share/), the Windows dynamic build
    # needs the file on disk or every findscu/movescu call fails with
    # "no data dictionary loaded".
    $dict = "dcmtk/share/dcmtk-$DcmtkVersion/dicom.dic"
    if (-not (Test-Path $dict)) {
        $found = (Get-ChildItem 'dcmtk/share' -Recurse -Filter 'dicom.dic' -ErrorAction SilentlyContinue).FullName
        throw "DCMTK data dictionary not at '$dict' (dcmtk.py expects it there). Found instead: $($found -join ', ')"
    }

    # The win64-dynamic build links against the MSVC runtime. If the zip does
    # not carry it, findscu.exe will not start on a clean machine that lacks the
    # VC++ redistributable, and the installer would need to chain vc_redist.
    $dlls = (Get-ChildItem 'dcmtk/bin' -Filter '*.dll').Name
    Write-Step "DCMTK bundled DLLs: $($dlls -join ', ')"
    foreach ($rt in @('vcruntime140.dll', 'msvcp140.dll')) {
        if ($dlls -notcontains $rt) {
            Write-Warning "DCMTK does not bundle $rt; it must come from the VC++ redistributable on the target machine."
        }
    }
}

# --- rclone -----------------------------------------------------------------
function Install-Rclone {
    if (Test-Path 'rclone/rclone.exe') {
        Write-Step 'rclone already present, skipping'
        return
    }
    Write-Step 'Downloading rclone (windows-amd64)'
    $name = "rclone-$RcloneVersion-windows-amd64"
    $url = "https://github.com/rclone/rclone/releases/download/$RcloneVersion/$name.zip"
    Invoke-WebRequest -Uri $url -OutFile 'rclone.zip'
    Expand-Archive -Path 'rclone.zip' -DestinationPath '.' -Force
    New-Item -ItemType Directory -Force -Path 'rclone' | Out-Null
    Copy-Item "$name/rclone.exe" 'rclone/rclone.exe' -Force
    Remove-Item $name -Recurse -Force
    Remove-Item 'rclone.zip'
}

# --- dicom-deid-rs ----------------------------------------------------------
function Build-DeidRs {
    Write-Step 'Building dicom-deid-rs (cargo build --release)'
    Invoke-In 'dicom-deid-rs' { cargo build --release }
}

# --- Django app (PyInstaller) ----------------------------------------------
function Build-DjangoApp {
    Write-Step 'Freezing Django app with PyInstaller'
    Invoke-In 'deid' { uv run pyinstaller --clean -y manage.spec }
    Invoke-In 'deid' { uv run pyinstaller --clean -y initialize_admin_password.spec }
}

# --- Stage assets for electron ---------------------------------------------
function Initialize-Assets {
    Write-Step 'Staging assets for electron-builder'
    $dest = 'electron/assets/dist'
    if (Test-Path $dest) { Remove-Item $dest -Recurse -Force }
    Copy-Item 'deid/home/settings.json' 'electron/assets/settings.json' -Force
    Copy-Item 'deid/dist' $dest -Recurse -Force
}

# --- Package ----------------------------------------------------------------
function Build-Installer {
    Write-Step 'Packaging NSIS installer (x64)'
    Invoke-In 'electron' { npx electron-builder --win --x64 --publish $Publish }
}

Install-Deps
Install-Dcmtk
Install-Rclone
Build-DeidRs
Build-Frontend
Build-DjangoApp
Initialize-Assets
Build-Installer

Write-Step 'Done. Installer is in electron/dist/.'
