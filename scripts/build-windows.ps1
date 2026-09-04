<#
.SYNOPSIS
    Build the iCore Windows desktop app (NSIS installer).

.DESCRIPTION
    Windows counterpart to the macOS Makefile. PyInstaller cannot cross-compile,
    so this must run on a real Windows machine (or a Windows CI runner) of the
    target architecture. Native cargo / PyInstaller builds land in their default
    output dirs, which the spec and electron config already expect.

    Steps mirror `make all`:
      deps -> external-deps (dcmtk, rclone) -> dicom-deid-rs -> build-django-app
      -> prepare-assets -> package (electron-builder --win)

.PARAMETER Arch
    Target architecture: x64 (default) or arm64. Affects the rclone download and
    the electron-builder flag only. The Rust engine and manage.exe are built
    natively for whatever architecture this host is.

.PARAMETER Publish
    electron-builder --publish value (never | onTag | always). Default: never.

.EXAMPLE
    pwsh scripts/build-windows.ps1 -Arch x64
#>
[CmdletBinding()]
param(
    [ValidateSet('x64', 'arm64')]
    [string]$Arch = 'x64',
    [string]$Publish = 'never'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Repo root is the parent of this script's directory.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$DcmtkVersion = '3.6.9'
$RcloneVersion = 'v1.68.2'
# DCMTK ships a prebuilt win64 (x64) dynamic build only; on arm64 it runs under
# Windows' x64 emulation. The Rust engine and rclone have native arm64 builds.
$RcloneArch = if ($Arch -eq 'arm64') { 'arm64' } else { 'amd64' }

function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

# --- Dependencies -----------------------------------------------------------
function Install-Deps {
    Write-Step 'Installing Python dependencies (uv sync)'
    uv sync
    if ($LASTEXITCODE -ne 0) { throw 'uv sync failed' }

    Write-Step 'Installing deid npm dependencies'
    npm install --prefix deid
    if ($LASTEXITCODE -ne 0) { throw 'npm install (deid) failed' }

    Write-Step 'Installing electron npm dependencies'
    npm install --prefix electron
    if ($LASTEXITCODE -ne 0) { throw 'npm install (electron) failed' }
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
    if (Test-Path 'dcmtk') { Remove-Item 'dcmtk' -Recurse -Force }
    Rename-Item $name 'dcmtk'

    # Keep the four tools we invoke plus every DLL the dynamic build needs;
    # drop the other CLI executables to keep the bundle small.
    $keepExe = @('findscu.exe', 'movescu.exe', 'storescp.exe', 'echoscu.exe')
    Get-ChildItem 'dcmtk/bin' -Filter '*.exe' |
        Where-Object { $keepExe -notcontains $_.Name } |
        Remove-Item -Force
}

# --- rclone -----------------------------------------------------------------
function Install-Rclone {
    if (Test-Path 'rclone/rclone.exe') {
        Write-Step 'rclone already present, skipping'
        return
    }
    Write-Step "Downloading rclone (windows-$RcloneArch)"
    $name = "rclone-$RcloneVersion-windows-$RcloneArch"
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
    Push-Location 'dicom-deid-rs'
    try {
        cargo build --release
        if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
    } finally {
        Pop-Location
    }
}

# --- Django app (PyInstaller) ----------------------------------------------
function Build-DjangoApp {
    Write-Step 'Freezing Django app with PyInstaller'
    Push-Location 'deid'
    try {
        uv run pyinstaller --clean -y manage.spec
        if ($LASTEXITCODE -ne 0) { throw 'pyinstaller (manage) failed' }
        uv run pyinstaller --clean -y initialize_admin_password.spec
        if ($LASTEXITCODE -ne 0) { throw 'pyinstaller (admin_password) failed' }
    } finally {
        Pop-Location
    }
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
    Write-Step "Packaging NSIS installer ($Arch)"
    Push-Location 'electron'
    try {
        $flag = if ($Arch -eq 'arm64') { '--arm64' } else { '--x64' }
        npx electron-builder --win $flag --publish $Publish
        if ($LASTEXITCODE -ne 0) { throw 'electron-builder failed' }
    } finally {
        Pop-Location
    }
}

Install-Deps
Install-Dcmtk
Install-Rclone
Build-DeidRs
Build-DjangoApp
Initialize-Assets
Build-Installer

Write-Step 'Done. Installer is in electron/dist/.'
