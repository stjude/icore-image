const { app, BrowserWindow, dialog, ipcMain, shell } = require('electron');
const { autoUpdater } = require("electron-updater")
const { spawn, exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const util = require('util');
const { initializeApp } = require('./lib/setup');

const execPromise = util.promisify(exec);

let mainWindow;
let serverProcess;
let workerProcess;

const isDev = process.env.ICORE_DEV === '1';

// Stop a child and everything it spawned. On Windows a bare kill() reaches
// only the parent, leaving storescp/rclone/dicom-deid-rs orphaned — storescp
// holding port 50001 breaks the next PACS retrieval — so kill the tree there
// in both dev and prod. Awaited so the app cannot exit (and NSIS cannot try to
// replace manage.exe) while taskkill is still running.
async function stopProcess(proc) {
  if (!proc) return;
  try {
    if (process.platform === 'win32' && proc.pid) {
      await execPromise(`taskkill /pid ${proc.pid} /T /F`);
    } else if (isDev && proc.pid) {
      // Dev children are detached group leaders running autoreload.
      process.kill(-proc.pid, 'SIGKILL');
    } else {
      proc.kill();
    }
  } catch (error) {
    // Already gone.
  }
}

// The PyInstaller-frozen Django entry point is `manage.exe` on Windows.
const MANAGE_BIN = process.platform === 'win32' ? 'manage.exe' : 'manage';

// Python defaults to the locale encoding (cp1252 on Windows) for stdout and for
// any open() without an explicit encoding. UTF-8 mode makes the child behave
// like macOS, so DICOM values with non-ASCII characters cannot break logging or
// file I/O. The hot paths also pass encoding= explicitly; this is the backstop.
const PYTHON_ENV = { PYTHONUTF8: '1' };

// Resolve the Python interpreter to use in dev mode. Prefer an explicit
// ICORE_PYTHON (set by `make dev`), then the project's uv virtualenv, then a
// bare interpreter. A bare `python` usually lacks the project dependencies.
function getDevPython() {
  if (process.env.ICORE_PYTHON) {
    return process.env.ICORE_PYTHON;
  }
  const repoRoot = path.join(__dirname, '..');
  const venvPython = process.platform === 'win32'
    ? path.join(repoRoot, '.venv', 'Scripts', 'python.exe')
    : path.join(repoRoot, '.venv', 'bin', 'python');
  if (fs.existsSync(venvPython)) {
    return venvPython;
  }
  return process.platform === 'win32' ? 'python' : 'python3';
}

// The DICOM listener ports (storescp binds 50001), freed on startup and
// shutdown in case a previous run did not exit cleanly.
const CTP_PORTS = [50000, 50001, 50010, 50020, 50030, 50040, 50050, 50060, 50070, 50080, 50090];

// Kill whatever is listening on any of `ports`, in one process. Best effort:
// nothing may be listening.
//
// The Windows branch used to be a `FOR /F ... %%a` one-liner, but exec() runs
// cmd.exe /c, where the loop variable is `%a` — `%%a` is batch-file-only, so
// the command always failed and the error was swallowed. Get-NetTCPConnection
// also avoids netstat/findstr's substring matching (`:8000` matched `:18000`)
// and TIME_WAIT rows with PID 0. PowerShell takes ~1s to start, so all ports
// go in a single invocation.
async function killProcessesOnPorts(ports) {
  const list = ports.join(',');
  try {
    let cmd;
    if (process.platform === 'win32') {
      cmd =
        `powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort ${list} ` +
        `-State Listen -ErrorAction SilentlyContinue | ` +
        `Select-Object -ExpandProperty OwningProcess -Unique | ` +
        `Where-Object { $_ -ne 0 } | ` +
        `ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }"`;
    } else {
      cmd = `lsof -ti:${list} | xargs kill -9 2>/dev/null || true`;
    }
    await execPromise(cmd);
  } catch (error) {
    // Best effort; nothing may be listening.
  }
}

// Must match icore_paths.icore_base_dir() exactly: Electron seeds
// settings.json and the database here and Django reads them from its own
// notion of the same directory. Windows uses LOCALAPPDATA so OneDrive's Known
// Folder Move cannot sync the sqlite files or PHI working directories.
const baseDir = process.platform === 'win32' && process.env.LOCALAPPDATA
  ? path.join(process.env.LOCALAPPDATA, 'iCore')
  : path.join(os.homedir(), 'Documents', 'iCore');
const configDir = path.join(baseDir, 'config');
const logsDir = path.join(baseDir, 'logs', 'system');
const logFilePath = path.join(logsDir, 'log.txt');
const dbPath = path.join(configDir, 'db.sqlite3');
const settingsPath = path.join(configDir, 'settings.json');
const oldLocationDir = path.join(os.homedir(), '.icore');

let logStream;

function logWithTimestamp(source, message) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] [${source}] ${message}\n`;
  
  if (logStream) {
    logStream.write(logMessage);
  }
}

ipcMain.handle('open-folder', async (event, folderPath) => {
  try {
    if (!folderPath) {
      return { success: false, error: 'No folder path provided' };
    }
    
    const expandedPath = folderPath.replace(/^~/, os.homedir());
    
    if (!fs.existsSync(expandedPath)) {
      return { success: false, error: 'Folder does not exist' };
    }

    const err = await shell.openPath(expandedPath);
    if (err) {
      return { success: false, error: err };
    }
    return { success: true };
  } catch (error) {
    logWithTimestamp('main', `Error opening folder: ${error}`);
    return { success: false, error: error.message };
  }
});

// A second instance would race the first for port 8000, run a second Celery
// worker against the same sqlite broker, and re-run migrate concurrently.
// Double-launching is easy on Windows (Start menu + desktop shortcut).
if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });
}

app.on('ready', async () => {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 720,
    // Match the app's bg-gray-100 so cross-page navigation never flashes white.
    backgroundColor: '#f3f4f6',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      webSecurity: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'loading.html'));

  const devPython = getDevPython();

  try {
    const defaultSettingsPath = app.isPackaged
      ? path.join(process.resourcesPath, 'app', 'assets', 'settings.json')
      : path.join(__dirname, 'assets', 'settings.json');

    let managePath;
    if (isDev) {
      managePath = path.join(__dirname, '..', 'deid', 'manage.py');
    } else {
      managePath = app.isPackaged
        ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', MANAGE_BIN)
        : path.join(__dirname, 'assets', 'dist', 'manage', MANAGE_BIN);
    }

    if (!fs.existsSync(defaultSettingsPath)) {
      throw new Error(`Default settings not found at: ${defaultSettingsPath}`);
    }

    if (!fs.existsSync(managePath)) {
      throw new Error(`Manage binary not found at: ${managePath}`);
    }

    await initializeApp({
      pythonEnv: PYTHON_ENV,
      baseDir,
      dbPath,
      settingsPath,
      defaultSettingsPath,
      managePath,
      spawnFn: spawn,
      isDev,
      oldLocationDir,
      pythonExec: devPython
    });

    fs.mkdirSync(logsDir, { recursive: true });
    logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
    
    logWithTimestamp('main', 'Application initialized successfully');
  } catch (error) {
    console.error('Initialization failed:', error);
    console.error('Error stack:', error.stack);
    
    try {
      fs.mkdirSync(logsDir, { recursive: true });
      logStream = fs.createWriteStream(logFilePath, { flags: 'a' });
      logWithTimestamp('main', `Initialization failed: ${error}\nStack: ${error.stack}`);
    } catch (logError) {
      console.error('Failed to write to log file:', logError);
    }
    
    dialog.showMessageBox({
      type: 'error',
      title: 'Initialization Failed',
      message: `Failed to initialize application: ${error.message}\n\nCheck console for details.`,
      buttons: ['OK']
    }).then(() => {
      app.quit();
    });
    return;
  }
  
  await killProcessesOnPorts(CTP_PORTS);

  // Pipe a child's output into the app log. windowsHide keeps the console
  // window that manage.exe (built with console=True) would otherwise show.
  const wireLogging = (proc, source) => {
    proc.stdout.on('data', (data) => logWithTimestamp(source, data.toString().trim()));
    proc.stderr.on('data', (data) => logWithTimestamp(source, data.toString().trim()));
    proc.on('error', (err) => logWithTimestamp(source, `Process error: ${err}`));
  };

  let startWorker;

  if (isDev) {
    // Free port 8000 in case a previous dev server (or its autoreload child)
    // is still holding it, then start Django + worker with hot reload.
    await killProcessesOnPorts([8000]);

    const managePyPath = path.join(__dirname, '..', 'deid', 'manage.py');
    const deidDir = path.join(__dirname, '..', 'deid');
    const env = { ...process.env, ...PYTHON_ENV, ICORE_DEV: '1' };

    logWithTimestamp('main', `Dev mode: launching Django + worker with ${devPython}`);

    // runserver (with autoreload) and the worker (wrapped in Django's
    // autoreloader) restart themselves on .py edits. On POSIX, detached: true
    // puts each in its own process group so we can kill their reloader
    // children on exit; on Windows it would instead give each child its own
    // visible console, and taskkill /T handles the tree anyway.
    const devOpts = {
      env,
      cwd: deidDir,
      detached: process.platform !== 'win32',
      windowsHide: true,
    };
    serverProcess = spawn(devPython, [managePyPath, 'runserver', '127.0.0.1:8000'], devOpts);
    startWorker = () => spawn(devPython, [managePyPath, 'worker'], devOpts);
  } else {
    const manageBinaryPath = app.isPackaged
      ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', MANAGE_BIN)
      : path.join(__dirname, 'assets', 'dist', 'manage', MANAGE_BIN);

    const prodOpts = {
      env: { ...process.env, ...PYTHON_ENV },
      cwd: path.dirname(manageBinaryPath),
      windowsHide: true,
    };
    serverProcess = spawn(manageBinaryPath, ['runserver', '--noreload'], prodOpts);
    startWorker = () => spawn(manageBinaryPath, ['worker'], prodOpts);
  }

  wireLogging(serverProcess, 'server');

  // Keep the worker alive. Cancelling a task kills its child processes, and a
  // pipeline crash can take the worker with it; without a restart the app goes
  // on accepting tasks that will never run. A worker that dies immediately
  // (e.g. a broken migration) backs off to a minute between attempts rather
  // than hammering the log; one that ran for a while resets the delay.
  let workerRestartDelay = 1000;
  const superviseWorker = () => {
    const startedAt = Date.now();
    workerProcess = startWorker();
    wireLogging(workerProcess, 'worker');
    workerProcess.on('exit', (code, signal) => {
      if (mainWindow.isClosing) return;
      if (Date.now() - startedAt > 60000) workerRestartDelay = 1000;
      logWithTimestamp('worker', `Worker exited (code=${code} signal=${signal}); restarting in ${workerRestartDelay / 1000}s`);
      setTimeout(superviseWorker, workerRestartDelay);
      workerRestartDelay = Math.min(workerRestartDelay * 2, 60000);
    });
  };
  superviseWorker();

  mainWindow.webContents.on('console-message', (event, level, message) => {
    logWithTimestamp('renderer', `Console [${level}]: ${message}`);
  });

  mainWindow.webContents.on('crashed', () => {
    logWithTimestamp('main', 'Renderer process crashed');
  });

  mainWindow.on('unresponsive', () => {
    logWithTimestamp('main', 'Window became unresponsive');
  });

  // Retry loading until the dev server is accepting connections, so startup
  // is robust regardless of how long the interpreter takes to boot.
  const appUrl = 'http://127.0.0.1:8000/';
  mainWindow.webContents.on('did-fail-load', (event, errorCode, errorDescription, validatedURL) => {
    if (validatedURL && validatedURL.startsWith('http://127.0.0.1:8000')) {
      logWithTimestamp('main', `Load failed (${errorCode} ${errorDescription}); retrying...`);
      setTimeout(() => {
        if (mainWindow && !mainWindow.isDestroyed()) {
          mainWindow.loadURL(appUrl);
        }
      }, 1000);
    }
  });

  await new Promise(resolve => setTimeout(resolve, 5000));
  mainWindow.loadURL(appUrl);

  if (app.isPackaged) {
    let betaUpdates = false;
    try {
      if (fs.existsSync(settingsPath)) {
        const userSettings = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
        betaUpdates = userSettings.beta_updates_enabled === true;
      }
    } catch (e) {
      logWithTimestamp('updater', `failed reading beta_updates_enabled: ${e}`);
    }
    autoUpdater.allowPrerelease = betaUpdates;
    logWithTimestamp('updater', `allowPrerelease=${betaUpdates}`);
    autoUpdater.checkForUpdatesAndNotify().catch((error) => {
      logWithTimestamp('updater', `Update check failed: ${error}`);
    });
  }

  // The back button is rendered by the app's own header (see base.html), so it
  // paints with the page instead of being injected late and shifting the layout.

  mainWindow.on('close', async (e) => {
    if (!mainWindow.isClosing) {
      e.preventDefault();
      const choice = await dialog.showMessageBox(mainWindow, {
        type: 'question',
        buttons: ['Yes', 'No'],
        title: 'Confirm Close',
        message: 'Are you sure you want to close the application?',
        detail: 'Any currently running tasks will be canceled and may become corrupted.'
      });

      if (choice.response === 0) {
        mainWindow.isClosing = true;
        logWithTimestamp('main', 'Main window closed');

        await stopProcess(serverProcess);
        serverProcess = null;
        await stopProcess(workerProcess);
        workerProcess = null;
        if (isDev) {
          await killProcessesOnPorts([8000]);
        }
        await killProcessesOnPorts(CTP_PORTS);

        if (logStream) {
          logStream.end();
        }
        
        mainWindow.close();
      }
    }
  });
});
