const { app, BrowserWindow, dialog, ipcMain } = require('electron');
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

async function killProcessOnPort(port) {
  try {
    const platform = process.platform;
    let cmd;
    if (platform === 'darwin' || platform === 'linux') {
      cmd = `lsof -ti:${port} | xargs kill -9 2>/dev/null || true`;
    } else if (platform === 'win32') {
      cmd = `FOR /F "tokens=5" %%a IN ('netstat -aon ^| findstr :${port}') DO taskkill /F /PID %%a`;
    }
    if (cmd) {
      await execPromise(cmd);
    }
  } catch (error) {
    // Best effort; nothing may be listening.
  }
}

const baseDir = path.join(os.homedir(), 'Documents', 'iCore');
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

function logUpdaterError(stage, error) {
  const detail = error && error.stack ? error.stack : String(error);
  logWithTimestamp('updater', `ERROR ${stage}: ${detail}`);
}

// electron-updater logs to `console`, which is discarded in a packaged app, and
// it re-emits Squirrel.Mac failures as 'error' events that throw when nothing
// is listening. Route both into log.txt so a failed update is diagnosable from
// a user's log instead of being invisible.
function instrumentAutoUpdater() {
  autoUpdater.logger = {
    info: (message) => logWithTimestamp('updater', message),
    warn: (message) => logWithTimestamp('updater', `WARN ${message}`),
    error: (message) => logWithTimestamp('updater', `ERROR ${message}`),
    // MacUpdater reports the hand-off to Squirrel.Mac (proxy server lifecycle,
    // which file Squirrel actually fetched) only at debug level.
    debug: (message) => logWithTimestamp('updater', `DEBUG ${message}`),
  };

  autoUpdater.on('error', (error) => logUpdaterError('updater event', error));

  autoUpdater.on('checking-for-update', () => {
    logWithTimestamp('updater', 'checking for update');
  });

  autoUpdater.on('update-available', (info) => {
    logWithTimestamp('updater', `update available: ${info.version}`);
  });

  autoUpdater.on('update-not-available', (info) => {
    logWithTimestamp('updater', `no update available (latest ${info.version})`);
  });

  // A full download is ~250 MB, so report only when the tens digit advances.
  let loggedDecade = -1;
  autoUpdater.on('download-progress', (progress) => {
    const decade = Math.floor(progress.percent / 10);
    if (decade > loggedDecade) {
      loggedDecade = decade;
      logWithTimestamp('updater', `download ${Math.floor(progress.percent)}%`);
    }
  });

  autoUpdater.on('update-downloaded', (info) => {
    logWithTimestamp('updater', `update downloaded: ${info.version} (${info.downloadedFile})`);
  });
}

// Squirrel.Mac installs by replacing the whole .app bundle in place, so an
// update downloads fine but silently never installs when the bundle is not
// writable by the current user, is still running from a mounted DMG, or was
// translocated by Gatekeeper. Record which of those applies at check time.
function logUpdaterEnvironment() {
  const bundlePath = path.resolve(path.dirname(process.execPath), '..', '..');

  let writable = 'yes';
  try {
    fs.accessSync(bundlePath, fs.constants.W_OK);
    fs.accessSync(path.dirname(bundlePath), fs.constants.W_OK);
  } catch (error) {
    writable = `no (${error.code})`;
  }

  logWithTimestamp('updater', `version=${app.getVersion()} bundle=${bundlePath}`);
  logWithTimestamp(
    'updater',
    `bundleWritable=${writable} ` +
    `translocated=${bundlePath.includes('/AppTranslocation/')} ` +
    `onMountedVolume=${bundlePath.startsWith('/Volumes/')}`
  );
}

async function killProcessesOnCtpPorts() {
  const ctpPorts = [50000, 50001, 50010, 50020, 50030, 50040, 50050, 50060, 50070, 50080, 50090];
  
  for (const port of ctpPorts) {
    try {
      const platform = process.platform;
      let cmd;
      
      if (platform === 'darwin' || platform === 'linux') {
        cmd = `lsof -ti:${port} | xargs kill -9 2>/dev/null || true`;
      } else if (platform === 'win32') {
        cmd = `FOR /F "tokens=5" %%a IN ('netstat -aon ^| findstr :${port}') DO taskkill /F /PID %%a`;
      }
      
      if (cmd) {
        await execPromise(cmd);
      }
    } catch (error) {
    }
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
    
    spawn('open', [expandedPath]);
    return { success: true };
  } catch (error) {
    logWithTimestamp('main', `Error opening folder: ${error}`);
    return { success: false, error: error.message };
  }
});

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

  const isDev = process.env.ICORE_DEV === '1';
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
        ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
        : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');
    }

    if (!fs.existsSync(defaultSettingsPath)) {
      throw new Error(`Default settings not found at: ${defaultSettingsPath}`);
    }

    if (!fs.existsSync(managePath)) {
      throw new Error(`Manage binary not found at: ${managePath}`);
    }

    await initializeApp({
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
  
  await killProcessesOnCtpPorts();
  
  if (isDev) {
    // Free port 8000 in case a previous dev server (or its autoreload child)
    // is still holding it, then start Django + worker with hot reload.
    await killProcessOnPort(8000);

    const managePyPath = path.join(__dirname, '..', 'deid', 'manage.py');
    const deidDir = path.join(__dirname, '..', 'deid');
    const env = { ...process.env, ICORE_DEV: '1' };

    logWithTimestamp('main', `Dev mode: launching Django + worker with ${devPython}`);

    // runserver (with autoreload) and the worker (wrapped in Django's
    // autoreloader) restart themselves on .py edits. detached: true puts each
    // in its own process group so we can kill their reloader children on exit.
    serverProcess = spawn(devPython, [managePyPath, 'runserver', '127.0.0.1:8000'], {
      env,
      cwd: deidDir,
      detached: true,
    });
    workerProcess = spawn(devPython, [managePyPath, 'worker'], {
      env,
      cwd: deidDir,
      detached: true,
    });
  } else {
    const manageBinaryPath = app.isPackaged
      ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
      : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');
    
    serverProcess = spawn(manageBinaryPath, ['runserver', '--noreload']);
    workerProcess = spawn(manageBinaryPath, ['worker']);
  }

  serverProcess.stdout.on('data', (data) => {
    logWithTimestamp('server', data.toString().trim());
  });
  
  serverProcess.stderr.on('data', (data) => {
    logWithTimestamp('server', data.toString().trim());
  });
  
  serverProcess.on('error', (err) => {
    logWithTimestamp('server', `Process error: ${err}`);
  });

  workerProcess.stdout.on('data', (data) => {
    logWithTimestamp('worker', data.toString().trim());
  });
  
  workerProcess.stderr.on('data', (data) => {
    logWithTimestamp('worker', data.toString().trim());
  });
  
  workerProcess.on('error', (err) => {
    logWithTimestamp('worker', `Process error: ${err}`);
  });

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
    instrumentAutoUpdater();
    logUpdaterEnvironment();

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
    // checkForUpdatesAndNotify only rejects for the check itself; fold the
    // download promise into the same chain so a failed download is logged too.
    autoUpdater.checkForUpdatesAndNotify()
      .then((result) => result && result.downloadPromise)
      .catch((error) => logUpdaterError('check and download', error));
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
        
        // In dev the processes are detached group leaders running autoreload
        // children; kill the whole group. In prod a plain kill suffices.
        const stopProcess = (proc) => {
          if (!proc) return;
          try {
            if (isDev && proc.pid) {
              process.kill(-proc.pid, 'SIGKILL');
            } else {
              proc.kill();
            }
          } catch (error) {
            // Already gone.
          }
        };

        stopProcess(serverProcess);
        serverProcess = null;
        stopProcess(workerProcess);
        workerProcess = null;

        if (isDev) {
          await killProcessOnPort(8000);
        }

        await killProcessesOnCtpPorts();
        
        if (logStream) {
          logStream.end();
        }
        
        mainWindow.close();
      }
    }
  });
});
