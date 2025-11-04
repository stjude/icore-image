const { app, BrowserWindow, dialog, ipcMain } = require('electron');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { initializeApp } = require('./lib/setup');

let mainWindow;
let serverProcess;
let workerProcess;

const baseDir = path.join(os.homedir(), 'Documents', 'iCore');
const configDir = path.join(baseDir, 'config');
const logsDir = path.join(baseDir, 'logs', 'system');
const logFilePath = path.join(logsDir, 'log.txt');
const dbPath = path.join(configDir, 'db.sqlite3');
const settingsPath = path.join(configDir, 'settings.json');

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
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      webSecurity: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'loading.html'));

  const isDev = process.env.ICORE_DEV === '1';

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
      isDev
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
  
  if (isDev) {
    const managePyPath = path.join(__dirname, '..', 'deid', 'manage.py');
    const deidDir = path.join(__dirname, '..', 'deid');
    const env = { ...process.env, ICORE_DEV: '1' };
    
    serverProcess = spawn('python', [managePyPath, 'runserver'], { env, cwd: deidDir });
    workerProcess = spawn('python', [managePyPath, 'worker'], { env, cwd: deidDir });
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

  await new Promise(resolve => setTimeout(resolve, 5000));
  mainWindow.loadURL('http://127.0.0.1:8000/imagequery');

  mainWindow.webContents.on('did-finish-load', () => {
    if (mainWindow.webContents.navigationHistory.canGoBack) {
      mainWindow.setBackgroundColor('#FFFFFF');
      mainWindow.webContents.executeJavaScript(`
        const menuBar = document.createElement('div');
        menuBar.style.position = 'fixed';
        menuBar.style.top = '0';
        menuBar.style.left = '0';
        menuBar.style.width = '100%';
        menuBar.style.height = '40px';
        menuBar.style.backgroundColor = '#f8f9fa';
        menuBar.style.borderBottom = '1px solid #dee2e6';
        menuBar.style.zIndex = '9999';
        menuBar.style.display = 'flex';
        menuBar.style.alignItems = 'center';
        menuBar.style.padding = '0 10px';

        const backButton = document.createElement('button');
        backButton.innerHTML = '←';
        backButton.style.padding = '5px 10px';
        backButton.style.marginRight = '10px';
        backButton.style.border = 'none';
        backButton.style.backgroundColor = 'transparent';
        backButton.style.cursor = 'pointer';
        backButton.onclick = () => window.history.back();
        
        menuBar.appendChild(backButton);
        document.body.appendChild(menuBar);
        
        document.body.style.paddingTop = '40px';
      `);
    }
  });

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
        
        if (serverProcess) {
          serverProcess.kill();
          serverProcess = null;
        }
        
        if (workerProcess) {
          workerProcess.kill();
          workerProcess = null;
        }
        
        if (logStream) {
          logStream.end();
        }
        
        mainWindow.close();
      }
    }
  });
});
