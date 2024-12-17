const { app, BrowserWindow, dialog } = require('electron');
const { spawn, exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
let mainWindow;
let serverProcess;
let workerProcess;

// Create AIMINER logs directory in user's home folder if it doesn't exist
const logsDir = path.join(os.homedir(), '.aiminer');
fs.mkdirSync(logsDir, { recursive: true });

// Create log write streams
const serverLogStream = fs.createWriteStream(path.join(logsDir, 'server.log'), { flags: 'a' });
const workerLogStream = fs.createWriteStream(path.join(logsDir, 'worker.log'), { flags: 'a' });
const mainLogStream = fs.createWriteStream(path.join(logsDir, 'main.log'), { flags: 'a' });

function logWithTimestamp(stream, message) {
    const timestamp = new Date().toISOString();
    stream.write(`[${timestamp}] ${message}\n`);
}

function checkDockerRunning() {
    return new Promise((resolve) => {
        exec('/usr/local/bin/docker info', (error) => {
            resolve(!error);
        });
    });
}

app.on('ready', async () => {
    // Check if Docker is running
    const isDockerRunning = await checkDockerRunning();
    if (!isDockerRunning) {
        const message = 'Docker Desktop is not running. Please start Docker Desktop and try again.';
        logWithTimestamp(mainLogStream, `Error: ${message}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Docker Not Running',
            message: message,
            buttons: ['OK']
        }).then(() => {
            app.quit();
        });
        return;
    }

    const managePath = app.isPackaged 
        ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
        : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');
        
    serverProcess = spawn(managePath, ['runserver', '--noreload']);
    workerProcess = spawn(managePath, ['worker']);
    
    // Pipe process outputs to log files
    serverProcess.stdout.pipe(serverLogStream);
    serverProcess.stderr.pipe(serverLogStream);
    serverProcess.on('error', (err) => logWithTimestamp(serverLogStream, `Process error: ${err}`));
    
    workerProcess.stdout.pipe(workerLogStream);
    workerProcess.stderr.pipe(workerLogStream);
    workerProcess.on('error', (err) => logWithTimestamp(workerLogStream, `Process error: ${err}`));
    
    mainWindow = new BrowserWindow({
        width: 1280,
        height: 720,
        webPreferences: {
            preload: path.join(__dirname, 'preload.js'),
            contextIsolation: true,
            webSecurity: false,
        },
    });    
    
    // Log main window events
    mainWindow.webContents.on('console-message', (event, level, message) => {
        logWithTimestamp(mainLogStream, `Console [${level}]: ${message}`);
    });
    
    mainWindow.webContents.on('crashed', () => {
        logWithTimestamp(mainLogStream, 'Renderer process crashed');
    });
    
    mainWindow.on('unresponsive', () => {
        logWithTimestamp(mainLogStream, 'Window became unresponsive');
    });
    
    mainWindow.loadFile(path.join(__dirname, 'loading.html'));
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    mainWindow.loadURL('http://127.0.0.1:8000/imagedeid');

    // Open DevTools
    mainWindow.webContents.openDevTools();

    // Enable back/forward navigation
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
                
                // Add padding to body to prevent content from going under menubar
                document.body.style.paddingTop = '40px';
            `);
        }
    });
    
    mainWindow.on('closed', () => {
        logWithTimestamp(mainLogStream, 'Main window closed');
        if (serverProcess) {
            serverProcess.kill();
            serverProcess = null;
        }
        if (workerProcess) {
            workerProcess.kill();
            workerProcess = null;
        }
        mainWindow = null;
    });
});