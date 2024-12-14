const { app, BrowserWindow } = require('electron');
const { spawn } = require('child_process');
const path = require('path');
let mainWindow;
let serverProcess;
let workerProcess;
app.on('ready', async () => {
    const managePath = app.isPackaged 
        ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
        : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');
        
    serverProcess = spawn(managePath, ['runserver', '--noreload']);
    workerProcess = spawn(managePath, ['worker']);
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    mainWindow = new BrowserWindow({
        width: 1280,
        height: 720,
        webPreferences: {
            preload: path.join(__dirname, 'preload.js'),
            contextIsolation: true,
            webSecurity: false,
        },
    });
    mainWindow.loadURL('http://127.0.0.1:8000/imagedeid');
    mainWindow.on('closed', () => {
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