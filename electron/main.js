const { app, BrowserWindow, dialog } = require('electron');
const { spawn, exec } = require('child_process');
const path = require('path');
let mainWindow;
let serverProcess;
let workerProcess;

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
        dialog.showMessageBox({
            type: 'error',
            title: 'Docker Not Running',
            message: 'Docker Desktop is not running. Please start Docker Desktop and try again.',
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
    
    await new Promise(resolve => setTimeout(resolve, 5000));
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