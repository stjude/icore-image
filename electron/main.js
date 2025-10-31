const { app, BrowserWindow, dialog } = require('electron');
const { spawn, exec } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
let mainWindow;
let serverProcess;
let workerProcess;

// Create ICORE logs directory in user's home folder if it doesn't exist
const logsDir = path.join(os.homedir(), '.icore');
fs.mkdirSync(logsDir, { recursive: true });

// Create log write streams
const serverLogStream = fs.createWriteStream(path.join(logsDir, 'server.log'), { flags: 'a' });
const workerLogStream = fs.createWriteStream(path.join(logsDir, 'worker.log'), { flags: 'a' });
const mainLogStream = fs.createWriteStream(path.join(logsDir, 'main.log'), { flags: 'a' });

function logWithTimestamp(stream, message) {
    const timestamp = new Date().toISOString();
    stream.write(`[${timestamp}] ${message}\n`);
}


function checkAdminPassword() {
    return true; // TODO: remove this (hardcoding for now)
    const adminPasswordPath = path.join(os.homedir(), '.secure', '.config', '.sysdata', 'icapf.txt');
    return fs.existsSync(adminPasswordPath);
}



function installProcessor() {
    if (app.isPackaged) {
        const userProcessorDir = path.join(os.homedir(), 'iCore', 'bin', 'icore_processor');
        const resourceProcessorPath = path.join(process.resourcesPath, 'app', 'assets', 'dist', 'icore_processor');
        
        // Always copy to ensure updates are deployed
        if (fs.existsSync(userProcessorDir)) {
            fs.rmSync(userProcessorDir, { recursive: true, force: true });
        }
        
        fs.mkdirSync(path.dirname(userProcessorDir), { recursive: true });
        fs.cpSync(resourceProcessorPath, userProcessorDir, { recursive: true });
        
        // Make the binary executable
        const binaryPath = path.join(userProcessorDir, 'icore_processor');
        if (fs.existsSync(binaryPath)) {
            fs.chmodSync(binaryPath, '755');
        }
    }
}

async function initializeFirstRun() {
    const settingsPath = path.join(logsDir, 'settings.json');
    if (!fs.existsSync(settingsPath)) {
        const managePath = app.isPackaged 
            ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
            : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');

        // Ensure db.sqlite3 exists before migration
        const dbPath = path.join(logsDir, 'db.sqlite3');
        if (!fs.existsSync(dbPath)) {
            fs.writeFileSync(dbPath, '');
            logWithTimestamp(mainLogStream, 'Created empty db.sqlite3 file');
        }

        // Run migrate command
        await new Promise((resolve, reject) => {
            const migrateProcess = spawn(managePath, ['migrate']);
            migrateProcess.on('close', (code) => {
                if (code === 0) resolve();
                else reject(new Error(`Migration failed with code ${code}`));
            });
        });

        // Copy default settings.json
        const defaultSettingsPath = app.isPackaged
            ? path.join(process.resourcesPath, 'app', 'assets', 'settings.json')
            : path.join(__dirname, 'assets', 'settings.json');
            
        fs.copyFileSync(defaultSettingsPath, settingsPath);
    }

    installProcessor();
}

async function updatePacsSettings() {
    const settingsPath = path.join(logsDir, 'settings.json');
    const defaultSettingsPath = app.isPackaged
        ? path.join(process.resourcesPath, 'app', 'assets', 'settings.json')
        : path.join(__dirname, 'assets', 'settings.json');

    try {
        const defaultSettings = JSON.parse(fs.readFileSync(defaultSettingsPath, 'utf8'));
        if (defaultSettings.pacs_configs) {
            const currentSettings = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
            currentSettings.pacs_configs = defaultSettings.pacs_configs;
            fs.writeFileSync(settingsPath, JSON.stringify(currentSettings, null, 2));
            logWithTimestamp(mainLogStream, 'PACS settings updated successfully');
        }
    } catch (error) {
        logWithTimestamp(mainLogStream, `Failed to update PACS settings: ${error}`);
    }
}

async function updateRcloneConfig() {
    const rcloneDestPath = path.join(logsDir, 'rclone.conf');
    const rcloneSourcePath = app.isPackaged
        ? path.join(process.resourcesPath, 'app', 'assets', 'rclone.conf')
        : path.join(__dirname, 'assets', 'rclone.conf');

    try {
        if (fs.existsSync(rcloneSourcePath)) {
            fs.copyFileSync(rcloneSourcePath, rcloneDestPath);
            logWithTimestamp(mainLogStream, 'Rclone config copied successfully');
        } else {
            // Create empty rclone.conf if source doesn't exist
            fs.writeFileSync(rcloneDestPath, '');
            logWithTimestamp(mainLogStream, 'Empty rclone config created');
        }
    } catch (error) {
        logWithTimestamp(mainLogStream, `Failed to update rclone config: ${error}`);
    }
}

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


    // Initialize first run if needed
    try {
        await initializeFirstRun();
        await updatePacsSettings();
        await updateRcloneConfig();
    } catch (error) {
        logWithTimestamp(mainLogStream, `First run initialization failed: ${error}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Initialization Failed',
            message: 'Failed to initialize application. Please try again.',
            buttons: ['OK']
        }).then(() => {
            app.quit();
        });
        return;
    }

    const adminPasswordExists = checkAdminPassword();
    if (!adminPasswordExists) {
        const message = 'The application is not properly set up. Admin password file is missing.';
        logWithTimestamp(mainLogStream, `Error: ${message}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Setup Error',
            message: message,
            buttons: ['OK']
        }).then(() => {
            app.quit();
        });
        return;
    }


    // Delete existing database and re-migrate
    const dbPath = path.join(os.homedir(), '.icore', 'db.sqlite3');
    if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
    }

    const managePath = app.isPackaged 
        ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
        : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');

    // Ensure db.sqlite3 exists before migration
    if (!fs.existsSync(dbPath)) {
        fs.writeFileSync(dbPath, '');
        logWithTimestamp(mainLogStream, 'Created empty db.sqlite3 file');
    }

    // Run migrate command
    try {
        await new Promise((resolve, reject) => {
            const migrateProcess = spawn(managePath, ['migrate']);
            migrateProcess.on('close', (code) => {
                if (code === 0) resolve();
                else reject(new Error(`Migration failed with code ${code}`));
            });
        });
    } catch (error) {
        logWithTimestamp(mainLogStream, `Database migration failed: ${error}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Database Migration Failed',
            message: 'Failed to initialize database. Please try again.',
            buttons: ['OK']
        }).then(() => {
            app.quit();
        });
        return;
    }
        
    serverProcess = spawn(managePath, ['runserver', '--noreload']);
    workerProcess = spawn(managePath, ['worker']);
    
    // Pipe process outputs to log files
    serverProcess.stdout.pipe(serverLogStream);
    serverProcess.stderr.pipe(serverLogStream);
    serverProcess.on('error', (err) => logWithTimestamp(serverLogStream, `Process error: ${err}`));
    
    workerProcess.stdout.pipe(workerLogStream);
    workerProcess.stderr.pipe(workerLogStream);
    workerProcess.on('error', (err) => logWithTimestamp(workerLogStream, `Process error: ${err}`));
    
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
    
    await new Promise(resolve => setTimeout(resolve, 5000));
    mainWindow.loadURL('http://127.0.0.1:8000/imagedeid');

    // Open DevTools
    // mainWindow.webContents.openDevTools();

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
    
    mainWindow.on('close', async (e) => {
        // Only show dialog if it hasn't been confirmed yet
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
                logWithTimestamp(mainLogStream, 'Main window closed');
                if (serverProcess) {
                    serverProcess.kill();
                    serverProcess = null;
                }
                if (workerProcess) {
                    workerProcess.kill();
                    workerProcess = null;
                }
                mainWindow.close();
            }
        }
    });
});