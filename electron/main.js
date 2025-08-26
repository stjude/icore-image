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

function checkDockerRunning() {
    return new Promise((resolve) => {
        exec('/usr/local/bin/docker info', (error) => {
            resolve(!error);
        });
    });
}

function checkAdminPassword() {
    return true; // TODO: remove this (hardcoding for now)
    const adminPasswordPath = path.join(os.homedir(), '.secure', '.config', '.sysdata', 'icapf.txt');
    return fs.existsSync(adminPasswordPath);
}

async function installDocker() {
    const installScript = app.isPackaged
        ? path.join(process.resourcesPath, 'app', 'assets', 'install-docker.sh')
        : path.join(__dirname, 'assets', 'install-docker.sh');

    return new Promise((resolve, reject) => {
        mainWindow.loadFile(path.join(__dirname, 'loading.html'));
        mainWindow.webContents.executeJavaScript(`
            document.getElementById('status').textContent = 'Setting up Docker...';
        `);

        const dockerLogStream = fs.createWriteStream(path.join(logsDir, 'docker-install.log'), { flags: 'a' });
        const installation = spawn('bash', [installScript]);

        installation.stdout.on('data', (data) => {
            const message = data.toString();
            logWithTimestamp(dockerLogStream, message);
            console.log(message);
        });

        installation.stderr.on('data', (data) => {
            const message = data.toString();
            logWithTimestamp(dockerLogStream, `ERROR: ${message}`);
            console.error(message);
        });

        installation.on('error', (error) => {
            const message = `Failed to start Docker installation: ${error}`;
            logWithTimestamp(dockerLogStream, `ERROR: ${message}`);
            console.error(message);
            reject(error);
        });

        installation.on('close', (code) => {
            if (code === 0) {
                logWithTimestamp(dockerLogStream, 'Docker installation completed successfully');
                resolve();
            } else {
                const message = `Docker installation failed with code ${code}`;
                logWithTimestamp(dockerLogStream, `ERROR: ${message}`);
                reject(new Error(message));
            }
        });
    });
}

async function loadDockerImage() {
    const imagePath = app.isPackaged
        ? path.join(process.resourcesPath, 'app', 'assets', 'icore_processor.tar')
        : path.join(__dirname, 'assets', 'icore_processor.tar');

    return new Promise((resolve, reject) => {
        mainWindow.loadFile(path.join(__dirname, 'loading.html'));
        mainWindow.webContents.executeJavaScript(`
            document.getElementById('status').textContent = 'Loading Docker image...';
        `);
        
        exec(`/usr/local/bin/docker load -i "${imagePath}"`, (error, stdout, stderr) => {
            if (error) {
                reject(error);
            } else {
                resolve();
            }
        });
    });
}

function installProcessor() {
    if (app.isPackaged) {
        const userProcessorDir = path.join(os.homedir(), 'iCore', 'bin');
        const userProcessorPath = path.join(userProcessorDir, 'processor');
        
        if (!fs.existsSync(userProcessorPath)) {
            fs.mkdirSync(userProcessorDir, { recursive: true });
            const resourceProcessorPath = path.join(process.resourcesPath, 'app', 'assets', 'dist', 'processor');
            fs.copyFileSync(resourceProcessorPath, userProcessorPath);
            fs.chmodSync(userProcessorPath, '755');
        }
    }
}

async function initializeFirstRun() {
    const settingsPath = path.join(logsDir, 'settings.json');
    if (!fs.existsSync(settingsPath)) {
        const managePath = app.isPackaged 
            ? path.join(process.resourcesPath, 'app', 'assets', 'dist', 'manage', 'manage')
            : path.join(__dirname, 'assets', 'dist', 'manage', 'manage');

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

    // Install Docker
    try {
        await installDocker();
        logWithTimestamp(mainLogStream, 'Docker installed successfully');
    } catch (error) {
        logWithTimestamp(mainLogStream, `Docker installation failed: ${error}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Docker Installation Failed',
            message: 'Failed to install Docker. Please try again.',
            buttons: ['OK']
        }).then(() => {
            app.quit();
        });
        return;
    }

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

    // Load docker image
    try {
        await loadDockerImage();
        logWithTimestamp(mainLogStream, 'Docker image loaded successfully');
    } catch (error) {
        logWithTimestamp(mainLogStream, `Failed to load docker image: ${error}`);
        dialog.showMessageBox({
            type: 'error',
            title: 'Docker Image Load Failed',
            message: 'Failed to load required docker image. Please try again.',
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
                // Kill all running docker containers
                exec('/usr/local/bin/docker kill $(/usr/local/bin/docker ps -q)', (error) => {
                    if (error) {
                        logWithTimestamp(mainLogStream, `Failed to kill docker containers: ${error}`);
                    } else {
                        logWithTimestamp(mainLogStream, 'All docker containers killed successfully');
                    }
                    mainWindow.close();
                });
            }
        }
    });
});