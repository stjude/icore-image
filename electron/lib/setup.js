const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

function ensureDirectories(baseDir) {
  fs.mkdirSync(baseDir, { recursive: true });
  fs.mkdirSync(path.join(baseDir, 'config'), { recursive: true });
  fs.mkdirSync(path.join(baseDir, 'logs', 'system'), { recursive: true });
}

function ensureDatabase(dbPath) {
  const dbDir = path.dirname(dbPath);
  fs.mkdirSync(dbDir, { recursive: true });
  
  if (!fs.existsSync(dbPath)) {
    fs.writeFileSync(dbPath, '');
  }
}

function deepMerge(target, source) {
  const result = { ...target };
  
  for (const key in source) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      if (target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
        result[key] = deepMerge(target[key], source[key]);
      } else {
        result[key] = source[key];
      }
    } else if (!(key in target)) {
      result[key] = source[key];
    }
  }
  
  return result;
}

function mergeSettings(userSettingsPath, defaultSettingsPath) {
  if (!fs.existsSync(defaultSettingsPath)) {
    throw new Error(`Default settings file not found: ${defaultSettingsPath}`);
  }
  
  const settingsDir = path.dirname(userSettingsPath);
  fs.mkdirSync(settingsDir, { recursive: true });
  
  const defaultSettings = JSON.parse(fs.readFileSync(defaultSettingsPath, 'utf8'));
  
  if (!fs.existsSync(userSettingsPath)) {
    fs.writeFileSync(userSettingsPath, JSON.stringify(defaultSettings, null, 2));
    return;
  }
  
  const userSettings = JSON.parse(fs.readFileSync(userSettingsPath, 'utf8'));
  const mergedSettings = deepMerge(userSettings, defaultSettings);
  fs.writeFileSync(userSettingsPath, JSON.stringify(mergedSettings, null, 2));
}

function runMigration(managePath, dbPath, spawnFn = spawn) {
  return new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    
    const migrateProcess = spawnFn(managePath, ['migrate']);
    
    if (migrateProcess.stdout) {
      migrateProcess.stdout.on('data', (data) => {
        stdout += data.toString();
      });
    }
    
    if (migrateProcess.stderr) {
      migrateProcess.stderr.on('data', (data) => {
        stderr += data.toString();
      });
    }
    
    migrateProcess.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        const errorMsg = `Migration failed with code ${code}\nStdout: ${stdout}\nStderr: ${stderr}`;
        reject(new Error(errorMsg));
      }
    });
  });
}

async function initializeApp(config) {
  const { baseDir, dbPath, settingsPath, defaultSettingsPath, managePath, spawnFn } = config;
  
  ensureDirectories(baseDir);
  ensureDatabase(dbPath);
  mergeSettings(settingsPath, defaultSettingsPath);
  await runMigration(managePath, dbPath, spawnFn);
}

module.exports = {
  ensureDirectories,
  ensureDatabase,
  mergeSettings,
  runMigration,
  initializeApp
};

