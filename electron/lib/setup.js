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

function migrateFromOldLocation(oldLocationDir, newLocationDir) {
  if (!fs.existsSync(oldLocationDir)) {
    return;
  }

  const oldDbPath = path.join(oldLocationDir, 'db.sqlite3');
  const oldSettingsPath = path.join(oldLocationDir, 'settings.json');
  const newDbPath = path.join(newLocationDir, 'db.sqlite3');
  const newSettingsPath = path.join(newLocationDir, 'settings.json');

  if (fs.existsSync(newDbPath) || fs.existsSync(newSettingsPath)) {
    return;
  }

  const shouldMigrateDb = fs.existsSync(oldDbPath);
  const shouldMigrateSettings = fs.existsSync(oldSettingsPath);

  if (shouldMigrateDb || shouldMigrateSettings) {
    fs.mkdirSync(newLocationDir, { recursive: true });
  }

  if (shouldMigrateDb) {
    fs.copyFileSync(oldDbPath, newDbPath);
  }

  if (shouldMigrateSettings) {
    fs.copyFileSync(oldSettingsPath, newSettingsPath);
  }
}

function runMigration(managePath, dbPath, spawnFn = spawn, isDev = false) {
  return new Promise((resolve, reject) => {
    let stdout = '';
    let stderr = '';
    
    let migrateProcess;
    if (isDev) {
      const env = { ...process.env, ICORE_DEV: '1' };
      migrateProcess = spawnFn('python', [managePath, 'migrate'], { env });
    } else {
      migrateProcess = spawnFn(managePath, ['migrate']);
    }
    
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
  const { baseDir, dbPath, settingsPath, defaultSettingsPath, managePath, spawnFn, isDev, oldLocationDir } = config;
  
  ensureDirectories(baseDir);
  
  if (oldLocationDir) {
    const configDir = path.dirname(dbPath);
    migrateFromOldLocation(oldLocationDir, configDir);
  }
  
  ensureDatabase(dbPath);
  mergeSettings(settingsPath, defaultSettingsPath);
  await runMigration(managePath, dbPath, spawnFn, isDev);
}

module.exports = {
  ensureDirectories,
  ensureDatabase,
  mergeSettings,
  migrateFromOldLocation,
  runMigration,
  initializeApp
};

