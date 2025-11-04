const fs = require('fs');
const path = require('path');
const os = require('os');
const { ensureDirectories, ensureDatabase, mergeSettings, runMigration, initializeApp } = require('./setup');

describe('ensureDirectories', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('creates the base directory if it does not exist', () => {
    const baseDir = path.join(tempDir, 'iCore');
    ensureDirectories(baseDir);
    expect(fs.existsSync(baseDir)).toBe(true);
  });

  it('creates nested directory structure including config and logs', () => {
    const baseDir = path.join(tempDir, 'iCore');
    ensureDirectories(baseDir);
    const configDir = path.join(baseDir, 'config');
    const logDir = path.join(baseDir, 'logs', 'system');
    expect(fs.existsSync(configDir)).toBe(true);
    expect(fs.existsSync(logDir)).toBe(true);
  });

  it('does not throw if directories already exist', () => {
    const baseDir = path.join(tempDir, 'iCore');
    fs.mkdirSync(baseDir, { recursive: true });
    expect(() => ensureDirectories(baseDir)).not.toThrow();
  });

  it('creates logs/system directory that allows file creation', () => {
    const baseDir = path.join(tempDir, 'iCore');
    ensureDirectories(baseDir);
    const logSystemDir = path.join(baseDir, 'logs', 'system');
    const testLogFile = path.join(logSystemDir, 'authentication.log');
    
    expect(fs.existsSync(logSystemDir)).toBe(true);
    
    fs.writeFileSync(testLogFile, 'test log entry\n');
    expect(fs.existsSync(testLogFile)).toBe(true);
    expect(fs.readFileSync(testLogFile, 'utf8')).toBe('test log entry\n');
  });
});

describe('ensureDatabase', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('creates an empty database file if it does not exist', () => {
    const dbPath = path.join(tempDir, 'db.sqlite3');
    ensureDatabase(dbPath);
    expect(fs.existsSync(dbPath)).toBe(true);
    expect(fs.readFileSync(dbPath, 'utf8')).toBe('');
  });

  it('does not overwrite an existing database', () => {
    const dbPath = path.join(tempDir, 'db.sqlite3');
    const existingContent = 'existing data';
    fs.writeFileSync(dbPath, existingContent);
    ensureDatabase(dbPath);
    expect(fs.readFileSync(dbPath, 'utf8')).toBe(existingContent);
  });
});

describe('mergeSettings', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('creates settings file from default when user settings do not exist', () => {
    const userSettingsPath = path.join(tempDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const defaultSettings = { key1: 'value1', key2: 'value2' };
    
    fs.writeFileSync(defaultSettingsPath, JSON.stringify(defaultSettings));
    mergeSettings(userSettingsPath, defaultSettingsPath);
    
    expect(fs.existsSync(userSettingsPath)).toBe(true);
    const userSettings = JSON.parse(fs.readFileSync(userSettingsPath, 'utf8'));
    expect(userSettings).toEqual(defaultSettings);
  });

  it('preserves existing user settings values', () => {
    const userSettingsPath = path.join(tempDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const userSettings = { key1: 'user-value', key2: 'user-value2' };
    const defaultSettings = { key1: 'default-value', key2: 'default-value2' };
    
    fs.writeFileSync(userSettingsPath, JSON.stringify(userSettings));
    fs.writeFileSync(defaultSettingsPath, JSON.stringify(defaultSettings));
    mergeSettings(userSettingsPath, defaultSettingsPath);
    
    const mergedSettings = JSON.parse(fs.readFileSync(userSettingsPath, 'utf8'));
    expect(mergedSettings.key1).toBe('user-value');
    expect(mergedSettings.key2).toBe('user-value2');
  });

  it('adds new keys from default settings to user settings', () => {
    const userSettingsPath = path.join(tempDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const userSettings = { key1: 'user-value' };
    const defaultSettings = { key1: 'default-value', key2: 'new-default', key3: 'another-new' };
    
    fs.writeFileSync(userSettingsPath, JSON.stringify(userSettings));
    fs.writeFileSync(defaultSettingsPath, JSON.stringify(defaultSettings));
    mergeSettings(userSettingsPath, defaultSettingsPath);
    
    const mergedSettings = JSON.parse(fs.readFileSync(userSettingsPath, 'utf8'));
    expect(mergedSettings.key1).toBe('user-value');
    expect(mergedSettings.key2).toBe('new-default');
    expect(mergedSettings.key3).toBe('another-new');
  });

  it('handles nested objects correctly', () => {
    const userSettingsPath = path.join(tempDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const userSettings = { 
      nested: { userKey: 'user-value' },
      simple: 'value'
    };
    const defaultSettings = { 
      nested: { userKey: 'default', newKey: 'new-default' },
      simple: 'default-value',
      newNested: { key: 'value' }
    };
    
    fs.writeFileSync(userSettingsPath, JSON.stringify(userSettings));
    fs.writeFileSync(defaultSettingsPath, JSON.stringify(defaultSettings));
    mergeSettings(userSettingsPath, defaultSettingsPath);
    
    const mergedSettings = JSON.parse(fs.readFileSync(userSettingsPath, 'utf8'));
    expect(mergedSettings.nested.userKey).toBe('user-value');
    expect(mergedSettings.nested.newKey).toBe('new-default');
    expect(mergedSettings.simple).toBe('value');
    expect(mergedSettings.newNested).toEqual({ key: 'value' });
  });
});

describe('runMigration', () => {
  it('spawns manage process with migrate argument', async () => {
    const managePath = '/path/to/manage';
    const dbPath = '/path/to/db.sqlite3';
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await runMigration(managePath, dbPath, mockSpawn);
    
    expect(mockSpawn).toHaveBeenCalledWith(managePath, ['migrate']);
  });

  it('resolves when migration succeeds', async () => {
    const managePath = '/path/to/manage';
    const dbPath = '/path/to/db.sqlite3';
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await expect(runMigration(managePath, dbPath, mockSpawn)).resolves.toBeUndefined();
  });

  it('rejects when migration fails', async () => {
    const managePath = '/path/to/manage';
    const dbPath = '/path/to/db.sqlite3';
    const mockSpawn = jest.fn(() => ({
      stdout: {
        on: jest.fn()
      },
      stderr: {
        on: jest.fn()
      },
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(1);
        }
      })
    }));

    await expect(runMigration(managePath, dbPath, mockSpawn)).rejects.toThrow('Migration failed with code 1');
  });

  it('captures stdout and stderr from migration process', async () => {
    const managePath = '/path/to/manage';
    const dbPath = '/path/to/db.sqlite3';
    
    let stdoutCallback;
    let stderrCallback;
    
    const mockSpawn = jest.fn(() => ({
      stdout: {
        on: jest.fn((event, callback) => {
          if (event === 'data') {
            stdoutCallback = callback;
          }
        })
      },
      stderr: {
        on: jest.fn((event, callback) => {
          if (event === 'data') {
            stderrCallback = callback;
          }
        })
      },
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          stdoutCallback(Buffer.from('Migration output\n'));
          stderrCallback(Buffer.from('Error: database locked\n'));
          callback(1);
        }
      })
    }));

    try {
      await runMigration(managePath, dbPath, mockSpawn);
      fail('Should have thrown an error');
    } catch (error) {
      expect(error.message).toContain('Migration failed with code 1');
      expect(error.message).toContain('Migration output');
      expect(error.message).toContain('Error: database locked');
    }
  });
});

describe('initializeApp', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('orchestrates all initialization steps', async () => {
    const baseDir = path.join(tempDir, 'iCore');
    const configDir = path.join(baseDir, 'config');
    const dbPath = path.join(configDir, 'db.sqlite3');
    const settingsPath = path.join(configDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const managePath = path.join(tempDir, 'manage');
    
    fs.writeFileSync(defaultSettingsPath, JSON.stringify({ key: 'value' }));
    
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await initializeApp({
      baseDir,
      dbPath,
      settingsPath,
      defaultSettingsPath,
      managePath,
      spawnFn: mockSpawn
    });

    expect(fs.existsSync(baseDir)).toBe(true);
    expect(fs.existsSync(configDir)).toBe(true);
    expect(fs.existsSync(path.join(baseDir, 'logs', 'system'))).toBe(true);
    expect(fs.existsSync(dbPath)).toBe(true);
    expect(fs.existsSync(settingsPath)).toBe(true);
    expect(mockSpawn).toHaveBeenCalledWith(managePath, ['migrate']);
  });

  it('creates all necessary directories for logs including authentication.log location', async () => {
    const baseDir = path.join(tempDir, 'iCore');
    const configDir = path.join(baseDir, 'config');
    const dbPath = path.join(configDir, 'db.sqlite3');
    const settingsPath = path.join(configDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const managePath = path.join(tempDir, 'manage');
    
    fs.writeFileSync(defaultSettingsPath, JSON.stringify({ key: 'value' }));
    
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await initializeApp({
      baseDir,
      dbPath,
      settingsPath,
      defaultSettingsPath,
      managePath,
      spawnFn: mockSpawn
    });

    const logSystemDir = path.join(baseDir, 'logs', 'system');
    const authLogPath = path.join(logSystemDir, 'authentication.log');
    
    expect(fs.existsSync(logSystemDir)).toBe(true);
    
    fs.writeFileSync(authLogPath, '[INFO] Test authentication log\n');
    expect(fs.existsSync(authLogPath)).toBe(true);
  });

  it('handles missing default settings file', async () => {
    const baseDir = path.join(tempDir, 'iCore');
    const configDir = path.join(baseDir, 'config');
    const dbPath = path.join(configDir, 'db.sqlite3');
    const settingsPath = path.join(configDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'nonexistent-settings.json');
    const managePath = path.join(tempDir, 'manage');
    
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await expect(initializeApp({
      baseDir,
      dbPath,
      settingsPath,
      defaultSettingsPath,
      managePath,
      spawnFn: mockSpawn
    })).rejects.toThrow();
  });

  it('migrates old location files before initializing new location', async () => {
    const oldLocationDir = path.join(tempDir, '.icore');
    const baseDir = path.join(tempDir, 'iCore');
    const configDir = path.join(baseDir, 'config');
    const dbPath = path.join(configDir, 'db.sqlite3');
    const settingsPath = path.join(configDir, 'settings.json');
    const defaultSettingsPath = path.join(tempDir, 'default-settings.json');
    const managePath = path.join(tempDir, 'manage');
    
    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.writeFileSync(path.join(oldLocationDir, 'db.sqlite3'), 'old db');
    fs.writeFileSync(path.join(oldLocationDir, 'settings.json'), JSON.stringify({ migrated: true }));
    fs.writeFileSync(defaultSettingsPath, JSON.stringify({ migrated: false, newKey: 'value' }));
    
    const mockSpawn = jest.fn(() => ({
      on: jest.fn((event, callback) => {
        if (event === 'close') {
          callback(0);
        }
      })
    }));

    await initializeApp({
      baseDir,
      dbPath,
      settingsPath,
      defaultSettingsPath,
      managePath,
      spawnFn: mockSpawn,
      oldLocationDir
    });

    expect(fs.existsSync(dbPath)).toBe(true);
    expect(fs.readFileSync(dbPath, 'utf8')).toBe('old db');
    expect(fs.existsSync(settingsPath)).toBe(true);
    
    const mergedSettings = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
    expect(mergedSettings.migrated).toBe(true);
    expect(mergedSettings.newKey).toBe('value');
  });
});

describe('binary path resolution', () => {
  it('validates icorecli path can be resolved relative to manage binary', () => {
    const assetsDir = '/test/assets/dist';
    const manageBinaryPath = path.join(assetsDir, 'manage', 'manage');
    const expectedIcoreCliPath = path.join(assetsDir, 'icorecli', 'icorecli');
    
    const computedIcoreCliPath = path.resolve(
      path.dirname(manageBinaryPath),
      '..',
      'icorecli',
      'icorecli'
    );
    
    expect(computedIcoreCliPath).toBe(expectedIcoreCliPath);
  });
});

describe('migrateFromOldLocation', () => {
  let tempDir;
  let oldLocationDir;
  let newLocationDir;
  const { migrateFromOldLocation } = require('./setup');

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
    oldLocationDir = path.join(tempDir, '.icore');
    newLocationDir = path.join(tempDir, 'Documents', 'iCore', 'config');
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('migrates both db.sqlite3 and settings.json when old location has both files and new location is empty', () => {
    const oldDb = path.join(oldLocationDir, 'db.sqlite3');
    const oldSettings = path.join(oldLocationDir, 'settings.json');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.writeFileSync(oldDb, 'old db content');
    fs.writeFileSync(oldSettings, JSON.stringify({ oldKey: 'oldValue' }));

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newDb)).toBe(true);
    expect(fs.existsSync(newSettings)).toBe(true);
    expect(fs.readFileSync(newDb, 'utf8')).toBe('old db content');
    expect(JSON.parse(fs.readFileSync(newSettings, 'utf8'))).toEqual({ oldKey: 'oldValue' });
    
    expect(fs.existsSync(oldDb)).toBe(true);
    expect(fs.existsSync(oldSettings)).toBe(true);
  });

  it('does not migrate when new location already has db.sqlite3', () => {
    const oldDb = path.join(oldLocationDir, 'db.sqlite3');
    const oldSettings = path.join(oldLocationDir, 'settings.json');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.mkdirSync(newLocationDir, { recursive: true });
    fs.writeFileSync(oldDb, 'old db content');
    fs.writeFileSync(oldSettings, JSON.stringify({ oldKey: 'oldValue' }));
    fs.writeFileSync(newDb, 'new db content');

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.readFileSync(newDb, 'utf8')).toBe('new db content');
    expect(fs.existsSync(newSettings)).toBe(false);
  });

  it('does not migrate when new location already has settings.json', () => {
    const oldDb = path.join(oldLocationDir, 'db.sqlite3');
    const oldSettings = path.join(oldLocationDir, 'settings.json');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.mkdirSync(newLocationDir, { recursive: true });
    fs.writeFileSync(oldDb, 'old db content');
    fs.writeFileSync(oldSettings, JSON.stringify({ oldKey: 'oldValue' }));
    fs.writeFileSync(newSettings, JSON.stringify({ newKey: 'newValue' }));

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newDb)).toBe(false);
    expect(JSON.parse(fs.readFileSync(newSettings, 'utf8'))).toEqual({ newKey: 'newValue' });
  });

  it('does nothing when old location does not exist', () => {
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newDb)).toBe(false);
    expect(fs.existsSync(newSettings)).toBe(false);
  });

  it('migrates only settings.json when only settings.json exists in old location', () => {
    const oldSettings = path.join(oldLocationDir, 'settings.json');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.writeFileSync(oldSettings, JSON.stringify({ oldKey: 'oldValue' }));

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newDb)).toBe(false);
    expect(fs.existsSync(newSettings)).toBe(true);
    expect(JSON.parse(fs.readFileSync(newSettings, 'utf8'))).toEqual({ oldKey: 'oldValue' });
  });

  it('migrates only db.sqlite3 when only db.sqlite3 exists in old location', () => {
    const oldDb = path.join(oldLocationDir, 'db.sqlite3');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.writeFileSync(oldDb, 'old db content');

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newDb)).toBe(true);
    expect(fs.existsSync(newSettings)).toBe(false);
    expect(fs.readFileSync(newDb, 'utf8')).toBe('old db content');
  });

  it('creates new location directory if it does not exist during migration', () => {
    const oldDb = path.join(oldLocationDir, 'db.sqlite3');
    const oldSettings = path.join(oldLocationDir, 'settings.json');
    const newDb = path.join(newLocationDir, 'db.sqlite3');
    const newSettings = path.join(newLocationDir, 'settings.json');

    fs.mkdirSync(oldLocationDir, { recursive: true });
    fs.writeFileSync(oldDb, 'old db content');
    fs.writeFileSync(oldSettings, JSON.stringify({ oldKey: 'oldValue' }));

    expect(fs.existsSync(newLocationDir)).toBe(false);

    migrateFromOldLocation(oldLocationDir, newLocationDir);

    expect(fs.existsSync(newLocationDir)).toBe(true);
    expect(fs.existsSync(newDb)).toBe(true);
    expect(fs.existsSync(newSettings)).toBe(true);
  });

  it('does not throw when old location directory exists but is empty', () => {
    fs.mkdirSync(oldLocationDir, { recursive: true });

    expect(() => migrateFromOldLocation(oldLocationDir, newLocationDir)).not.toThrow();
  });
});

