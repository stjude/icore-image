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

  it('validates processor path can be resolved relative to manage binary', () => {
    const assetsDir = '/test/assets/dist';
    const manageBinaryPath = path.join(assetsDir, 'manage', 'manage');
    const expectedProcessorPath = path.join(assetsDir, 'processor');
    
    const computedProcessorPath = path.resolve(
      path.dirname(manageBinaryPath),
      '..',
      'processor'
    );
    
    expect(computedProcessorPath).toBe(expectedProcessorPath);
  });
});

