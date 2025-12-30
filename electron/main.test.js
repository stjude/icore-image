const fs = require('fs');
const path = require('path');
const os = require('os');

const mockApp = {
  on: jest.fn(),
  isPackaged: false,
  quit: jest.fn()
};

const mockDialog = {
  showMessageBox: jest.fn().mockResolvedValue({ response: 0 })
};

const mockIpcMain = {
  handle: jest.fn()
};

let mockWindowInstance;
const mockBrowserWindow = jest.fn().mockImplementation(() => {
  mockWindowInstance = {
    loadFile: jest.fn(),
    loadURL: jest.fn(),
    setBackgroundColor: jest.fn(),
    on: jest.fn(),
    close: jest.fn(),
    webContents: {
      on: jest.fn(),
      executeJavaScript: jest.fn(),
      navigationHistory: {
        canGoBack: false
      }
    },
    isClosing: false
  };
  return mockWindowInstance;
});

const mockSpawn = jest.fn().mockImplementation(() => ({
  stdout: { on: jest.fn() },
  stderr: { on: jest.fn() },
  on: jest.fn(),
  kill: jest.fn()
}));

const mockExec = jest.fn((cmd, callback) => {
  if (callback) callback(null, '', '');
});

const mockInitializeApp = jest.fn().mockResolvedValue(undefined);

const originalFs = jest.requireActual('fs');
const mockFsExistsSync = jest.fn().mockReturnValue(true);

jest.mock('electron', () => ({
  app: mockApp,
  BrowserWindow: mockBrowserWindow,
  dialog: mockDialog,
  ipcMain: mockIpcMain
}));

jest.mock('child_process', () => ({
  spawn: mockSpawn,
  exec: mockExec
}));

jest.mock('./lib/setup', () => ({
  initializeApp: mockInitializeApp
}));

jest.mock('fs', () => ({
  ...jest.requireActual('fs'),
  existsSync: jest.fn().mockReturnValue(true)
}));

describe('main.js', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
    jest.clearAllMocks();
    mockInitializeApp.mockResolvedValue(undefined);
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('registers ready event handler on app', () => {
    jest.isolateModules(() => {
      require('./main');
      expect(mockApp.on).toHaveBeenCalledWith('ready', expect.any(Function));
    });
  });

  it('calls initializeApp with correct paths during ready event', async () => {
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    expect(mockInitializeApp).toHaveBeenCalled();
    const callArgs = mockInitializeApp.mock.calls[0][0];
    expect(callArgs.baseDir).toContain('Documents/iCore');
    expect(callArgs.dbPath).toContain('config/db.sqlite3');
    expect(callArgs.settingsPath).toContain('config/settings.json');
    expect(callArgs.defaultSettingsPath).toContain('assets/settings.json');
    expect(callArgs.managePath).toContain('manage');
    
    jest.restoreAllMocks();
  });

  it('loads root URL which redirects to appropriate module', async () => {
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    expect(mockWindowInstance.loadURL).toHaveBeenCalledWith('http://127.0.0.1:8000/');
    
    jest.restoreAllMocks();
  });

  it('spawns server and worker processes after initialization', async () => {
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    expect(mockSpawn).toHaveBeenCalledWith(
      expect.stringContaining('manage'),
      ['runserver', '--noreload']
    );
    
    expect(mockSpawn).toHaveBeenCalledWith(
      expect.stringContaining('manage'),
      ['worker']
    );
    
    jest.restoreAllMocks();
  });

  it('shows error dialog and quits when initialization fails', async () => {
    mockInitializeApp.mockRejectedValueOnce(new Error('Test error'));
    
    const consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    await readyHandler();
    
    expect(mockDialog.showMessageBox).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'error',
        title: 'Initialization Failed'
      })
    );
    
    consoleErrorSpy.mockRestore();
  });

  it('creates log stream in correct directory', async () => {
    const logDir = path.join(os.homedir(), 'Documents', 'iCore', 'logs', 'system');
    
    if (fs.existsSync(logDir)) {
      fs.rmSync(logDir, { recursive: true, force: true });
    }
    
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    expect(fs.existsSync(logDir)).toBe(true);
    
    jest.restoreAllMocks();
  });
});

describe('logging functionality', () => {
  let tempDir;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'icore-test-'));
    jest.clearAllMocks();
    mockInitializeApp.mockResolvedValue(undefined);
  });

  afterEach(() => {
    if (fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('sets up logging directory structure', async () => {
    const logDir = path.join(os.homedir(), 'Documents', 'iCore', 'logs', 'system');
    
    if (fs.existsSync(logDir)) {
      fs.rmSync(logDir, { recursive: true, force: true });
    }
    
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    expect(fs.existsSync(logDir)).toBe(true);
    
    jest.restoreAllMocks();
  });
});

describe('process cleanup', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockInitializeApp.mockResolvedValue(undefined);
  });

  it('kills server and worker processes on window close', async () => {
    const mockServerProcess = {
      stdout: { on: jest.fn() },
      stderr: { on: jest.fn() },
      on: jest.fn(),
      kill: jest.fn()
    };
    
    const mockWorkerProcess = {
      stdout: { on: jest.fn() },
      stderr: { on: jest.fn() },
      on: jest.fn(),
      kill: jest.fn()
    };
    
    mockSpawn.mockImplementationOnce(() => mockServerProcess)
            .mockImplementationOnce(() => mockWorkerProcess);
    
    let readyHandler;
    let closeHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      mockBrowserWindow.mockImplementation(() => {
        mockWindowInstance = {
          loadFile: jest.fn(),
          loadURL: jest.fn(),
          setBackgroundColor: jest.fn(),
          close: jest.fn(),
          on: jest.fn((event, handler) => {
            if (event === 'close') {
              closeHandler = handler;
            }
          }),
          webContents: {
            on: jest.fn(),
            executeJavaScript: jest.fn(),
            navigationHistory: {
              canGoBack: false
            }
          },
          isClosing: false
        };
        return mockWindowInstance;
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    const execCallsBeforeClose = mockExec.mock.calls.length;
    
    await closeHandler({ preventDefault: jest.fn() });
    
    expect(mockServerProcess.kill).toHaveBeenCalled();
    expect(mockWorkerProcess.kill).toHaveBeenCalled();
    
    const execCallsAfterClose = mockExec.mock.calls.length;
    expect(execCallsAfterClose).toBeGreaterThan(execCallsBeforeClose);
    
    const closeExecCalls = mockExec.mock.calls.slice(execCallsBeforeClose);
    const ctpPorts = [50000, 50001, 50010, 50020, 50030, 50040, 50050, 50060, 50070, 50080, 50090];
    const portsCheckedOnClose = closeExecCalls.filter(call => {
      const cmd = call[0];
      return ctpPorts.some(port => cmd.includes(`:${port}`));
    });
    
    expect(portsCheckedOnClose.length).toBeGreaterThan(0);
    
    jest.restoreAllMocks();
  });
});

describe('CTP process cleanup on startup', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockInitializeApp.mockResolvedValue(undefined);
  });

  it('kills processes using CTP ports before starting workers', async () => {
    let readyHandler;
    
    jest.isolateModules(() => {
      mockApp.on.mockImplementation((event, handler) => {
        if (event === 'ready') {
          readyHandler = handler;
        }
      });
      
      require('./main');
    });
    
    jest.spyOn(global, 'setTimeout').mockImplementation((cb) => {
      cb();
      return null;
    });
    
    await readyHandler();
    
    const ctpPorts = [50000, 50001, 50010, 50020, 50030, 50040, 50050, 50060, 50070, 50080, 50090];
    
    expect(mockExec).toHaveBeenCalled();
    const execCalls = mockExec.mock.calls;
    
    const portsChecked = execCalls.filter(call => {
      const cmd = call[0];
      return ctpPorts.some(port => cmd.includes(`:${port}`));
    });
    
    expect(portsChecked.length).toBeGreaterThan(0);
    
    jest.restoreAllMocks();
  });
});
