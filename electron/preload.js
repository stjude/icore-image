const { contextBridge, webUtils } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
    getPathForFile: webUtils.getPathForFile,
});