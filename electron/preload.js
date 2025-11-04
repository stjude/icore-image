const { contextBridge, webUtils, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('electronAPI', {
    getPathForFile: webUtils.getPathForFile,
    openFolder: (folderPath) => ipcRenderer.invoke('open-folder', folderPath),
});