const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

/**
 * afterSign hook for electron-builder
 * Pre-signs all Mach-O binaries in the PyInstaller bundle to avoid
 * "bundle format is ambiguous" errors during automatic signing
 */
async function afterSign(context) {
  const { electronPlatformName, appOutDir, packager } = context;
  
  // Only run on macOS
  if (electronPlatformName !== 'darwin') {
    console.log('Skipping afterSign hook - not macOS platform');
    return;
  }

  console.log('Running afterSign hook for macOS...');
  
  // Get signing identity from environment
  const signingIdentity = process.env.CSC_NAME || process.env.CSC_IDENTITY_AUTO_DISCOVERY;
  if (!signingIdentity) {
    console.log('No signing identity found, skipping afterSign hook');
    return;
  }

  const appName = packager.appInfo.productFilename;
  const appPath = path.join(appOutDir, `${appName}.app`);
  const assetsPath = path.join(appPath, 'Contents', 'Resources', 'app', 'assets');
  const icoreProcessorPath = path.join(assetsPath, 'dist', 'icore_processor');
  const entitlementsPath = path.join(__dirname, 'entitlements.mac.plist');

  // Check if icore_processor bundle exists
  if (!fs.existsSync(icoreProcessorPath)) {
    console.log('icore_processor bundle not found, skipping afterSign hook');
    return;
  }

  console.log(`Signing binaries in: ${icoreProcessorPath}`);

  try {
    // Find and sign all Mach-O binaries in the PyInstaller bundle
    await signMachOBinaries(icoreProcessorPath, signingIdentity, entitlementsPath);
    console.log('afterSign hook completed successfully');
  } catch (error) {
    console.error('afterSign hook failed:', error.message);
    throw error;
  }
}

/**
 * Recursively find and sign all Mach-O binaries
 */
async function signMachOBinaries(dirPath, signingIdentity, entitlementsPath) {
  const items = fs.readdirSync(dirPath);
  
  for (const item of items) {
    const itemPath = path.join(dirPath, item);
    const stat = fs.statSync(itemPath);
    
    if (stat.isDirectory()) {
      // Recursively process subdirectories first (depth-first)
      await signMachOBinaries(itemPath, signingIdentity, entitlementsPath);
    } else if (stat.isFile()) {
      // Check if it's a Mach-O binary
      try {
        const fileOutput = execSync(`file "${itemPath}"`, { encoding: 'utf8' });
        if (fileOutput.includes('Mach-O')) {
          console.log(`  Signing: ${path.relative(process.cwd(), itemPath)}`);
          
          // Sign the binary
          const signCommand = [
            'codesign',
            '--force',
            '--sign', `"${signingIdentity}"`,
            '--options', 'runtime',
            '--entitlements', `"${entitlementsPath}"`,
            '--timestamp',
            `"${itemPath}"`
          ].join(' ');
          
          execSync(signCommand, { stdio: 'pipe' });
        }
      } catch (error) {
        // If file command fails or it's not a Mach-O binary, skip
        if (!error.message.includes('file: cannot open')) {
          console.warn(`  Warning: Could not check file type for ${itemPath}: ${error.message}`);
        }
      }
    }
  }
}

module.exports = afterSign;
