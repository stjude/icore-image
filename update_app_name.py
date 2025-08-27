#!/usr/bin/env python3
"""
Script to update the application display name.
Usage: python update_app_name.py "New App Name"
"""

import os
import re
import sys
import argparse

def update_file_content(file_path, old_name, new_name):
    """Update content in a single file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Only replace the display name, not internal references
        if old_name in content:
            content = content.replace(old_name, new_name)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated: {file_path}")
            return True
        return False
    except Exception as e:
        print(f"Error updating {file_path}: {e}")
        return False

def update_display_files(project_root, old_name, new_name):
    """Update files that display the application name to users"""
    display_files = [
        'deid/templates/base.html',  # Page title and header
        'deid/templates/settings/admin_settings.html',  # Admin settings page
    ]
    
    for display_file in display_files:
        file_path = os.path.join(project_root, display_file)
        if os.path.exists(file_path):
            update_file_content(file_path, old_name, new_name)

def update_package_json(project_root, new_name):
    """Update package.json with new app name and build scripts"""
    package_json_path = os.path.join(project_root, 'electron', 'package.json')
    if os.path.exists(package_json_path):
        try:
            with open(package_json_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Create safe versions of the new name
            new_name_safe = re.sub(r'[^a-zA-Z0-9]', '', new_name.lower())
            new_name_no_spaces = new_name.replace(' ', '')
            
            # Update the name field
            content = re.sub(r'"name":\s*"[^"]*"', f'"name": "{new_name_safe}"', content)
            
            # Update electron-packager app name in build scripts
            content = re.sub(r'electron-packager \. iCore', f'electron-packager . {new_name_no_spaces}', content)
            
            with open(package_json_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated: {package_json_path}")
        except Exception as e:
            print(f"Error updating package.json: {e}")

def update_dmg_script(project_root, new_name):
    """Update the DMG creation script"""
    dmg_script_path = os.path.join(project_root, 'electron', 'create-dmg.sh')
    if os.path.exists(dmg_script_path):
        try:
            with open(dmg_script_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            new_name_no_spaces = new_name.replace(' ', '')
            
            # Update all references to iCore in the DMG script
            content = content.replace('iCore.dmg', f'{new_name_no_spaces}.dmg')
            content = content.replace('iCore.app', f'{new_name_no_spaces}.app')
            content = content.replace('iCore Installer', f'{new_name} Installer')
            
            with open(dmg_script_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated: {dmg_script_path}")
        except Exception as e:
            print(f"Error updating DMG script: {e}")

def update_package_json_for_default(project_root, app_name):
    """Update package.json for default app name (iCore)"""
    package_json_path = os.path.join(project_root, 'electron', 'package.json')
    if os.path.exists(package_json_path):
        try:
            with open(package_json_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Set the name to "icore" (lowercase, no spaces)
            content = re.sub(r'"name":\s*"[^"]*"', '"name": "icore"', content)
            
            # Update electron-packager app name to "iCore" (original case)
            content = re.sub(r'electron-packager \. [^ ]*', 'electron-packager . iCore', content)
            
            with open(package_json_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated package.json for default name: {package_json_path}")
        except Exception as e:
            print(f"Error updating package.json: {e}")

def update_dmg_script_for_default(project_root, app_name):
    """Update DMG script for default app name (iCore)"""
    dmg_script_path = os.path.join(project_root, 'electron', 'create-dmg.sh')
    if os.path.exists(dmg_script_path):
        try:
            with open(dmg_script_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Ensure all references are set to iCore
            content = content.replace('noticore.dmg', 'iCore.dmg')
            content = content.replace('noticore.app', 'iCore.app')
            content = content.replace('noticore Installer', 'iCore Installer')
            
            with open(dmg_script_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Updated DMG script for default name: {dmg_script_path}")
        except Exception as e:
            print(f"Error updating DMG script: {e}")

def update_main_js_app_name(project_root, new_name):
    """Update app.setName() in main.js if it exists, or add it if it doesn't"""
    main_js_path = os.path.join(project_root, 'electron', 'main.js')
    if os.path.exists(main_js_path):
        try:
            with open(main_js_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Update app.setName() if it exists
            if 'app.setName(' in content:
                content = re.sub(r'app\.setName\([\'"][^\'"]*[\'"]\)', f'app.setName(\'{new_name}\')', content)
                with open(main_js_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"Updated app.setName in: {main_js_path}")
            else:
                # Add app.setName() after the app.on('ready') line
                if 'app.on(\'ready\'' in content:
                    # Find the app.on('ready') line and add app.setName() right after it
                    lines = content.split('\n')
                    for i, line in enumerate(lines):
                        if 'app.on(\'ready\'' in line:
                            # Add app.setName() on the next line
                            lines.insert(i + 1, f'    app.setName(\'{new_name}\');')
                            break
                    
                    content = '\n'.join(lines)
                    with open(main_js_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Added app.setName to: {main_js_path}")
        except Exception as e:
            print(f"Error updating main.js: {e}")

def create_default_settings(project_root, new_name):
    """Create default settings with the new app name"""
    settings_dir = os.path.join(project_root, 'deid', 'home', 'settings')
    os.makedirs(settings_dir, exist_ok=True)
    
    settings_file = os.path.join(settings_dir, 'settings.json')
    settings_content = f'''{{
    "custom_app_name": "{new_name}"
}}'''
    
    with open(settings_file, 'w', encoding='utf-8') as f:
        f.write(settings_content)
    print(f"Created default settings: {settings_file}")

def main():
    parser = argparse.ArgumentParser(description='Update application display name')
    parser.add_argument('new_name', help='New application name')
    parser.add_argument('--project-root', default='.', help='Project root directory')
    
    args = parser.parse_args()
    
    project_root = os.path.abspath(args.project_root)
    new_name = args.new_name
    old_name = 'iCore'
    
    print(f"Updating application display name from '{old_name}' to '{new_name}'")
    print(f"Project root: {project_root}")
    print()
    
    # Only update if the name is actually different
    if new_name != old_name:
        # Update display files
        update_display_files(project_root, old_name, new_name)
        update_package_json(project_root, new_name)
        update_dmg_script(project_root, new_name)
        update_main_js_app_name(project_root, new_name)
        create_default_settings(project_root, new_name)
        
        print()
        print("Application display name update completed!")
    else:
        # For default name, just ensure package.json has the correct name
        update_package_json_for_default(project_root, new_name)
        update_dmg_script_for_default(project_root, new_name)
        update_main_js_app_name(project_root, new_name)
        create_default_settings(project_root, new_name)
        
        print()
        print("Default application name configuration completed!")

if __name__ == '__main__':
    main() 