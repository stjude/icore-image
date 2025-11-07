#!/usr/bin/env python3
import json
import sys
import re
import subprocess
from pathlib import Path

def get_repo_root():
    return Path(__file__).parent.parent

def read_package_json():
    package_json_path = get_repo_root() / "electron" / "package.json"
    with open(package_json_path, 'r') as f:
        return json.load(f)

def write_package_json(data):
    package_json_path = get_repo_root() / "electron" / "package.json"
    with open(package_json_path, 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')

def validate_version(version):
    pattern = r'^\d+\.\d+\.\d+$'
    return re.match(pattern, version) is not None

def check_git_status():
    try:
        result = subprocess.run(
            ['git', 'status', '--porcelain'],
            cwd=get_repo_root(),
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def get_current_branch():
    try:
        result = subprocess.run(
            ['git', 'branch', '--show-current'],
            cwd=get_repo_root(),
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/release.py <version>")
        print("Example: python scripts/release.py 0.0.15")
        sys.exit(1)
    
    new_version = sys.argv[1]
    
    if not validate_version(new_version):
        print(f"Error: Invalid version format '{new_version}'")
        print("Version must be in format X.Y.Z (e.g., 0.0.15)")
        sys.exit(1)
    
    package_data = read_package_json()
    current_version = package_data['version']
    
    print(f"iCore Release Tool")
    print(f"==================")
    print(f"Current version: {current_version}")
    print(f"New version:     {new_version}")
    print()
    
    current_branch = get_current_branch()
    if current_branch != 'main':
        print(f"Warning: You are on branch '{current_branch}', not 'main'")
        response = input("Continue anyway? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted")
            sys.exit(0)
    
    git_status = check_git_status()
    if git_status:
        print("Warning: You have uncommitted changes:")
        print(git_status)
        print()
        response = input("Continue anyway? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted")
            sys.exit(0)
    
    print("This will:")
    print(f"  1. Update electron/package.json to version {new_version}")
    print(f"  2. Commit the change")
    print(f"  3. Create and push git tag v{new_version}")
    print(f"  4. Trigger automated release build")
    print()
    
    response = input("Proceed with release? [y/N]: ")
    if response.lower() != 'y':
        print("Aborted")
        sys.exit(0)
    
    package_data['version'] = new_version
    write_package_json(package_data)
    print(f"✓ Updated electron/package.json to version {new_version}")
    
    try:
        subprocess.run(
            ['git', 'add', 'electron/package.json'],
            cwd=get_repo_root(),
            check=True
        )
        subprocess.run(
            ['git', 'commit', '-m', f'Release version {new_version}'],
            cwd=get_repo_root(),
            check=True
        )
        print(f"✓ Committed version change")
    except subprocess.CalledProcessError as e:
        print(f"Error committing changes: {e}")
        sys.exit(1)
    
    tag = f"v{new_version}"
    try:
        subprocess.run(
            ['git', 'tag', '-a', tag, '-m', f'Release {new_version}'],
            cwd=get_repo_root(),
            check=True
        )
        print(f"✓ Created git tag {tag}")
    except subprocess.CalledProcessError as e:
        print(f"Error creating git tag: {e}")
        sys.exit(1)
    
    try:
        subprocess.run(
            ['git', 'push', 'origin', current_branch or 'main'],
            cwd=get_repo_root(),
            check=True
        )
        print(f"✓ Pushed commit to origin")
        
        subprocess.run(
            ['git', 'push', 'origin', tag],
            cwd=get_repo_root(),
            check=True
        )
        print(f"✓ Pushed tag {tag} to origin")
    except subprocess.CalledProcessError as e:
        print(f"Error pushing to remote: {e}")
        print(f"\nYou can manually push with:")
        print(f"  git push origin {current_branch or 'main'}")
        print(f"  git push origin {tag}")
        sys.exit(1)
    
    print()
    print("✓ Release triggered successfully!")
    print(f"  GitHub Actions will now build and publish v{new_version}")
    print(f"  Monitor progress at: https://github.com/stjude/icore-image/actions")

if __name__ == '__main__':
    main()

