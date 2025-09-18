import os
import bcrypt
import argparse
import sys

def get_password_file_path():
    secure_dir = os.path.join(os.path.expanduser('~'), '.secure', '.config', '.sysdata')
    os.makedirs(secure_dir, exist_ok=True)
    return os.path.join(secure_dir, 'icapf.txt')

def check_password_strength(password):
    if len(password) < 20:
        return 1
    if not any(c.islower() for c in password):
        return 1
    if not any(c.isupper() for c in password):
        return 1
    if not any(c.isdigit() for c in password):
        return 1
    if not any(c in "!@#$%^&*()-_=+[{]}\\|;:,<.>/?`~" for c in password):
        return 1
    return 0

def initialize_admin_password(password):
    """Initialize admin password file by copying from source"""
    if check_password_strength(password) != 0:
        print("Password is too weak")
        return
    try:
        # Get source and destination paths
        dest_path = get_password_file_path()
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Copy the password file
        with open(dest_path, 'wb') as dst:
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            dst.write(hashed_password)
    except Exception as e:
        print(f"Error initializing admin password: {str(e)}")

def main():
    parser = argparse.ArgumentParser(
        description="Admin Management Tool for iCore",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'command',
        choices=['password'],
        help='Command to execute'
    )
    parser.add_argument(
        '--password', '-p',
        help='Password to use (for non-interactive mode)'
    )
    args = parser.parse_args()
    try:
        if args.command == 'password':
            initialize_admin_password(args.password)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()