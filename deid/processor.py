import argparse
import os
import sys
import yaml
import subprocess

ICORE_PROCESSOR_PATH = os.path.abspath(
    os.path.join(os.path.dirname(sys.executable), '..', 'icorecli', 'icorecli')
)


def create_parser():
    parser = argparse.ArgumentParser(
        description='ICORE Image Processing Tool - Process medical images with various modules',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--config', '-c',
        type=str,
        default='config.yml',
        help='Path to configuration YAML file (default: config.yml)'
    )
    
    parser.add_argument(
        '--input-dir',
        type=str,
        required=True,
        help='Input directory path'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        required=True,
        help='Output directory path'
    )
    
    parser.add_argument(
        '--appdata-dir',
        type=str,
        required=True,
        help='Application data directory path'
    )

    parser.add_argument(
        '--module', '-m',
        type=str,
        help='Executable location for a custom module'
    )
    return parser


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()

    cmd = [
        ICORE_PROCESSOR_PATH,
        os.path.abspath(args.config),
        os.path.abspath(args.input_dir),
        os.path.abspath(args.output_dir)
    ]
    
    env = os.environ.copy()
    env['ICORE_APPDATA_DIR'] = os.path.abspath(args.appdata_dir)
    if args.module:
        env['ICORE_MODULES_DIR'] = os.path.dirname(args.module)
    
    shell_cmd = ' '.join(f'"{arg}"' if ' ' in arg else arg for arg in cmd)
    print("Copy and run this command to test:")
    print(shell_cmd)
    
    try:
        result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error output: {e.stderr}")
        raise Exception(f"Process failed with exit code {e.returncode}: {e.stderr}")
