import argparse
import os
import yaml
from shutil import which
import subprocess

DOCKER = which('docker') or '/usr/local/bin/docker'


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

    config = yaml.safe_load(open(args.config))

    docker_cmd = [
            DOCKER, 'run', '--rm',
            '-v', f'{os.path.abspath(args.config)}:/config.yml',
            '-v', f'{os.path.abspath(args.input_dir)}:/input',
            '-v', f'{os.path.abspath(args.output_dir)}:/output',
            '-v', f'{os.path.abspath(args.appdata_dir)}:/appdata',
    ]
    if args.module:
        docker_cmd.extend(['-v', f'{os.path.dirname(args.module)}:/modules'])
    if config.get('pacs'):
        for p in config['pacs']:
            if p.get('port'):
                docker_cmd.extend(['-p', f"{p.get('port')}:{p.get('port')}"])
                docker_cmd.extend(['-p', "50001:50001"])
    docker_cmd.append('icore_processor')
    print("Copy and run this command to test:")
    print(' '.join(f'"{arg}"' if ' ' in arg else arg for arg in docker_cmd))
    subprocess.run(docker_cmd)
