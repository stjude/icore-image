import os
import tempfile
import subprocess
import argparse
from PIL import Image


def create_icns_from_png(png_path, output_dir):
    """
    Convert a PNG image to an ICNS file and save it in the electron/assets folder.

    :param png_path: Path to the source PNG file.
    :return: Path to the generated ICNS file.
    """
    # Create a temporary directory for the iconset
    with tempfile.TemporaryDirectory() as temp_dir:
        iconset_path = os.path.join(temp_dir, "icon.iconset")
        os.makedirs(iconset_path, exist_ok=True)

        # Define the required icon sizes for ICNS
        sizes = [16, 32, 64, 128, 256, 512, 1024]

        for size in sizes:
            output_file = os.path.join(iconset_path, f"icon_{size}x{size}.png")
            output_file_2x = os.path.join(iconset_path, f"icon_{size}x{size}@2x.png")

            # Create 1x version
            with Image.open(png_path) as img:
                img = img.resize((size, size), Image.Resampling.LANCZOS)
                img.save(output_file, 'PNG')

            # Create 2x version (for Retina displays)
            if size * 2 <= 1024:  # Don't create 2x for 1024 (would be 2048)
                with Image.open(png_path) as img:
                    img = img.resize((size * 2, size * 2), Image.Resampling.LANCZOS)
                    img.save(output_file_2x, 'PNG')

        # Convert iconset to ICNS file using iconutil
        icns_file = os.path.join(output_dir, 'icon.icns')
        subprocess.run([
            'iconutil', '-c', 'icns', iconset_path, '-o', icns_file
        ], check=True)

    return icns_file

def parse_arguments():
    """
    Parse command line arguments for PNG path and ICNS path.

    :return: Parsed arguments containing png_path and icns_path.
    """
    parser = argparse.ArgumentParser(description='Convert a PNG image to an ICNS file.')
    parser.add_argument('--png_path', type=str, required=True, help='Path to the source PNG file.')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to the output directory.')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    png_path = args.png_path
    output_dir = args.output_dir
    icns_path = create_icns_from_png(png_path, output_dir)
    print(f"ICNS file created at: {icns_path}")