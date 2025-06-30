import os
import shutil
import xml.etree.ElementTree as ET

def create_config(input_dir, output_dir):
    # Read the base_config.xml file
    tree = ET.parse('base_config.xml')
    root = tree.getroot()

    # Find and update the ARCHIVE_DIRECTORY
    archive_import = root.find(".//ArchiveImportService")
    if archive_import is not None:
        archive_import.set('treeRoot', input_dir)

    # Find and update the OUTPUT_DIRECTORY
    file_storage = root.find(".//FileStorageService")
    if file_storage is not None:
        file_storage.set('root', output_dir)

    # Write the modified XML to config.xml
    tree.write('config.xml', encoding='utf-8', xml_declaration=True)

    print(f"Config file created with input directory: {input_dir} and output directory: {output_dir}")

# Example usage
create_config('/path/to/input', '/path/to/output')

