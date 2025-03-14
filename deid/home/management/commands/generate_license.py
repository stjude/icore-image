import json

from django.core.management.base import BaseCommand
from home.license_management import LicenseManager


class Command(BaseCommand):
    help = "Generate a license for a payed module."

    def add_arguments(self, parser):
        parser.add_argument(
            "license_info",
            type=str,
            help=(
                "A list of of information about what modules to license and the"
                "expiration dates of their licenses. Format: "
                '[{"modules": ["module_name", ...], "expiration": "YYYY-MM-DD"}, ...] '
                f"Acceptable module names: {', '.join(LicenseManager.module_names)}"
            ),
        )

    def handle(self, *args, **options):
        license_info = json.loads(options["license_info"])
        LicenseManager().generate_license(license_info)
        self.stdout.write(self.style.SUCCESS("License generated successfully at `new_license.txt`."))
