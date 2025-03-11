from django.core.management.base import BaseCommand
from home.license_management import generate_license
from home.models import Project


class Command(BaseCommand):
    help = "Generate a license for a payed module."

    def add_arguments(self, parser):
        parser.add_argument(
            "module",
            type=str,
            choices=[choice[0] for choice in Project.TaskType.choices],
            help="Module name for which the license is being generated.",
        )
        parser.add_argument(
            "expiration",
            type=str,
            help="Expiration date for the license in YYYY-MM-DD format.",
        )

    def handle(self, *args, **options):
        generate_license(options['module'], options['expiration'])
        self.stdout.write(self.style.SUCCESS("License generated successfully at `new_license.txt`."))
