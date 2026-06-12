"""Emit the OpenAPI document for the job-creation API.

The Pydantic request models in ``home.api_models`` are the source of truth;
this command serializes them into ``frontend/openapi.json``, from which
``npm run generate:api`` produces the TypeScript declarations the frontend
payload builders are typed against. Output is deterministic (sorted keys) so
CI can detect drift between the models and the committed artifacts.
"""

import json
import os

from django.core.management.base import BaseCommand
from pydantic.json_schema import models_json_schema

from home.api_models import RUN_ENDPOINTS, RunResponse

OUTPUT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), *[os.pardir] * 4, "frontend", "openapi.json")
)


def build_openapi_document():
    models = [(model, "validation") for _, model, _ in RUN_ENDPOINTS]
    models.append((RunResponse, "serialization"))
    _, definitions = models_json_schema(
        models, ref_template="#/components/schemas/{model}"
    )

    paths = {}
    for path, model, operation_id in RUN_ENDPOINTS:
        paths[path] = {
            "post": {
                "operationId": operation_id,
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "$ref": f"#/components/schemas/{model.__name__}"
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "Job created",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/RunResponse"}
                            }
                        },
                    }
                },
            }
        }

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "iCore job-creation API",
            "description": "Generated from home/api_models.py — do not edit.",
            "version": "1.0.0",
        },
        "paths": paths,
        "components": {"schemas": definitions["$defs"]},
    }


class Command(BaseCommand):
    help = "Generate frontend/openapi.json from the Pydantic API models."

    def handle(self, *args, **options):
        document = build_openapi_document()
        with open(OUTPUT_PATH, "w") as f:
            json.dump(document, f, indent=2, sort_keys=True)
            f.write("\n")
        self.stdout.write(f"Wrote {OUTPUT_PATH}")
