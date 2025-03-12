import base64
import datetime as dt
import json
import os
from pathlib import Path
from typing import Optional

import dotenv
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization.ssh import (
    SSHPrivateKeyTypes,
    SSHPublicKeyTypes,
)
from home.models import Project

APP_DIR = Path(__file__).resolve().parent
dotenv_path = os.path.join(APP_DIR, ".env")

if os.path.isfile(dotenv_path):
    dotenv.load_dotenv(dotenv_path, override=True)


class LicenseValidationError(Exception):
    ...


class LicenseManager:
    _public_key: Optional[SSHPublicKeyTypes] = None
    _private_key: Optional[SSHPrivateKeyTypes] = None
    _licenses: Optional[dict] = None
    licenses_path = os.path.join(APP_DIR, "licenses.json")

    @property
    def licenses(self) -> dict:
        if self._licenses is None:
            if os.path.isfile(self.licenses_path):
                with open(self.licenses_path, "r") as f:
                    self._licenses = json.load(f)
                    assert isinstance(self._licenses, dict), "licenses.json is not a dictionary."
            else:
                self._licenses = {}

                with open(self.licenses_path, "w") as f:
                    json.dump(self._licenses, f)
        return self._licenses

    @property
    def public_key(self) -> SSHPublicKeyTypes:
        if self._public_key is None:
            with open("icore_rsa.pub", "rb") as f:
                public_key_bytes = f.read()

            self._public_key = serialization.load_ssh_public_key(public_key_bytes)
        return self._public_key

    @property
    def private_key(self) -> SSHPrivateKeyTypes:
        if self._private_key is None:
            private_key_str = os.environ["RSA_PRIVATE_KEY"]
            self._private_key = serialization.load_ssh_private_key(
                private_key_str.encode(),
                password=None
            )
        return self._private_key

    def generate_license(self, module: str, expiration: str, output_path: str = "new_license.json") -> None:
        """
        Generate a license for a given module and expiration date.
        """
        expiration_date = dt.datetime.strptime(expiration, "%Y-%m-%d")
        license_dict = {
            "module": module,
            "expiration": expiration_date.timestamp(),
        }
        license_str = json.dumps(license_dict)
        signature = self.private_key.sign(
            license_str.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        signature_str = base64.b64encode(signature).decode("utf-8")
        license_dict["signature"] = signature_str
        signed_license = json.dumps(license_dict).encode()

        with open(output_path, "wb") as f:
            f.write(signed_license)

    def license_is_valid(self, license_dict: dict) -> dict:
        """
        Validate a license.

        Returns:
            dict: The license dictionary if valid.
        """
        try:
            assert isinstance(license_dict, dict), "Submitted license is not a json dictionary."

            expiration_date = dt.datetime.fromtimestamp(license_dict["expiration"])
            signature_str: str = license_dict["signature"]
            signature = base64.b64decode(signature_str)
            module = license_dict["module"]
            signed_bytes = json.dumps(
                {k:license_dict[k] for k in license_dict if k != "signature"}
                ).encode()

            assert module in [choice[0] for choice in Project.TaskType.choices], "Invalid module."
            assert expiration_date > dt.datetime.now(), "License has expired."
            self.public_key.verify(
                signature,
                signed_bytes,
                padding.PKCS1v15(),
                hashes.SHA256()
            )
        except AssertionError as e:
            raise LicenseValidationError(str(e))
        except KeyError as e:
            raise LicenseValidationError(f"Invalid license. Missing the {str(e)} key.")
        except Exception as e:
            raise LicenseValidationError(f"{type(e)} error occurred while validating the license: {str(e)}")

        return license_dict
    
    def add_license(self, license_dict: dict) -> None:
        self.licenses[license_dict["module"]] = license_dict

        with open(self.licenses_path, "w") as f:
            json.dump(self.licenses, f)

    def module_license_is_valid(self, module: str) -> None:
        if (module_license := self.licenses.get(module)) is None:
            raise LicenseValidationError("No license found for this module.")
        else:
            self.license_is_valid(module_license)
