import base64
import datetime as dt
import json
import os
from typing import Optional

import dotenv
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization.ssh import (
    SSHPrivateKeyTypes,
    SSHPublicKeyTypes,
)
from django.conf import settings
from home.constants import APP_DIR, LICENSES_PATH, RSA_PUBLIC_KEY_PATH
from home.models import Project

if settings.DEBUG:
    dotenv_path = os.path.join(APP_DIR, ".env")
    if os.path.isfile(dotenv_path):
        dotenv.load_dotenv(dotenv_path, override=True)


class LicenseValidationError(Exception):
    ...


class LicenseManager:
    _public_key: Optional[SSHPublicKeyTypes] = None
    _private_key: Optional[SSHPrivateKeyTypes] = None
    _licenses: dict = {}
    _newly_licensed_modules: list[str] = []
    module_names = [choice[0] for choice in Project.TaskType.choices]
    paid_modules = [Project.TaskType.TEXT_EXTRACT]

    @property
    def licenses(self) -> dict:
        if (updated_licenses := self._licenses_dict_update()) is not None:
            self._licenses = updated_licenses
        return self._licenses

    @property
    def public_key(self) -> SSHPublicKeyTypes:
        if self._public_key is None:
            with open(RSA_PUBLIC_KEY_PATH, "rb") as f:
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
    
    def _licenses_dict_update(self) -> Optional[dict]:
        """
        Check if the licenses dictionary is not up-to-date with what's in the
        licenses.json file. This happens when separate process modifies the file.
        If the file is not up-to-date, return the updated dictionary.
        Returns:
            Optional[dict]: The updated licenses dictionary if it has changed, None
            otherwise.
        """
        if os.path.isfile(LICENSES_PATH):
            with open(LICENSES_PATH, "r") as f:
                licenses = json.load(f)
        else:
            licenses = {}
            os.makedirs(os.path.dirname(LICENSES_PATH), exist_ok=True)
            with open(LICENSES_PATH, "w") as f:
                json.dump(licenses, f)
        return None if self._licenses == licenses else licenses

    def _check_module_license(self, module:str, module_license_dict: dict) -> dict:
        """
        Validate a license.

        Returns:
            dict: The license dictionary if valid.
        """
        try:
            assert isinstance(module_license_dict, dict), (
                "Invalid license: improper formatting."
            )
            assert module in self.module_names, (
                f'Invalid module name in license: "{module}".'
            )
            assert (expiration_date := dt.datetime.fromtimestamp(
                module_license_dict["expiration"]
            )) > dt.datetime.now(), (
                "Invalid license: expired"
                f" {(dt.datetime.now() - expiration_date).days}"
                " days ago."
            )

            signature_str: str = module_license_dict["signature"]
            signature = base64.b64decode(signature_str)
            signed_bytes = json.dumps(
                {
                    k: module_license_dict[k] 
                    for k in module_license_dict if k != "signature"
                }
            ).encode()
            self.public_key.verify(
                signature,
                signed_bytes,
                padding.PKCS1v15(),
                hashes.SHA256()
            )
        except AssertionError as e:
            raise LicenseValidationError(str(e))
        except KeyError:
            raise LicenseValidationError("Invalid license: improper formatting.")
        except InvalidSignature:
            raise LicenseValidationError("Invalid license: authentication failed.")
        except Exception:
            raise LicenseValidationError(
                "An error occurred while validating the license,"
                "possibly due to tampering."
            )

        return module_license_dict

    def _generate_module_license(self, expiration: str) -> dict:
        """
        Generate a license for a given module and expiration date.
        """
        expiration_date = dt.datetime.strptime(expiration, "%Y-%m-%d")
        module_license_dict = {
            "expiration": expiration_date.timestamp(),
        }
        license_str = json.dumps(module_license_dict)
        signature = self.private_key.sign(
            license_str.encode(),
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        signature_str = base64.b64encode(signature).decode("utf-8")
        module_license_dict["signature"] = signature_str
        return module_license_dict

    def generate_license(
            self,
            license_info: list[dict],
            output_path: str = "new_license.json"
        ) -> None:
        """
        Generate a license file based on the modules and expiration dates given in
        the license_info list.
        """
        license_dict = {}
        for module_group in license_info:
            modules = module_group["modules"]
            expiration = module_group["expiration"]
            for module in modules:
                if module not in self.module_names:
                    raise LicenseValidationError(
                        f"Invalid module: {module}. Valid modules are: "
                        f'{", ".join(self.module_names)}'
                    )
                if module in self._newly_licensed_modules:
                    raise LicenseValidationError(
                        f"Duplicate module license: {module}. Be sure that each module"
                        "only appears once in the license info list."
                    )
                license_dict[module] = self._generate_module_license(expiration)
                self._newly_licensed_modules.append(module)

        with open(output_path, "wb") as f:
            f.write(json.dumps(license_dict).encode())
    
    def validate_license(self, license_dict: dict) -> dict:
        for module, module_license in license_dict.items():
            self._check_module_license(module, module_license)
        return license_dict
    
    def add_license(self, license_dict: dict) -> None:
        licenses = self.licenses
        for module in license_dict:
            licenses[module] = license_dict[module]

        with open(LICENSES_PATH, "w") as f:
            json.dump(licenses, f)

    def module_license_is_valid(self, module: Project.TaskType) -> dict:
        module_str = str(module)

        try:
            module_license_dict = self.licenses[module_str]
        except KeyError:
            raise LicenseValidationError("No relevant license found.")

        return self._check_module_license(module_str, module_license_dict)

LICENSE_MANAGER = LicenseManager()
