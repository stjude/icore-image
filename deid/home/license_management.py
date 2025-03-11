import datetime as dt
import json
import os
from pathlib import Path

import dotenv
import rsa

BASE_DIR = Path(__file__).resolve().parent.parent
dotenv_path = os.path.join(BASE_DIR, ".env")

if os.path.isfile(dotenv_path):
    dotenv.load_dotenv(dotenv_path, override=True)

RSA_N_PUBLIC = (
    "976730265046086586399681182548388178841044392699471211579840396940994841522792485"
    "622181336870723765153963129063789942901879794710911662622259779123419527679315189"
    "902952437635285689487241274370358921042031861072884525564019084192584554456829787"
    "04831086199572672351505298339523234674785686417303068492890669323"
    )
RSA_N_PRIVATE = os.environ["RSA_N_PRIVATE"]
RSA_E = os.environ["RSA_E"]
RSA_D = os.environ["RSA_D"]
RSA_P = os.environ["RSA_P"]
RSA_Q = os.environ["RSA_Q"]


class LicenseValidationError(Exception):
    ...


def generate_license(module: str, expiration: str):
    """
    Generate a license for a given module and expiration date.
    """
    public_key = rsa.PublicKey(int(RSA_N_PUBLIC), int(RSA_E))
    expiration_date =dt.datetime.strptime(expiration, "%Y-%m-%d")
    license_dict = {
        "module": module,
        "expiration": expiration_date.timestamp(),
    }
    license_str = json.dumps(license_dict)
    encrypted_license = rsa.encrypt(license_str.encode(), public_key)

    with open("new_license.txt", "wb") as f:
        f.write(encrypted_license)

def license_invalidation(license_bytes: bytes, module: str) -> str:
    """
    Validate a license for a given module.
    """
    private_key = rsa.PrivateKey(
        int(RSA_N_PRIVATE),
        int(RSA_E),
        int(RSA_D),
        int(RSA_P),
        int(RSA_Q)
    )
    try:
        decrypted_license = rsa.decrypt(license_bytes, private_key).decode()
        license_dict = json.loads(decrypted_license)
        assert isinstance(license_dict, dict), "Decrypted license is not a dictionary."
        license_module = license_dict["module"]
        expiration_date = dt.datetime.fromtimestamp(license_dict["expiration"])
        assert license_module == module, f'This license is not valid for the "{module}" module.'
        assert expiration_date > dt.datetime.now(), "License has expired."
    except AssertionError as e:
        return str(e)
    except KeyError as e:
        return f"Invalid license. Missing the {str(e)} key."
    except Exception as e:
        return f"{type(e)} error occurred while decrypting the license: {str(e)}"

    return ""
