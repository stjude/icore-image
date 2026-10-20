import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from grammar import UnknownTagError, generate_anonymizer_script


def test_unknown_tag_raises_instead_of_returning_a_script():
    # A misspelled keyword used to come back as the error text in place of the
    # script; the engine then ran with no header rules and left PHI untouched.
    with pytest.raises(UnknownTagError, match="PatinetSex"):
        generate_anonymizer_script(
            "PatientAge\nPatinetSex", "StudyDate", "PatientID", -21, "SITE1"
        )


def test_known_tags_produce_a_script():
    script = generate_anonymizer_script(
        "PatientAge", "StudyDate", "PatientID", -21, "SITE1"
    )
    assert script.startswith("<script>")
    assert 'n="PatientID">@hash(this,10)</e>' in script
