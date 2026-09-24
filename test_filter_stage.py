import os

import pytest
from pydicom import Dataset

from pipeline.context import PipelineContext
from pipeline.stages.filter import DicomFilterStage, matches_filters
from test_utils import Fixtures


def _ds(**tags) -> Dataset:
    ds = Dataset()
    for k, v in tags.items():
        setattr(ds, k, v)
    return ds


def f(tag, action, value=""):
    return {"tag": tag, "action": action, "value": value}


def test_general_filters_are_anded():
    ds = _ds(Modality="CT", Manufacturer="SIEMENS")
    assert matches_filters(
        ds, [f("Modality", "equals", "CT"), f("Manufacturer", "contains", "SIEM")], {}
    )
    assert not matches_filters(
        ds, [f("Modality", "equals", "CT"), f("Manufacturer", "contains", "GE")], {}
    )


def test_modality_groups_are_ored_and_anded_with_general():
    modality_filters = {
        "CT": [f("Modality", "equals", "CT"), f("SliceThickness", "isLessThan", "1")],
        "MR": [f("Modality", "equals", "MR")],
    }
    assert matches_filters(_ds(Modality="MR"), [], modality_filters)
    assert matches_filters(
        _ds(Modality="CT", SliceThickness="0.5"), [], modality_filters
    )
    assert not matches_filters(
        _ds(Modality="CT", SliceThickness="3"), [], modality_filters
    )
    assert not matches_filters(_ds(Modality="US"), [], modality_filters)
    assert not matches_filters(
        _ds(Modality="MR", Manufacturer="GE"),
        [f("Manufacturer", "not_containsIgnoreCase", "ge")],
        modality_filters,
    )


def test_case_insensitive_and_negated_actions():
    ds = _ds(StudyDescription="Chest CT w/ Contrast")
    assert matches_filters(
        ds, [f("StudyDescription", "startsWithIgnoreCase", "chest")], {}
    )
    assert matches_filters(
        ds, [f("StudyDescription", "endsWithIgnoreCase", "CONTRAST")], {}
    )
    assert not matches_filters(ds, [f("StudyDescription", "startsWith", "chest")], {})
    assert matches_filters(
        ds, [f("StudyDescription", "not_equalsIgnoreCase", "abdomen")], {}
    )


def test_missing_element_is_empty_string_and_non_numeric_compares_false():
    ds = _ds(Modality="CT")
    assert matches_filters(ds, [f("Manufacturer", "equals", "")], {})
    assert not matches_filters(ds, [f("Manufacturer", "contains", "x")], {})
    assert matches_filters(ds, [f("Manufacturer", "not_contains", "x")], {})
    assert not matches_filters(ds, [f("Manufacturer", "isGreaterThan", "1")], {})
    assert not matches_filters(ds, [f("SliceThickness", "isLessThan", "1")], {})


def test_multivalue_uses_first_value_for_numeric_and_joined_string_for_text():
    ds = _ds(ImageType=["ORIGINAL", "PRIMARY"], PixelSpacing=[0.5, 0.7])
    assert matches_filters(ds, [f("ImageType", "contains", "PRIMARY")], {})
    assert matches_filters(ds, [f("PixelSpacing", "isLessThan", "0.6")], {})


def test_invalid_filters_rejected_at_construction():
    with pytest.raises(ValueError, match="Unknown DICOM keyword"):
        DicomFilterStage([f("NotATag", "equals", "x")], {})
    with pytest.raises(ValueError, match="Unsupported filter action"):
        DicomFilterStage([], {"CT": [f("Modality", "matches", "CT")]})


def test_stage_quarantines_failing_files_in_place(tmp_path):
    input_dir = tmp_path / "out" / "sub"
    input_dir.mkdir(parents=True)
    appdata = tmp_path / "appdata"
    appdata.mkdir()
    Fixtures.create_minimal_dicom(modality="CT").save_as(input_dir / "ct.dcm")
    Fixtures.create_minimal_dicom(modality="MR").save_as(input_dir / "mr.dcm")
    (input_dir / "junk.txt").write_text("not dicom")

    ctx = PipelineContext(
        run_dirs=None,  # ty: ignore[invalid-argument-type]
        output_dir=str(tmp_path / "out"),
        appdata_dir=str(appdata),
        dicom_input_dir=str(tmp_path / "out"),
    )
    DicomFilterStage([], {"CT": [f("Modality", "equals", "CT")]}).execute(ctx)

    assert ctx.images_saved == 1
    assert ctx.images_quarantined == 2
    assert os.listdir(input_dir) == ["ct.dcm"]
    assert sorted(os.listdir(appdata / "quarantine" / "sub")) == ["junk.txt", "mr.dcm"]
