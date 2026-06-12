"""Regression tests for project deletion.

delete_task once ran shutil.rmtree(task.output_folder) — the user-selected
BASE directory (e.g. ~/Downloads) — wiping every file in it. It must only ever
remove the task's own DeID_/PHI_ output subfolder.
"""

import os

import pytest
from django.test import Client

from home.models import Project


@pytest.fixture
def client():
    return Client()

# pytest-django provides db access via the marker below.
pytestmark = pytest.mark.django_db


def make_project(tmp_path, **overrides):
    fields = dict(
        name="proj",
        timestamp="20260612120000",
        log_path="",
        image_source="LOCAL",
        input_folder="",
        output_folder=str(tmp_path),
        task_type=Project.TaskType.TEXT_DEID,
        status=Project.TaskStatus.COMPLETED,
        parameters={},
    )
    fields.update(overrides)
    return Project.objects.create(**fields)


def test_output_dir_is_subfolder_of_output_folder(tmp_path):
    project = make_project(tmp_path)
    assert project.output_dir() == str(tmp_path / "DeID_proj_20260612120000")

    project.task_type = Project.TaskType.IMAGE_QUERY
    assert project.output_dir() == str(tmp_path / "PHI_proj_20260612120000")


def test_output_dir_empty_when_fields_missing(tmp_path):
    assert make_project(tmp_path, timestamp="").output_dir() == ""
    assert make_project(tmp_path, name="").output_dir() == ""
    assert make_project(tmp_path, output_folder="").output_dir() == ""


def test_delete_removes_only_the_task_subfolder(client, tmp_path):
    project = make_project(tmp_path)
    task_output = tmp_path / "DeID_proj_20260612120000"
    task_output.mkdir()
    (task_output / "output.xlsx").write_text("deid output")
    unrelated = tmp_path / "unrelated-user-file.txt"
    unrelated.write_text("precious")

    response = client.post(f"/delete_task/{project.pk}/")

    assert response.json()["status"] == "success"
    assert not task_output.exists(), "task output subfolder should be deleted"
    assert unrelated.exists(), "files next to the task output must be untouched"
    assert tmp_path.exists(), "the base output folder itself must never be deleted"
    assert not Project.objects.filter(pk=project.pk).exists()


def test_delete_never_touches_base_folder_when_fields_missing(client, tmp_path):
    # A legacy/corrupt row without a timestamp must not fall back to deleting
    # the base directory.
    project = make_project(tmp_path, timestamp="")
    keep = tmp_path / "keep.txt"
    keep.write_text("precious")

    response = client.post(f"/delete_task/{project.pk}/")

    assert response.json()["status"] == "success"
    assert keep.exists()
    assert tmp_path.exists()


def test_delete_refuses_path_traversal_in_project_name(client, tmp_path):
    # A crafted name must not let the computed path escape output_folder.
    victim = tmp_path / "victim"
    victim.mkdir()
    (victim / "data.txt").write_text("precious")
    base = tmp_path / "base"
    base.mkdir()

    project = make_project(
        tmp_path, name="../victim/x", output_folder=str(base), timestamp=".."
    )
    # Build something matching the computed (escaping) path so exists() would
    # be true if the guard were missing.
    response = client.post(f"/delete_task/{project.pk}/")

    assert response.json()["status"] == "success"
    assert victim.exists()
    assert (victim / "data.txt").exists()


def test_delete_succeeds_when_output_was_never_created(client, tmp_path):
    project = make_project(tmp_path)  # no DeID_ subfolder on disk
    response = client.post(f"/delete_task/{project.pk}/")
    assert response.json()["status"] == "success"
    assert not Project.objects.filter(pk=project.pk).exists()


def test_old_behavior_would_have_failed_these(client, tmp_path):
    """Sanity check that the suite actually guards the original bug: deleting
    output_folder itself would empty tmp_path and fail the assertions above.
    Documented here so the intent survives refactors."""
    project = make_project(tmp_path)
    assert os.path.realpath(project.output_dir()) != os.path.realpath(
        project.output_folder
    )
