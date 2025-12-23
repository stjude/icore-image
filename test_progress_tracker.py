import json
import os
from pathlib import Path

import pytest

from progress_tracker import ProgressTracker


def test_create_new_tracker():
    """Creates empty tracker"""
    tracker = ProgressTracker()
    
    assert tracker.get_completed_rows() == set()
    assert tracker.get_stats()["total_rows_completed"] == 0
    assert tracker.get_stats()["total_studies_downloaded"] == 0


def test_mark_row_queried():
    """Marks row as queried with study UID"""
    tracker = ProgressTracker()
    
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_row_queried(1, "1.2.3.4.6")
    tracker.mark_row_queried(1, "1.2.3.4.7")  # Multiple studies for same row
    
    # Rows are not considered completed until studies are downloaded
    assert tracker.get_completed_rows() == set()


def test_mark_study_downloaded():
    """Marks study as downloaded"""
    tracker = ProgressTracker()
    
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    assert tracker.is_study_downloaded("1.2.3.4.5") is True
    assert tracker.is_study_downloaded("1.2.3.4.6") is False
    assert tracker.get_completed_rows() == {0}


def test_get_completed_rows():
    """Returns completed row indices"""
    tracker = ProgressTracker()
    
    # Row 0: 1 study
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    # Row 1: 2 studies (both must be downloaded for row to be complete)
    tracker.mark_row_queried(1, "1.2.3.4.6")
    tracker.mark_row_queried(1, "1.2.3.4.7")
    tracker.mark_study_downloaded("1.2.3.4.6")
    
    # Row 2: 1 study
    tracker.mark_row_queried(2, "1.2.3.4.8")
    tracker.mark_study_downloaded("1.2.3.4.8")
    
    # Only rows 0 and 2 are complete (row 1 is missing study 1.2.3.4.7)
    assert tracker.get_completed_rows() == {0, 2}
    
    # Complete row 1
    tracker.mark_study_downloaded("1.2.3.4.7")
    assert tracker.get_completed_rows() == {0, 1, 2}


def test_get_pending_rows():
    """Returns pending rows given total"""
    tracker = ProgressTracker()
    
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    tracker.mark_row_queried(2, "1.2.3.4.7")
    tracker.mark_study_downloaded("1.2.3.4.7")
    
    # Rows 0 and 2 are complete, rows 1, 3, 4 are pending
    pending = tracker.get_pending_rows(total_rows=5)
    assert pending == {1, 3, 4}


def test_save_and_load_progress(tmp_path):
    """Round-trip save/load preserves state"""
    tracker = ProgressTracker()
    
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    tracker.mark_row_queried(1, "1.2.3.4.6")
    tracker.mark_row_queried(1, "1.2.3.4.7")
    tracker.mark_study_downloaded("1.2.3.4.6")
    tracker.mark_study_downloaded("1.2.3.4.7")
    
    # Save progress
    tracker.save_progress(str(tmp_path))
    
    # Load into new tracker
    loaded_tracker = ProgressTracker.load_progress(str(tmp_path))
    
    # Verify state is preserved
    assert loaded_tracker.get_completed_rows() == {0, 1}
    assert loaded_tracker.is_study_downloaded("1.2.3.4.5") is True
    assert loaded_tracker.is_study_downloaded("1.2.3.4.6") is True
    assert loaded_tracker.is_study_downloaded("1.2.3.4.7") is True
    assert loaded_tracker.is_study_downloaded("1.2.3.4.8") is False


def test_is_study_downloaded():
    """Checks study download status"""
    tracker = ProgressTracker()
    
    assert tracker.is_study_downloaded("1.2.3.4.5") is False
    
    tracker.mark_row_queried(0, "1.2.3.4.5")
    assert tracker.is_study_downloaded("1.2.3.4.5") is False
    
    tracker.mark_study_downloaded("1.2.3.4.5")
    assert tracker.is_study_downloaded("1.2.3.4.5") is True


def test_progress_with_multiple_studies_per_row():
    """Handles 1:N row:study mapping"""
    tracker = ProgressTracker()
    
    # Row 0: 3 studies
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_row_queried(0, "1.2.3.4.6")
    tracker.mark_row_queried(0, "1.2.3.4.7")
    
    # Download only 2 of 3 studies
    tracker.mark_study_downloaded("1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.6")
    
    # Row should not be complete yet
    assert tracker.get_completed_rows() == set()
    
    # Download the last study
    tracker.mark_study_downloaded("1.2.3.4.7")
    
    # Now row should be complete
    assert tracker.get_completed_rows() == {0}


def test_get_stats_empty():
    """Stats for empty tracker"""
    tracker = ProgressTracker()
    
    stats = tracker.get_stats()
    assert stats["total_rows_completed"] == 0
    assert stats["total_studies_downloaded"] == 0
    assert stats["total_files_downloaded"] == 0


def test_get_stats_partial():
    """Stats for partially complete tracker"""
    tracker = ProgressTracker()
    
    # Row 0: 1 study
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    # Row 1: 2 studies
    tracker.mark_row_queried(1, "1.2.3.4.6")
    tracker.mark_row_queried(1, "1.2.3.4.7")
    tracker.mark_study_downloaded("1.2.3.4.6")
    tracker.mark_study_downloaded("1.2.3.4.7")
    
    # Row 2: queried but not downloaded yet
    tracker.mark_row_queried(2, "1.2.3.4.8")
    
    stats = tracker.get_stats()
    assert stats["total_rows_completed"] == 2  # Rows 0 and 1
    assert stats["total_studies_downloaded"] == 3
    # Note: file_count tracking was removed, so this will be 0
    assert stats["total_files_downloaded"] == 0


def test_load_progress_file_not_exists(tmp_path):
    """Loading progress when file doesn't exist returns empty tracker"""
    tracker = ProgressTracker.load_progress(str(tmp_path))
    
    assert tracker.get_completed_rows() == set()
    assert tracker.get_stats()["total_rows_completed"] == 0


def test_save_creates_progress_file(tmp_path):
    """Saving progress creates the .icore_progress.json file"""
    tracker = ProgressTracker()
    tracker.mark_row_queried(0, "1.2.3.4.5")
    tracker.mark_study_downloaded("1.2.3.4.5")
    
    tracker.save_progress(str(tmp_path))
    
    progress_file = Path(tmp_path) / ".icore_progress.json"
    assert progress_file.exists()
    
    # Verify JSON structure
    with open(progress_file) as f:
        data = json.load(f)
    
    assert "version" in data
    assert "created_at" in data
    assert "last_updated" in data
    assert "rows" in data
    assert "studies" in data

