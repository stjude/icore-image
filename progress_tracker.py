import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Set, Optional


PROGRESS_FILE_NAME = ".icore_progress.json"


class ProgressTracker:
    """Tracks progress of DICOM query/retrieval operations at the row level"""
    
    def __init__(self):
        self.version = "1.0"
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.last_updated = self.created_at
        
        # Maps row_index -> {"study_uids": [uid1, uid2], "files_downloaded": int, "completed_at": str}
        self.rows: Dict[int, Dict] = {}
        
        # Maps study_uid -> {"row_index": int, "status": str, "file_count": int}
        self.studies: Dict[str, Dict] = {}
    
    def mark_row_queried(self, row_index: int, study_uid: str):
        """Mark that a row was queried and produced a study UID"""
        if row_index not in self.rows:
            self.rows[row_index] = {
                "status": "queried",
                "study_uids": [],
                "files_downloaded": 0
            }
        
        if study_uid not in self.rows[row_index]["study_uids"]:
            self.rows[row_index]["study_uids"].append(study_uid)
        
        if study_uid not in self.studies:
            self.studies[study_uid] = {
                "row_index": row_index,
                "status": "queried",
                "file_count": 0
            }
        
        self.last_updated = datetime.now(timezone.utc).isoformat()
    
    def mark_study_downloaded(self, study_uid: str):
        """Mark that a study has been successfully downloaded"""
        if study_uid not in self.studies:
            logging.warning(f"Study {study_uid} marked as downloaded but was never queried")
            return
        
        self.studies[study_uid]["status"] = "downloaded"
        
        # Check if all studies for this row are now downloaded
        row_index = self.studies[study_uid]["row_index"]
        if row_index in self.rows:
            row_study_uids = self.rows[row_index]["study_uids"]
            all_downloaded = all(
                self.studies.get(uid, {}).get("status") == "downloaded"
                for uid in row_study_uids
            )
            
            if all_downloaded:
                self.rows[row_index]["status"] = "completed"
                self.rows[row_index]["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        self.last_updated = datetime.now(timezone.utc).isoformat()
    
    def get_completed_rows(self) -> Set[int]:
        """Get set of completed row indices"""
        return {
            row_index
            for row_index, row_data in self.rows.items()
            if row_data.get("status") == "completed"
        }
    
    def get_pending_rows(self, total_rows: int) -> Set[int]:
        """Get set of rows that still need processing"""
        completed = self.get_completed_rows()
        return {i for i in range(total_rows) if i not in completed}
    
    def is_study_downloaded(self, study_uid: str) -> bool:
        """Check if a study has been downloaded"""
        return self.studies.get(study_uid, {}).get("status") == "downloaded"
    
    def get_stats(self) -> Dict:
        """Return progress statistics"""
        completed_rows = self.get_completed_rows()
        downloaded_studies = [
            uid for uid, data in self.studies.items()
            if data.get("status") == "downloaded"
        ]
        
        total_files = sum(
            data.get("file_count", 0)
            for data in self.studies.values()
            if data.get("status") == "downloaded"
        )
        
        return {
            "total_rows_completed": len(completed_rows),
            "total_studies_downloaded": len(downloaded_studies),
            "total_files_downloaded": total_files
        }
    
    def save_progress(self, appdata_dir: str):
        """Save progress to JSON file in appdata directory"""
        progress_file = Path(appdata_dir) / PROGRESS_FILE_NAME
        
        self.last_updated = datetime.now(timezone.utc).isoformat()
        
        data = {
            "version": self.version,
            "created_at": self.created_at,
            "last_updated": self.last_updated,
            "total_rows": len(self.rows),
            "rows": {
                str(k): v for k, v in self.rows.items()
            },
            "studies": self.studies
        }
        
        os.makedirs(appdata_dir, exist_ok=True)
        
        with open(progress_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.debug(f"Progress saved to {progress_file}")
    
    @classmethod
    def load_progress(cls, appdata_dir: str) -> 'ProgressTracker':
        """Load progress from JSON file, or create new tracker if file doesn't exist"""
        progress_file = Path(appdata_dir) / PROGRESS_FILE_NAME
        
        if not progress_file.exists():
            logging.debug(f"No progress file found at {progress_file}, creating new tracker")
            return cls()
        
        try:
            with open(progress_file, 'r') as f:
                data = json.load(f)
            
            tracker = cls()
            tracker.version = data.get("version", "1.0")
            tracker.created_at = data.get("created_at", tracker.created_at)
            tracker.last_updated = data.get("last_updated", tracker.last_updated)
            
            # Convert string keys back to integers for rows
            tracker.rows = {
                int(k): v for k, v in data.get("rows", {}).items()
            }
            
            tracker.studies = data.get("studies", {})
            
            logging.info(f"Loaded progress from {progress_file}: "
                        f"{len(tracker.get_completed_rows())} rows completed, "
                        f"{len([s for s in tracker.studies.values() if s.get('status') == 'downloaded'])} studies downloaded")
            
            return tracker
        
        except (json.JSONDecodeError, ValueError) as e:
            logging.error(f"Failed to load progress file {progress_file}: {e}")
            logging.info("Creating new tracker")
            return cls()

