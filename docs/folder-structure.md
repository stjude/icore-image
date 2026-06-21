# iCore Image — Folder Structure Guide

iCore Image stores files in two main locations:

1. **Your Output Directory** — where de-identified images and results are saved
2. **~/Documents/iCore/** — where the application keeps its internal data, logs, and configuration

---

## 1. Output Directory

A default output folder is configured in **Settings** (defaults to `~/Downloads` if not set). You can also override it per project. Inside the output folder, iCore creates a subdirectory for each job run using the project name and a timestamp.

### Directory naming

The subdirectory prefix depends on the workflow type:

- **`DeID_`** — for de-identification workflows (Image De-id, Text De-id, Export, Single Click)
- **`PHI_`** — for workflows that handle original/identified data (Query & Retrieve, Header Extract, Image Export)

Full pattern: `{Prefix}_{ProjectName}_{Timestamp}/`

### Image De-identification (Local or PACS)

De-identified DICOM files are organized by study and series:

```
Output Folder/
└── DeID_MyStudy_20240215143025/
    ├── DATE-20240115--CT--PID-12345/
    │   ├── SER-1/
    │   │   ├── 1.dcm
    │   │   ├── 2.dcm
    │   │   └── 3.dcm
    │   └── SER-2/
    │       └── 1.dcm
    ├── DATE-20240115--MR--PID-67890/
    │   └── SER-1/
    │       ├── 1.dcm
    │       └── 2.dcm
    └── DATE-20240220--CT--PID-12345/
        └── SER-1/
            └── 1.dcm
```

The study/series folder naming pattern is:

```
DATE-{StudyDate}--{Modality}--PID-{PatientID}/SER-{SeriesNumber}/
```

- **StudyDate** — date of the imaging study (YYYYMMDD)
- **Modality** — imaging type (CT, MR, etc.)
- **PatientID** — the de-identified patient ID
- **SeriesNumber** — series number within the study

### Query & Retrieve (No De-identification)

Retrieved images are placed in an `images/` subdirectory:

```
Output Folder/
└── PHI_MyQuery_20240301091500/
    └── images/
        ├── DATE-20240301--CT--PID-11111/
        │   └── SER-1/
        │       └── *.dcm
        └── DATE-20240301--MR--PID-22222/
            └── SER-1/
                └── *.dcm
```

### Text De-identification

```
Output Folder/
└── DeID_TextProject_20240301091500/
    └── output.xlsx
```

### Header Extraction

```
Output Folder/
└── PHI_HeaderProject_20240301091500/
    └── header_extraction.xlsx
```

### De-identification + Export

Same structure as Image De-identification, but files are also uploaded to Azure Blob Storage at the path `{container}/{project_name}/...`.

---

## 2. Application Data (~/Documents/iCore/)

This directory is created automatically and stores all internal application data.

```
~/Documents/iCore/
│
├── config/
│   ├── db.sqlite3              # Application database (projects, settings)
│   └── settings.json           # Application preferences
│
├── logs/
│   ├── system/
│   │   ├── log.txt             # Main application log (always running)
│   │   └── authentication.log  # Authentication attempt log
│   │
│   ├── 2024-01-15_10-30-00/    # Logs for a specific job run
│   │   ├── run.txt             # Processing log
│   │   └── ctp.txt             # DICOM pipeline log
│   │
│   └── 2024-01-16_14-22-11/    # Logs for another job run
│       ├── run.txt
│       └── ctp.txt
│
└── appdata/
    ├── 2024-01-15_10-30-00/    # Data for a specific job run
    │   ├── metadata.xlsx       # Original DICOM info (before de-id)
    │   ├── deid_metadata.xlsx  # De-identified DICOM info (after de-id)
    │   ├── linker.xlsx         # Maps original IDs → de-identified IDs
    │   ├── failed_queries.csv  # Any queries that failed
    │   └── quarantine/         # Files rejected by filters
    │
    └── 2024-01-16_14-22-11/    # Data for another job run
        ├── metadata.xlsx
        ├── deid_metadata.xlsx
        ├── linker.xlsx
        └── failed_queries.csv
```

### How Jobs Are Tracked

Every time a project is run, iCore creates a **timestamp** (e.g., `2024-01-15_10-30-00`). This same timestamp is used as the folder name in both `logs/` and `appdata/`, linking all artifacts for that run together.

### What Each File Contains

| File | Description |
|------|-------------|
| **db.sqlite3** | Stores all your projects, PACS configurations, and job history |
| **settings.json** | Application preferences and settings |
| **system/log.txt** | Persistent application log from the main process |
| **system/authentication.log** | Log of authentication attempts (AET title and hostname) |
| **run.txt** | Detailed processing log for a specific job |
| **ctp.txt** | DICOM anonymization pipeline log |
| **metadata.xlsx** | Audit log of DICOM studies *before* de-identification |
| **deid_metadata.xlsx** | Audit log of DICOM studies *after* de-identification |
| **linker.xlsx** | Mapping table: original patient IDs to de-identified IDs |
| **failed_queries.csv** | List of PACS queries that failed, with reasons |
| **quarantine/** | DICOM files that were rejected by CTP filters (e.g., non-image files) |
