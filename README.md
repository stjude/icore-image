# aiminer

## Docker image

The `aiminer` docker image is currently in development. It will contain functionality for all modules.

The docker image will be used as follows:

```
docker run --rm -v ./my-config.yml:config.yml -v ./my-input:input -v ./my-output:output aiminer
```

* `config.yml`: Configuration file for `aiminer`. You'll have to create this for each job based on user input. See examples below.
* `input`: Directory containing DICOMs or a single `input.xlsx` file depending on the job.
* `output`: Directory where pipeline outputs and logs will be stored.

### headerqr

`config.yml` template:

```
module: headerqr
pacs_ip: localhost
pacs_port: 4242
pacs_ae: ORTHANC
mrn_col: PatientID
date_col: StudyDate
date_window: 3
ctp_filters: |
    Modality.contains("CT")
    * NumberOfSeriesRelatedInstances.isGreaterThan(1)
    * NumberOfSeriesRelatedInstances.isLessThan(2000)
```

### headerextract

`config.yml` template:

```
module: headerextract
ctp_filters: |
    Modality.contains("CT")
    * NumberOfSeriesRelatedInstances.isGreaterThan(1)
    * NumberOfSeriesRelatedInstances.isLessThan(2000)
```

### imageqr

`config.yml` template:

```
module: imageqr
pacs_ip: localhost
pacs_port: 4242
pacs_ae: ORTHANC
acc_col: AccessionNumber
ctp_filters: |
    Modality.contains("CT")
    * NumberOfSeriesRelatedInstances.isGreaterThan(1)
    * NumberOfSeriesRelatedInstances.isLessThan(2000)
```

### imagedeid

`config.yml` template for local de-identification:

```
module: imagedeid
ctp_filters: |
    !ImageType.contains("INVALID")
    + !InstanceNumber.equals("1")
```

`config.yml` template for PACS de-identification:

```
module: imagedeid
pacs_ip: localhost
pacs_port: 4242
pacs_ae: ORTHANC
acc_col: AccessionNumber
ctp_filters: |
    !ImageType.contains("INVALID")
    + !InstanceNumber.equals("1")
```
