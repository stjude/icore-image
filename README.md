# aiminer

## Docker image

The `aiminer` docker image is currently in development. It will contain functionality for all modules.

The docker image will be used as follows:

```
docker run --rm -it -v $(pwd)/config.yml:/config.yml -v $(pwd)/input:/input -v $(pwd)/output:/output aiminer
```

* `config.yml`: Configuration file for `aiminer`. You'll have to create this for each job based on user input. See the example `config.yml` in the repo or the module specific examples below.
* `input`: Directory containing DICOMs or a single `input.xlsx` file depending on the job.
* `output`: Directory where pipeline outputs and logs will be stored.

If you're using imageqr or imagedeid from PACS, you'll have to expose the appropriate ports in the docker command above with the -p option.

## How to use from django app

1. Create the `config.yml` file based on user input.
2. Call the docker command (ie: `subprocess.run(["docker", "run"...], ...)`).
3. Logs will be written to `output/logs.txt`. You can read that in real time. Progress and errors will be reported there.
4. Do any post-processing and cleanup necessary.

## How to build the image

```
docker image rm -f aiminer
docker build . --tag aiminer
```

### headerqr

`config.yml` template:

```
module: headerqr
pacs_ip: 192.168.0.1
pacs_port: 4242
pacs_aet: ORTHANC
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
pacs_ip: 192.168.0.1
pacs_port: 4242
pacs_aet: ORTHANC
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
pacs_ip: 192.168.0.1
pacs_port: 4242
pacs_aet: ORTHANC
acc_col: AccessionNumber
ctp_filters: |
    !ImageType.contains("INVALID")
    + !InstanceNumber.equals("1")
```

## Orthanc setup

For imagedeid from PACS and imageqr, you'll need to set up a local DICOM server. Orthanc is one option.

1. Start Orthanc using the config file `orthanc-config.json` in the repo.

```
./Orthanc orthanc-config.json
```

2. Upload some DICOMs to the Orthanc PACS from [](http://localhost:8042/app/explorer.html#upload).

3. Create the excel file `input/input.xlsx`.

4. Create `config.yml` file and run the docker image. Note the `pacs_ip` will have to be `host.docker.internal` if you're trying to reference localhost on the host machine from the container.

```
docker run --rm -it -v $(pwd)/config.yml:/config.yml -v $(pwd)/input:/input -v $(pwd)/output:/output -p 50001:50001 -p 4242:4242 aiminer

```