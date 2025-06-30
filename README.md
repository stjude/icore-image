# iCore Image

## Usage Guide
For a detailed usage guide, please refer to the [User Guide](https://innolitics.notion.site/iCore-v0-0-7-User-Manual-160bd5b7a754804287bed990845636cd).


## Docker image

The `icore_processor` docker image is currently in development. It will contain functionality for all modules.

The docker image will be used as follows:

```
docker run --rm -it -v $(pwd)/config.yml:/config.yml -v $(pwd)/input:/input -v $(pwd)/output:/output -v $(pwd)/appdata:/appdata -vicore_processor
```

* `config.yml`: Configuration file for `icore_processor`. You'll have to create this for each job based on user input. See the example `config.yml` in the repo or the module specific examples below.
* `input`: Directory containing DICOMs or a single `input.xlsx` file depending on the job.
* `output`: Directory where pipeline outputs will be stored.
* `appdata`: Directory where the appdata and logs will be stored.

If you're using imageqr or imagedeid from PACS, you'll have to expose the appropriate ports in the docker command above with the -p option.

If using an external module the modules will need to be mounted as a volume using the -v option.

```
-v $(pwd)/modules:/modules
```

## How to use from command line

1. Create the `config.yml` file based on user input.
2. Call the docker command (ie: `subprocess.run(["docker", "run"...], ...)`).
3. Logs will be written to `output/logs.txt`. You can read that in real time. Progress and errors will be reported there.
4. Do any post-processing and cleanup necessary.

## How to build the image

```
docker image rm -f icore_processor
docker build . --tag icore_processor
```

### headerqr

`config.yml` template:

```
module: headerqr
pacs_ip: 192.168.0.1
pacs_port: 4242
pacs_aet: ORTHANC
application_aet: ICORE
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
application_aet: ICORE
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
ctp_lookup_table: |-
  None
ctp_anonymizer: |-
  <script>
     <p t="DATEINC">-3210</p>
     <p t="NOTICE1">IMPORTANT: Be sure to review Series Descriptions for PHI!!</p>
     <p t="NOTICE2">IMPORTANT: Tags inside of sequences may contain PHI.</p>
     <p t="PROFILENAME">CTP Clinical Trial Default</p>
     <p t="PROJECTNAME">Project</p>
     <p t="SITENAME">SiteName</p>
     <p t="SITEID">1</p>
     <p t="TRIALNAME">Trial</p>
     <p t="SUBJECT">Subject</p>
     <p t="UIDROOT">1.2.840.113654.2.70.1</p>
  </script>
```

`config.yml` template for PACS de-identification:

```
module: imagedeid
pacs_ip: 192.168.0.1
pacs_port: 4242
pacs_aet: ORTHANC
application_aet: ICORE
acc_col: AccessionNumber
ctp_filters: |
    !ImageType.contains("INVALID")
    + !InstanceNumber.equals("1")
ctp_lookup_table: |-
  None
ctp_anonymizer: |-
  <script>
     <p t="DATEINC">-3210</p>
     <p t="NOTICE1">IMPORTANT: Be sure to review Series Descriptions for PHI!!</p>
     <p t="NOTICE2">IMPORTANT: Tags inside of sequences may contain PHI.</p>
     <p t="PROFILENAME">CTP Clinical Trial Default</p>
     <p t="PROJECTNAME">Project</p>
     <p t="SITENAME">SiteName</p>
     <p t="SITEID">1</p>
     <p t="TRIALNAME">Trial</p>
     <p t="SUBJECT">Subject</p>
     <p t="UIDROOT">1.2.840.113654.2.70.1</p>
  </script>
```

### image_export

`config.yml` template:

```
module: image_export
rclone_config: $(pwd)/rclone.conf
storage_location: rclone_storage_name
project_name: project_name
```

### external module
Configuration file dependent upon the external module.

## Running the Django App in Development Mode

1. Install the dependencies

```
pip install -r requirements.txt
```

2. The app is run using 2 commands:

```
python manage.py runserver
python manage.py worker
```

The first command will start the django app on port 50001.
The second command will start the worker process to collect and process tasks using the `icore_processor` docker image.

3. The app will be available at `http://localhost:50001`.

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
docker run --rm -it -v $(pwd)/config.yml:/config.yml -v $(pwd)/input:/input -v $(pwd)/output:/output -v $(pwd)/appdata:/appdata -p 50001:50001 -p 4242:4242 icore_processor

```

## Packaging the app
The app can be packaged using the build.sh script.
```
./build.sh
```

The packaged app will be in the electron/iCore-darwin-arm64 directory.
