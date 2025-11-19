<p align="center"><img src="deid/static/logo.png" width="48px"/></p>

<p align="center"><b>iCore Image</b></p>

<p align="center"><a href="https://innolitics.notion.site/iCore-v0-0-7-User-Manual-160bd5b7a754804287bed990845636cd">Complete iCore User Guide</a></p>

<p align="center">Medical Image and Text De-identification Tool by St. Jude</p>

<p align="center"><img src="deid/static/screenshot.png" width="600px"/></p>

## Install Dependencies
iCore requires Python 3.12. For development it is recommended to use a virtual environment.
```
python -m venv venv
source venv/bin/activate
```
For MacOS, homebrew is also required. It can be installed with the following command:
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
Then install the dependencies.

```
make deps external-deps
```

## Run Development Build

```
make dev
```
Note: Electron uses Chromium to lauch the development build. It is recommended to clear the Chrome cache to avoid hanging of the application.

## Running Test Suite

```
make test
```

Note: you need docker installed and running (some tests set up an Orthanc server)

If you want to run the test suite in CI, trigger `Test Suite` workflow from Actions.

## Build macOS App

```
make
```