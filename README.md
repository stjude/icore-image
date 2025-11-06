<p align="center"><img src="deid/static/logo.png" width="48px"/></p>

<p align="center"><b>iCore Image</b></p>

<p align="center"><a href="https://innolitics.notion.site/iCore-v0-0-7-User-Manual-160bd5b7a754804287bed990845636cd">Complete iCore User Guide</a></p>

<p align="center">Medical Image and Text De-identification Tool by St. Jude</p>

<p align="center"><img src="deid/static/screenshot.png" width="600px"/></p>

## Install Dependencies

```
make deps external-deps
```

## Run Development Build

```
make dev
```

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

## Build Signed macOS App

If Apple Developer certificates are set up locally.

Note: the following env variables must be set

- APPLE_ID
- APPLE_APP_SPECIFIC_PASSWORD
- APPLE_TEAM_ID

```
make signed
```

If not, trigger `workflow_dispatch` from Actions tab manually and select `signed` from dropdown.