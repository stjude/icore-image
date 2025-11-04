# iCore Image

## Usage Guide
For a detailed usage guide, please refer to the [User Guide](https://innolitics.notion.site/iCore-v0-0-7-User-Manual-160bd5b7a754804287bed990845636cd).

## Develop macOS App

```
make dev
```

Note: if running for the first time, run `make deps` first.

## Running Test Suite

```
make test
```

Note: you need docker installed and running (some tests set up an Orthanc server)

If you want to run the full test suite in CI, trigger `Full Test` workflow from Actions.

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