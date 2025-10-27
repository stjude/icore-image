# iCore Image

## Usage Guide
For a detailed usage guide, please refer to the [User Guide](https://innolitics.notion.site/iCore-v0-0-7-User-Manual-160bd5b7a754804287bed990845636cd).

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