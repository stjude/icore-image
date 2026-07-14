# Local Azurite for export testing

A lightweight [Azurite](https://github.com/Azure/Azurite) (Azure Blob Storage
emulator) instance for exercising iCore's export path — the Azure upload that
runs after QC approval in the IMAGINE Workflow / Image Deidentification &
Export flows, and the standalone Secure Data Transfer module — against a local
blob store instead of real Azure.

The export stage (`pipeline/stages/export.py`) already recognizes a
`127.0.0.1`/`localhost` SAS URL as Azurite and points rclone at the emulator, so
no code changes are needed — just a running Azurite and a SAS URL.

> Makefile shortcuts: `make azurite` (start), `make azurite-sas` (print a SAS
> URL, or `make azurite-sas ARGS='--list'` to list blobs), `make azurite-down`
> (stop). The full commands are below.

## 1. Start Azurite

```bash
docker compose -f testing/azurite/docker-compose.yml up -d
```

Blob service listens on `http://127.0.0.1:10000` using Azurite's fixed
development account `devstoreaccount1`.

## 2. Get a SAS URL

```bash
uv run python testing/azurite/sas_url.py
```

This creates a container (`icore-export` by default) and prints a ready-to-paste
SAS URL, e.g.:

```
http://127.0.0.1:10000/devstoreaccount1/icore-export?se=...&sp=rwdl&sig=...
```

Options: `--container <name>`, `--port <n>`, `--hours <n>`.

## 3. Run an export

Start iCore (`make dev`), create a project in a workflow that exports (IMAGINE
Workflow, Image Deidentification / Export, or Secure Data Transfer), and paste
the SAS URL into the **SAS URL** field. After de-identification finishes and you
**Approve** in the QC viewer, the Export tab uploads to the local Azurite and
shows live progress.

## 4. Verify the upload

```bash
uv run python testing/azurite/sas_url.py --list
```

Lists the blobs in the container — you should see `<project_name>/…` paths
mirroring the de-identified output tree.

## Teardown

```bash
docker compose -f testing/azurite/docker-compose.yml down      # stop
docker compose -f testing/azurite/docker-compose.yml down -v   # stop + wipe data
```
