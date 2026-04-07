# etl-pull

Automatically sync course files from [SNU myETL](https://myetl.snu.ac.kr) (Canvas LMS).

## Install

```bash
pip install -e .
```

This installs `etl-pull` as a global command.

## Usage

Navigate to any directory where you want course files to be downloaded, then:

### Initialize

Set your access token, select courses, and download all files:

```bash
etl-pull init
```

To generate a token, go to **myETL > Profile > Settings > Approved Integrations > + New Access Token**.

Use `--reauth` to re-enter your token:

```bash
etl-pull init --reauth
```

### Pull updates

Download new or updated files:

```bash
etl-pull pull
```

### Check status

Show configured courses and synced file counts:

```bash
etl-pull status
```

## How it works

- Run `etl-pull` from any directory — course folders and config files are created in the current directory.
- Each course gets its own subdirectory, preserving the folder structure from myETL.
- `.etl_state.json` tracks file versions so only new or updated files are downloaded on subsequent pulls.
- `.etl_config.json` stores the token and selected courses.
