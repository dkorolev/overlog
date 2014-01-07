# overlog (node.js module)

## Motivation

An easy to use solution to store and fetch the logs.

## Library and Command-Line Interface

* Storer: ```push(entry)```
* Fetcher: ```fetch(time_interval)```

## PubSub Interface

* [ ] Storer: Single-flag configuration to publish log messages as they arrive.
* [ ] Fetcher: Supports persistent fetching.

## Interface Details

Directory to store the files in has to be provided.

For storer's locking mechanism, either path to a lock file (directory and file) or PubSub channel name (and optional non-default port) is required as well.

## Implementation

Log storage:

* Append-only intermediate files.
* Atomically rename them into final format.
* If necessary, replay older files at startup.
* Keep first and last timestamps in the filenames for easier access.

## Bells and Whistles

* With PubSub and a command-line tool.
* Process-level locked.
* Unit-tested through.
