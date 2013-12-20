# overlog

## Motivation

An easy to use solution to store and fetch the logs.

## Interface

* Storer: ```push(entry)```
* Fetcher: ```fetch(time_interval)```

## Interface details

Directory to store the files in will have to be provided.

For the storer, path to a lock file is necessary as well.

## Implementation

Log storage:

* Append-only intermediate files.
* Atomically rename them into final format.
* If necessary, replay older files at startup.
* Keep first and last timestamps in the filenames for easier access.

## Bells and Whistles

* Comes with a command-line tool.
* Unit-tested through.
