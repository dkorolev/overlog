# overlog (node.js module)

## Motivation

An easy to use solution to store and fetch the logs.

## Library and Command-Line Interface

* Storer: ```push(entry)``` adds an entry.
* Fetcher: ```fetch(time_interval)``` returns all entries from certain time interval.
* Fetcher: ```follow()``` mimics ```tail -f```.
* Fetcher: ```fetch_and_follow()``` fetches entries from certain time range and goes into following mode afterwards.

## Interface Details

Directory to store the files has to be provided. Technically, two directories are required: one for intermediate files, append-only, and one for thefinalized files. They will be created as ```$DIR/intermediate/``` and ```$DIR/destination/``` as necessary.

For storer's locking mechanism, either path to a lock file (directory and file) or PubSub channel name (and optional non-default port) is required as well.

## Implementation

### Log Messages

Log entries are JSON objects. They should have the ```"ms"``` field set to ```Date.now()``` at the moment log entry was created.

### Storage

* Append-only intermediate files.
* Atomically rename them into final format.
* If necessary, replay older files at startup.
* Keep first and last timestamps in the filenames for easier access.

### Status Page

In PubSub mode, an HTTP status page is being exposed, check out ```/statusz```.

### PubSub

The tool was designed for the ```fetch_and_follow()``` usecase, that is easiest to access with fetcher's command line flags ```--last_ms_then_follow``` and ```--since_ms_then_follow```.

node.js code performing these actions can be used directly as well.

## Bells and Whistles

* With PubSub and a command-line tool.
* Process-level locked.
* Unit-tested through.
