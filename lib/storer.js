// Keeps messages in an intermediate dir first, atomically moves them to the destination one.
//
// Requirements for the entries to be logged:
// 1) Must be JSON.
// 2) Must have the "ms" field with Date.now() output, the milliseconds.
// 3) Logging must be close to real-time: entry "ms" field populated by the caller must be close to "Date.now()".
//    This is important for the fetcher in order to not have to go through extra files.
//
// The easiest way to test this module is to run it as a command-line application.
// Please refer to the shell test for mode details.
//
// Uses file-based lock (via 'npm install pidlock') or a pubsub channel (via 'npm install faye').
//
// Intermediate filename schema: ('tmp:' + random_hash + '.log').
// Destination  filename schema: ('YYYY-MM-DD-HH-MM' + ':' + random_hash + ':' + firstentry_ms + ':' + lastentry_ms + '.log).
//
// Where "random_hash" is five random lowercase latin characters.

var _ = require('underscore');
var assert = require('assert');
var dateformat = require('dateformat');
var fs = require('fs');
var pidlock = require('pidlock');
var mkpath = require('mkpath');
var path = require('path');
var synchronized = require('synchronized');
var http = require('http');
var faye = require('faye');
var express = require('express');

var CircularBuffer = require('./CircularBuffer');

var prefix = 'DEBUG ';

function safeParseJson(string) {
    try {
        return JSON.parse(string);
    } catch (exception) {
        return null;
    }
};

function generateRandomHash() {
    var result = '';
    for (var i = 0; i < 5; ++i) {
        result += (Math.floor(Math.random() * 26) + 10).toString(36);
    }
    return result;
};

var mock_ms = 0;
function Impl(params) {
    this.params = _.clone(params);
    if (this.params.verbose) {
        this.log = function(message) {
            console.log(prefix + message);
        };
    } else {
        this.log = function() {
        };
    }
    assert(_.isString(this.params.intermediate_dir));
    assert(fs.existsSync(this.params.intermediate_dir));
    assert(_.isString(this.params.destination_dir));
    assert(fs.existsSync(this.params.destination_dir));
    if (!params.mock_time) {
        this.now = Date.now;
    } else {
        this.now = function() { return mock_ms; };
    }
    this.start_time_ms = this.now();
    this.recent_entries = new CircularBuffer();
    this.recent_entries_accepted_ms = new CircularBuffer();
    this.recent_files = new CircularBuffer();
    this.replayIntermediateFiles();
    this.total_consumed = 0;
    this.total_replayed = 0;
    this.total_file_renames = 0;
    this.ensureFileIsOpen();
    this.installTimeBoundaryFlusher();
};

Impl.prototype.ensureFileIsOpen = function() {
    if (_.isUndefined(this.fd)) {
        this.fn = path.join(this.params.intermediate_dir, 'tmp:' + generateRandomHash() + '.log');
        this.log('Intermediate "' + this.fn + '": opening.');
        this.fd = fs.openSync(this.fn, 'a');
        this.file_start_time_ms = this.now();
        this.log('Intermediate "' + this.fn + '": opened, fd=' + this.fd);
        this.uncommitted_entries = [];
        this.ms = {
            earliest: null,
            latest: null,
        };
        this.current_file_formatted_date = this.getFormattedDate();
    }
};

Impl.prototype.getFormattedDate = function() {
    return dateformat(this.now(), this.params.filename_dateformat || 'yyyy-mm-dd-HH');
};

Impl.prototype.ensureFileIsFlushed = function(force_write) {
    if (!_.isUndefined(this.fd)) {
        this.log('Intermediate "' + this.fn + '": closing.');
        fs.fsyncSync(this.fd);
        fs.closeSync(this.fd);
        delete this.fd;
        this.log('Intermediate "' + this.fn + '": closed.');
        if (force_write || this.uncommitted_entries.length > 0) {
            var destination_fn = path.join(
                this.params.destination_dir,
                this.getFormattedDate() +
                ':' + generateRandomHash() +
                ':' + this.ms.earliest + ':' + this.ms.latest + '.log');
            this.log('Destination "' + destination_fn + '": atomically renaming, ' + this.uncommitted_entries.length + ' entries.');
            fs.renameSync(this.fn, destination_fn);
            this.log('Destination "' + destination_fn + '": atomically renamed.');
            this.recent_files.push({
                name: destination_fn,
                timestamp_ms: this.now()
            });
            ++this.total_file_renames;
            this.uncommitted_entries = [];
        } else {
            this.log('Intermediate "' + this.fn + '": unlinking empty.');
            fs.unlinkSync(this.fn);
            this.log('Intermediate "' + this.fn + '": unlinked.');
        }
        delete this.fn;
        delete this.ms;
    }
};

Impl.prototype.stats = function(unittest_mode) {
    var result = {
        total_consumed: this.total_consumed,
        total_replayed: this.total_replayed,
        total_file_renames: this.total_file_renames,
        current_file: {
            number_of_entries: this.uncommitted_entries.length,
            time_interval: this.ms,
        },
    };
    if (!unittest_mode) {
        result.current_file.fn = this.fn;
        result.uptime_in_seconds = 1e-3 * (this.now() - this.start_time_ms);
        result.current_file.age_in_seconds = 1e-3 * (this.now() - this.file_start_time_ms);
        result.recent_entries = this.recent_entries.dump();
        result.recent_files = this.recent_files.dump();
        result.qps_overall = result.total_consumed / result.uptime_in_seconds;
        var msq = this.recent_entries_accepted_ms;
        var msq_size = msq.size();
        if (msq_size > 0) {
            result.qps_on_recent_entries = msq_size / (1e-3 * (this.now() - msq.peek_least_recent()));
        }
    }
    return result;
};

Impl.prototype.appendEntry = function(e) {
    assert(_.isObject(e));
    if (!_.isNumber(e.ms)) {
        this.log('NEED_MS_FIELD.');
        return;
    }
    if (this.params.mock_time) {
        mock_ms = e.ms;
    }
    var call_timestamp = this.now();
    if (Math.abs(e.ms - this.now()) > this.params.max_time_discrepancy_ms) {
        this.log('LARGE_TIME_DISCREPANCY');
        if (!this.skipped_because_of_time_discrepancy) {
            this.skipped_because_of_time_discrepancy = 0;
        }
        if (!this.skipped_because_of_discrepancy_threshold) {
            this.skipped_because_of_discrepancy_threshold = 1;
        }
        ++this.skipped_because_of_time_discrepancy;
        if (this.skipped_because_of_time_discrepancy >= this.skipped_because_of_discrepancy_threshold) {
            this.skipped_because_of_discrepancy_threshold *= 10;
            this.log(this.skipped_because_of_time_discrepancy + ' total skipped because of time discrepancy.');
        }
        return;
    }
    this.flushIfDestinationFilenameHasChanged();
    this.ensureFileIsOpen();
    if (!this.ms.earliest || e.ms < this.ms.earliest) {
        this.ms.earliest = e.ms;
    }
    if (!this.ms.latest || e.ms > this.ms.latest) {
        this.ms.latest = e.ms;
    }
    fs.writeSync(this.fd, JSON.stringify(e) + '\n');
    if (this.params.max_file_age_ms && this.uncommitted_entries.length === 0) {
        var self = this;
        var fn_to_flush = this.fn;
        setTimeout(
            function() {
                if (self.fn === fn_to_flush) {
                    self.log('Intermediate "' + self.fn + '": flushed by timeout.');
                    self.flush();
                }
            },
            this.params.max_file_age_ms);
    }
    this.recent_entries.push(_.clone(e));
    this.recent_entries_accepted_ms.push(call_timestamp);
    this.uncommitted_entries.push(_.clone(e));
    ++this.total_consumed;
    if (this.params.log_frequency && ((this.uncommitted_entries.length % this.params.log_frequency)) === 0) {
        this.log('Intermediate "' + this.fn + '": ' + this.uncommitted_entries.length + ' entries.');
    }
    if (this.params.max_entries_per_file && (this.uncommitted_entries.length > this.params.max_entries_per_file)) {
        this.flush();
    }
};

Impl.prototype.flush = function() {
    this.ensureFileIsFlushed();
    this.ensureFileIsOpen();
};

Impl.prototype.getPendingEntries = function() {
    return this.uncommitted_entries;
};

Impl.prototype.finalFlush = function() {
    this.ensureFileIsFlushed();
};

Impl.prototype.flushIfDestinationFilenameHasChanged = function() {
    var formatted_timestamp = this.getFormattedDate();
    if (formatted_timestamp != this.current_file_formatted_date) {
        if (this.uncommitted_entries.length > 0) {
            this.log('Time boundary: "' + formatted_timestamp + '": flushing.');
            this.flush();
        } else {
            this.log('Time boundary: "' + formatted_timestamp + '": nothing to flush.');
            this.current_file_formatted_date = formatted_timestamp;
        }
    }
};

Impl.prototype.installTimeBoundaryFlusher = function() {
    var self = this;
    setInterval(function() { self.flushIfDestinationFilenameHasChanged(); }, 1000);
};

Impl.prototype.tearDown = function(callback) {
    this.ensureFileIsFlushed();
    if (_.isFunction(callback)) {
        callback();
    }
};

Impl.prototype.replayIntermediateFiles = function() {
    var self = this;
    var files = fs.readdirSync(this.params.intermediate_dir);
    if (files.length > 0) {
        self.log('Replaying ' + files.length + ' files.');
        _.each(files, function(fn) {
            var full_fn = path.join(self.params.intermediate_dir, fn);
            self.log('Replaying "' + fn + '": begin.');
            self.ms = {
                earliest: null,
                latest: null,
            };
            var lines = {
                total: 0,
                good: 0,
            };
            _.each(fs.readFileSync(full_fn).toString().split('\n'), function(line) {
                if (line) {
                    ++lines.total;
                    var e = safeParseJson(line);
                    if (_.isObject(e) && _.isNumber(e.ms)) {
                        ++lines.good;
                        ++self.total_replayed;
                        self.ensureFileIsOpen();
                        if (!self.ms.earliest || e.ms < self.ms.earliest) {
                            self.ms.earliest = e.ms;
                        }
                        if (!self.ms.latest || e.ms > self.ms.latest) {
                            self.ms.latest = e.ms;
                        }
                        fs.writeSync(self.fd, JSON.stringify(e) + '\n');
                    }
                }
            });
            self.log(
                'Replaying "' + fn + '": ' + lines.good + ' entries parsed' +
                (lines.total === lines.good ? '' : ' (out of ' + lines.total + ' lines)') +
                '.');
            self.ensureFileIsFlushed(true);
            self.log('Replaying "' + fn + '": done.');
            fs.unlinkSync(full_fn);
        });
    }
};

function runStorer(config, userCodeCallback, tearDownCallback, extraCallbacks) {
    var intermediate_dir = config.storer_intermediate_dir || path.join(config.storer_workdir, '/intermediate');
    var publish = (extraCallbacks && extraCallbacks.publish) ? extraCallbacks.publish : (function() {});
    console.log('Intermediate dir "' + intermediate_dir + '".');
    mkpath.sync(intermediate_dir);
    var destination_dir = config.storer_destination_dir || path.join(config.storer_workdir, '/destination');
    console.log('Destination dir "' + destination_dir + '".');
    mkpath.sync(destination_dir);

    var impl = new Impl({
        verbose: config.verbose,
        intermediate_dir: intermediate_dir,
        destination_dir: destination_dir,
        log_frequency: config.storer_log_frequency,
        max_time_discrepancy_ms: config.storer_max_time_discrepancy_ms,
        max_entries_per_file: config.storer_max_entries_per_file,
        max_file_age_ms: config.storer_max_file_age_ms,
        mock_time: config.storer_mock_time,
        filename_dateformat: config.storer_filename_dateformat,
    });

    if (extraCallbacks && extraCallbacks.set_stats_callback) {
        extraCallbacks.set_stats_callback(function() { return impl.stats(); });
    }

    if (extraCallbacks && extraCallbacks.set_pending_callback) {
        extraCallbacks.set_pending_callback(function() { return impl.getPendingEntries(); });
    }

    process.on('SIGINT', function() {
        console.log('SIGINT, caught.');
        impl.finalFlush();
        tearDownCallback(function() {
            console.log('SIGINT, processed. You should not be seeing this.');
            process.exit(-1);
        });
    });

    userCodeCallback({
        push: function(entry, callback) {
            var json;
            if (_.isString(entry)) {
                if (config.storer_debug && entry === 'STOP') {
                    tearDownCallback(function() {
                        console.log('STOP, processed. You should not be seeing this.');
                        process.exit(-1);
                    });
                    return;
                } else if (config.storer_debug && entry === 'UNITTEST_STATS') {
                    console.log('UNITTEST\t' + JSON.stringify(impl.stats(true)));
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                } else if (config.storer_debug && entry === 'STATS') {
                    console.log(JSON.stringify(impl.stats()));
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                } else if (config.storer_debug && entry === 'CONFIG') {
                    console.log(JSON.stringify(config, null, 2));
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                } else if (config.storer_debug && entry === 'CREATE') {
                    impl.ensureFileIsOpen();
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                } else if (config.storer_debug && entry === 'FLUSH') {
                    impl.ensureFileIsFlushed();
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                } else if (config.storer_debug && entry === 'STATUS') {
                    console.log(JSON.stringify(impl, null, 2));
                    if (_.isFunction(callback)) {
                        setTimeout(callback, 0);
                    }
                    return;
                }
                json = safeParseJson(entry);
            } else {
                json = entry;
            }
            if (_.isObject(json)) {
                impl.appendEntry(json);
                publish(json);
            } else {
                impl.log('INVALID_JSON');
            }
            if (_.isFunction(callback)) {
                setTimeout(callback, 0);
            }
        },
        stats: impl.stats,
        shutdown: function(callback) {
            impl.tearDown();
            tearDownCallback(callback, function() {
                console.log('shutdown, processed. You should not be seeing this.');
                process.exit(-1);
            });
        }
    });
};

function spawnUsingPidLock(config, userCodeCallback) {
    var lockdir = config.storer_lockdir || config.storer_workdir;
    assert(_.isString(lockdir) && lockdir !== '');
    mkpath.sync(lockdir);
    console.log('Lock ("' + lockdir + '" / "' + config.storer_lockname + '"): acquiring.');
    pidlock.guard(lockdir, config.storer_lockname, function(error, data, cleanup) {
        if (error) {
            console.log('Lock ("' + lockdir + '" / "' + config.storer_lockname + '"): busy.');
        } else {
            console.log('Lock ("' + lockdir + '" / "' + config.storer_lockname + '"): acquired.');
            runStorer(config, userCodeCallback, function(tearDownCallback) {
                console.log('Lock ("' + lockdir + '" / "' + config.storer_lockname + '"): releasing.');
                cleanup();
                console.log('Lock ("' + lockdir + '" / "' + config.storer_lockname + '"): releases.');
                if (_.isFunction(tearDownCallback)) {
                    tearDownCallback();
                }
            });
        }
    });
};

function spawnUsingFaye(config, userCodeCallback) {
    var pubsub_port = config.pubsub_port;
    var pubsub_mount = config.pubsub_mount;
    var pubsub_channel = '/' + config.pubsub_channel;
    var pubsub_teardown_delay_ms = config.storer_pubsub_server_teardown_delay_ms || 2000;

    var app = express();
    var healthzCallback = function() { return 'STARTING' };
    var pendingCallback = function() { return []; };
    var statsCallback = healthzCallback;
    app.get('/', function(request, response) {
        response.send(healthzCallback());
    });
    app.get('/pending', function(request, response) {
        response.send(JSON.stringify(pendingCallback()));
    });
    app.get('/healthz', function(request, response) {
        response.send(healthzCallback());
    });
    app.get('/statusz', function(request, response) {
        var result = statsCallback();
        response.format({
            text: function() {
                response.send(JSON.stringify(result));
            },
            html: function() {
                response.writeHead(200, {"Content-Type": "application/json"});
                response.write(JSON.stringify(result, null, 2));
                response.end();
            },
        });
    });

    var server = http.createServer(app);

    console.log('Faye: mount "' + pubsub_mount + '", channel "' + pubsub_channel + '", port ' + pubsub_port + '.');
    var bayeux = new faye.NodeAdapter({ mount: pubsub_mount });
    bayeux.attach(server);

    server.listen(pubsub_port, function() {
        console.log('Faye: server started.');

        healthzCallback = function() { return 'OK'; };

        var client = bayeux.getClient();
        var publish = function(entry) {
            client.publish(pubsub_channel, { entry: entry });
        };

        runStorer(config, userCodeCallback, function() {
            console.log('Faye: stopping server.');
            server.close(function() {
                console.log('Faye: server stopped.');
                process.exit(0);
            });
            // Need to explicitly terminate since some PubSub connections may still be open.
            setTimeout(function() {
                console.log('Faye: could not stop the server, likely due to persistent connections; terminating anyway.');
                process.exit(0);
            }, pubsub_teardown_delay_ms);
        }, {
            set_stats_callback: function(callback) { statsCallback = callback; }, 
            set_pending_callback: function(callback) { pendingCallback = callback; }, 
            publish: publish,
        });
    });
};

module.exports.spawn = function(config, userCodeCallback) {
    assert(_.isFunction(userCodeCallback));
    var spawn = (config.pubsub_channel && config.pubsub_mount && config.pubsub_port) ? spawnUsingFaye : spawnUsingPidLock;
    spawn(config, userCodeCallback);
};

if (require.main === module) {
    var config = require('./config').fromCommandLine();
    module.exports.spawn(config, function(storer) {
        var rl = require('readline').createInterface(process.stdin, process.stdout);
        var lock;
        rl.on('line', function(line) {
            synchronized(lock, function(callback) {
                storer.push(line, callback);
            });
        });
        rl.on('close', function() {
            synchronized(lock, function(callback) {
                console.log('Tearing down.');
                storer.shutdown(function() {
                    console.log('Done.');
                    process.exit(0);
                });
            });
        });
    });
}
