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
// Intermediate filename schema: ('tmp:' + random_hash + '.log').
// Destination  filename schema: ('YYYY-MM-DD-HH-MM' + ':' + random_hash + ':' + firstentry_ms + ':' + lastentry_ms + '.log).
//
// Where "random_hash" is five random lowercase latin characters.

var _ = require('underscore');
var assert = require('assert');
var dateformat = require('dateformat');
var fs = require('fs');
var lockfile = require('lockfile');
var mkpath = require('mkpath');
var path = require('path');

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
    this.replayIntermediateFiles();
    this.total_consumed = 0;
    this.ensureFileIsOpen();
    this.installTimeBoundaryFlusher();
};

Impl.prototype.ensureFileIsOpen = function() {
    if (!this.fd) {
        this.fn = path.join(this.params.intermediate_dir, 'tmp:' + generateRandomHash() + '.log');
        this.log('Intermediate "' + this.fn + '": opening.');
        this.fd = fs.openSync(this.fn, 'a');
        this.log('Intermediate "' + this.fn + '": opened.');
        this.entries = 0;
        this.ms = {
            earliest: null,
            latest: null,
        };
    }
};

Impl.prototype.getFormattedDate = function() {
    return dateformat(Date.now(), 'yyyy-mm-dd-HH-MM');
};

Impl.prototype.ensureFileIsFlushed = function() {
    if (this.fd) {
        this.log('Intermediate "' + this.fn + '": closing.');
        fs.fsyncSync(this.fd);
        fs.closeSync(this.fd);
        delete this.fd;
        this.log('Intermediate "' + this.fn + '": closed.');
        if (this.entries) {
            var destination_fn = path.join(
                this.params.destination_dir,
                this.getFormattedDate() +
                ':' + generateRandomHash() +
                ':' + this.ms.earliest + ':' + this.ms.latest + '.log');
            this.log('Destination "' + destination_fn + '": atomically renaming.');
            fs.renameSync(this.fn, destination_fn);
            this.log('Destination "' + destination_fn + '": atomically renamed.');
        } else {
            this.log('Intermediate "' + this.fn + '": unlinking empty.');
            fs.unlinkSync(this.fn);
            this.log('Intermediate "' + this.fn + '": unlinked.');
        }
        delete this.fn;
        delete this.entries;
        delete this.ms;
    }
};

Impl.prototype.appendEntry = function(e) {
    assert(_.isObject(e));
    if (!_.isNumber(e.ms)) {
        this.log('NEED_MS_FIELD.');
        return;
    }
    if (Math.abs(e.ms - Date.now()) > this.params.max_time_discrepancy_ms) {
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
    this.ensureFileIsOpen();
    if (!this.ms.earliest || e.ms < this.ms.earliest) {
        this.ms.earliest = e.ms;
    }
    if (!this.ms.latest || e.ms > this.ms.latest) {
        this.ms.latest = e.ms;
    }
    fs.writeSync(this.fd, JSON.stringify(e) + '\n');
    if (this.params.max_file_age_ms && this.entries === 0) {
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
    ++this.entries;
    ++this.total_consumed;
    if (this.params.log_frequency && ((this.entries % this.params.log_frequency)) === 0) {
        this.log('Intermediate "' + this.fn + '": ' + this.entries + ' entries.');
    }
    if (this.params.max_entries_per_file && (this.entries > this.params.max_entries_per_file)) {
        this.flush();
    }
};

Impl.prototype.flush = function() {
    this.ensureFileIsFlushed();
    this.ensureFileIsOpen();
};

Impl.prototype.installTimeBoundaryFlusher = function() {
    var self = this;
    this.projected_destination_timestamp = self.getFormattedDate();
    setInterval(
        function() {
            var timestamp = self.getFormattedDate();
            if (timestamp != self.projected_destination_timestamp) {
                self.projected_destination_timestamp = timestamp;
                if (self.entries > 0) {
                    self.log('Time boundary: "' + timestamp + '": flushing.');
                    self.flush();
                } else {
                    self.log('Time boundary: "' + timestamp + '": nothing to flush.');
                }
            }
        },
        1000);
};

Impl.prototype.tearDown = function() {
    this.ensureFileIsFlushed();
};

Impl.prototype.replayIntermediateFiles = function() {
    var self = this;
    var files = fs.readdirSync(this.params.intermediate_dir);
    if (files !== []) {
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
                        ++self.entries;
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
            self.ensureFileIsFlushed();
            self.log(
                'Replaying "' + fn + '": done.');
            fs.unlinkSync(full_fn);
        });
    }
};

module.exports.spawn = function(config, callback) {
    assert(_.isFunction(callback));
    var lockfile_name = config.storer_lockfile;
    console.log('Lockfile "' + lockfile_name + '": acquiring.');
    mkpath.sync(path.dirname(lockfile_name));
    lockfile.lock(lockfile_name, function(error) {
        if (error) throw error;

        console.log('Lockfile "' + lockfile_name + '": acquired.');
        var intermediate_dir = config.storer_intermediate_dir || path.join(path.dirname(lockfile_name), '/intermediate');
        console.log('Intermediate dir "' + intermediate_dir + '".');
        mkpath.sync(intermediate_dir);
        var destination_dir = config.storer_destination_dir || path.join(path.dirname(lockfile_name), '/destination');
        console.log('Intermediate dir "' + destination_dir + '".');
        mkpath.sync(destination_dir);

        var impl = new Impl({
            verbose: config.verbose,
            intermediate_dir: intermediate_dir,
            destination_dir: destination_dir,
            log_frequency: config.storer_log_frequency,
            max_time_discrepancy_ms: config.storer_max_time_discrepancy_ms,
            max_entries_per_file: config.storer_max_entries_per_file,
            max_file_age_ms: config.storer_max_file_age_ms,
        });

        function tearDown(callback) {
            console.log('Lockfile "' + lockfile_name + '": releasing.');
            lockfile.unlock(lockfile_name, function(error) {
                if (error) throw error;
                console.log('Lockfile "' + lockfile_name + '": released.');
                if (_.isFunction(callback)) {
                    callback();
                }
            });
        };

        callback({
            push: function(entry, callback) {
                var json;
                if (_.isString(entry)) {
                    if (config.storer_debug && entry === 'STOP') {
                        tearDown(function() {
                            process.exit(0);
                        });
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
                } else {
                    impl.log('INVALID_JSON');
                }
                if (_.isFunction(callback)) {
                    setTimeout(callback, 0);
                }
            },
            shutdown: function(callback) {
                impl.tearDown();
                tearDown(callback);
            }
        });
    });
};

if (require.main === module) {
    var config = require('./config').fromCommandLine();
    module.exports.spawn(config, function(storer) {
        var rl = require('readline').createInterface(process.stdin, process.stdout);
        rl.on('line', storer.push.bind(storer));
        rl.on('close', function() {
            console.log('Tearing down.');
            storer.shutdown(function() {
                console.log('Done.');
                process.exit(0);
            });
        });
    });
}
