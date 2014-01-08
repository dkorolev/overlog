// Fetches log messages from the directory where the storer has put them.
//
// Notes on ranges:
// 1) Queries are [begin, end).
//    Begin timestamp is included.
//    End timestamp is excluded.
// 2) Log files are supposed to have ":first:last" timestamps.
//    Both included.
// The test covers this.

var _ = require('underscore');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var hound = require('hound');
var step = require('step');
var synchronized = require('synchronized');
var readline = require('readline');
var semaphore = require('semaphore');
var faye = require('faye');
var restler = require('restler');

var prefix = 'DEBUG ';
var LOG = function(message) {
    console.log(prefix + message);
};

function safeParseJson(string) {
    try {
        return JSON.parse(string);
    } catch (exception) {
        return null;
    }
};

module.exports.spawn = function(config, callback) {
    assert(_.isFunction(callback));

    function Fetcher(params) {
        assert(_.isObject(params));
        this.params = _.clone(params);
        this.log = this.params.verbose ? LOG : function() {};

        var dir = params.fetcher_dir;
        assert(_.isString(dir) && dir !== '');
        this.log('Fetching from "' + dir + '".');
        if (!fs.existsSync(dir)) {
            this.log('Dir does not exist.');
            throw new Error('"' + dir + '" does not exist.');
        }
        if (!fs.lstatSync(dir).isDirectory()) {
            this.log('Not a directory.');
            throw new Error('"' + dir + '" is not a directory.');
        }

        this.dir = dir;
        this.files = {};
        this.sorted_files = [];

        var self = this;

        _.each(fs.readdirSync(dir), self.use, self);

        if (this.params.use_watcher) {
            this.log('Using watcher.');
            var watcher = hound.watch(dir);
            watcher.on('create', function(fn) {
                self.use(path.basename(fn));
            });
        } else {
            this.log('Not using watcher.');
        }
    };

    Fetcher.prototype.use = function(fn) {
        assert(_.isString(fn));
        if (Object.hasOwnProperty.call(this.files, fn)) {
            this.log('Warning: file "' + fn + '" has already been considered. Ignoring.');
        } else {
            var full_fn = path.join(this.dir, fn);
            this.files[fn] = {
                full_fn: full_fn,
            };
            if (!fs.existsSync(full_fn)) {
                this.log('File "' + full_fn + '" does not exist.');
            } else if (!fs.lstatSync(full_fn).isFile()) {
                this.log('File "' + full_fn + '" is not a file.');
            } else {
                var split;
                var ms = {
                    first: null,
                    last: null,
                };
                if (fn.substr(-4) !== '.log' ||
                    (split = fn.substr(0, fn.length - 4).split(':'), split.length !== 4) ||
                    (ms.first = Number(split[2]), ms.last = Number(split[3]), !(ms.first && ms.last)) ||
                    ms.first > ms.last) {
                    this.log('Filename of "' + full_fn + '" is wrong. This file will not be considered.');
                } else {
                    this.log('Adding "' + full_fn + '".');
                    var self = this;
                    self.read_semaphore = semaphore(self.params.max_open_files || 250);
                    var read = function(callbacks) {
                        // TODO(dkorolev): Add caching.
                        assert(_.isObject(callbacks));
                        assert(_.isFunction(callbacks.entry));
                        assert(_.isFunction(callbacks.done));
                        self.read_semaphore.take(function() {
                            fs.readFile(full_fn, function(error, data) {
                                if (!error) {
                                    _.each(data.toString().split('\n'), function(line) {
                                        var entry = safeParseJson(line);
                                        if (entry) {
                                            callbacks.entry(entry);
                                        }
                                    });
                                    callbacks.done();
                                    self.read_semaphore.leave();
                                } else {
                                    callbacks.done(error, data);
                                    self.read_semaphore.leave();
                                }
                            });
                        });
                    };
                    this.files[fn].ms = ms;
                    this.files[fn].read = read;
                    this.sorted_files.push({
                        fn: fn,
                        ms: ms,
                        read: read,
                    });
                    // Sort in reverse order of last_ms.
                    // This way for log queries "ms >= X" the array has to be traversed front-to-back
                    // and "last_ms < X" is the stopping criterion.
                    this.sorted_files.sort(function(a, b) {
                        return b.ms.last - a.ms.last;
                    });
                }
            }
        }
    };

    Fetcher.prototype.follow = function(callback) {
        var self = this;
        assert(_.isFunction(callback));
        var server_pending_path = self.params.pubsub_server + ':' + self.params.pubsub_port + '/pending';
        self.log('Pending entries: connecting to "' + server_pending_path + '".');
        // TODO(dkorolev): Expose timeout as flag/parameter.
        restler.get(server_pending_path, {
            timeout: 10000,
        }).on('timeout', function() {
            self.log('Pending entries: timed out connecting to the server.');
            process.exit(1);
        }).on('error', function() {
            self.log('Pending entries: error connecting to the server.');
            process.exit(1);
        }).on('complete', function(data) {
            var parsed_data = safeParseJson(data);
            if (!_.isArray(parsed_data)) {
                self.log('Pending entries: got wrong data, expected an array.');
                process.exit(1);
            }
            self.log('Pending entries: Received ' + parsed_data.length + ' entries.');
            _.each(parsed_data, callback);
            var server_faye_path = self.params.pubsub_server + ':' + self.params.pubsub_port + self.params.pubsub_mount;
            self.log('PubSub: connecting to "' + server_faye_path + '".');
            var client = new faye.Client(server_faye_path);
            client.subscribe('/log', function(message) {
                if (_.isObject(message) && _.isObject(message.entry)) {
                    callback(message.entry);
                } else {
                    self.log('Ignoring malformed PubSub message: ' + JSON.stringify(message));
                }
            });
        });
    };

    var fetcher = new Fetcher({
        verbose: config.verbose,
        fetcher_dir: config.fetcher_dir,
        max_open_files: config.fetcher_max_open_files,
        use_watcher: config.fetcher_use_watcher,
        pubsub_server: config.fetcher_pubsub_server,
        pubsub_mount: config.pubsub_mount,
        pubsub_port: config.pubsub_port,
    });

    callback({
        fetch: function(q, callback) {
            assert(_.isFunction(callback));
            assert(!q || _.isObject(q));
            var query = q || {
                begin_ms: null,
                end_ms: null
            };
            if (!query.begin_ms) {
                query.begin_ms = 0;
            }
            if (!query.end_ms) {
                query.end_ms = Infinity;
            }
            if (fetcher.sorted_files.length === 0) {
                setTimeout(function() {
                    callback(null, []);
                }, 0);
            } else {
                var result = [];
                step(
                    function() {
                        var group = this.group();
                        var done = group();
                        for (var i = 0; i < fetcher.sorted_files.length; ++i) {
                            // Only consider files time interval of entries in which
                            // intersects with the query.
                            var f = fetcher.sorted_files[i];
                            if (query.begin_ms && f.ms.last < query.begin_ms) {
                                // This file, as well as everything else further this list,
                                // is out of the time window requested.
                                break;
                            }
                            if (!(f.ms.first >= query.end_ms) && !(f.ms.last < query.begin_ms)) {
                                var cb = group();
                                f.read({
                                    entry: function(e) {
                                        if (_.isObject(e) && Object.prototype.hasOwnProperty.call(e, 'ms')) {
                                            if (e.ms >= query.begin_ms && e.ms < query.end_ms) {
                                                result.push(e);
                                            }
                                        }
                                    },
                                    done: cb,
                                });
                            }
                        }
                        done();
                    },
                    function(error, data) {
                        if (error) {
                            callback(error);
                        } else {
                            result.sort(function(a, b) {
                                return a.ms - b.ms;
                            });
                            callback(null, result);
                        }
                    }
                );
            }
        },
        follow: function(callback) {
            fetcher.follow(callback);
        },
        shutdown: function(callback) {
            if (_.isFunction(callback)) {
                callback();
            }
        },
    });
};

if (require.main === module) {
    var config = require('./config').fromCommandLine();
    var log = config.verbose ? LOG : console.error;
    module.exports.spawn(config, function(fetcher) {
        function tearDown() {
            log('Tearing down.');
            fetcher.shutdown(function() {
                log('Done.');
                process.exit(0);
            });
        };

        // Basic logic, driven by command line flags.
        var follow = config.follow;
        assert(!(config.since_ms_then_follow && config.last_ms_then_follow));
        var doFollow = function() {
            fetcher.follow(function(e) {
                console.log(JSON.stringify(e));
            });
        };
        var doDumpAndFollow = function(error, data) {
            if (error) {
                console.error('Error: ' + error);
                process.exit(1);
            } else {
                _.each(data, function(e) {
                    console.log(JSON.stringify(e));
                });
                doFollow();
            }
        };
        if (config.since_ms_then_follow) {
            fetcher.fetch({ begin_ms: config.since_ms_then_follow }, doDumpAndFollow);
            follow = true;
            return;
        }
        if (config.last_ms_then_follow) {
            fetcher.fetch({ begin_ms: Date.now() - config.last_ms_then_follow }, doDumpAndFollow);
            follow = true;
            return;
        }
        if (follow) {
            doFollow();
            return;
        }

        // More advanced CLI logic using readline.
        var rl = readline.createInterface(process.stdin, process.stdout);

        function printSynopsis() {
            console.log(
                'Type in empty string or "ALL" without quotes to fetch all records,' +
                ' one timestamp in ms to fetch all records after that timestamp' +
                ' or two timestamps to fetch all records between those timestamps.');
        };

        function wrapOutputIntoCallback(callback) {
            return function(error, data) {
                if (error) {
                    log('Error: ' + error.toString());
                } else {
                    _.each(data, function(e) {
                        assert(_.isObject(e));
                        console.log(JSON.stringify(e));
                    });
                    log(data.length + ' entries.');
                }
                callback();
            };
        };

        var lock;
        rl.on('line', function(untrimmed_line) {
            synchronized(lock, function(callback) {
                var line = untrimmed_line.trim();
                if (line === '' || line === 'ALL') {
                    fetcher.fetch({}, wrapOutputIntoCallback(callback));
                } else if (line === 'STOP') {
                    log('Stopping.');
                    fetcher.shutdown(function() {
                        log('Done.');
                        process.exit(0);
                    });
                } else if (line === 'FOLLOW') {
                    log('Following. Kill to stop.');
                    fetcher.follow(function(e) {
                        console.log(JSON.stringify(e));
                    });
                    return;
                } else {
                    var q = line.split(' ');
                    if (q.length === 1) {
                        var a = Number(q[0]);
                        if (a) {
                            fetcher.fetch({
                                begin_ms: a
                            }, wrapOutputIntoCallback(callback));
                        } else {
                            printSynopsis();
                        }
                    } else if (q.length === 2) {
                        var a = Number(q[0]),
                            b = Number(q[1]);
                        if (a && b) {
                            fetcher.fetch({
                                begin_ms: Math.min(a, b),
                                end_ms: Math.max(a, b),
                            }, wrapOutputIntoCallback(callback));
                        } else {
                            printSynopsis();
                            callback();
                        }
                    } else {
                        printSynopsis();
                        callback();
                    }
                }
            });
        });
        rl.on('close', function() {
            synchronized(lock, function(callback) {
                tearDown();
                callback();
            });
        });
    });
}
