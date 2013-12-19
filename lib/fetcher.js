// Fetches log messages from the directory where the storer has put them.

var _ = require('underscore');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var hound = require('hound');
var step = require('step');
var synchronized = require('synchronized');
var readline = require('readline');

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
        var watcher = hound.watch(dir);
        watcher.on('create', function(fn) {
            self.use(path.basename(fn));
        });
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
                    var read = function(callbacks) {
                        // TODO(dkorolev): Add caching.
                        assert(_.isObject(callbacks));
                        assert(_.isFunction(callbacks.entry));
                        assert(_.isFunction(callbacks.done));
                        fs.readFile(full_fn, function(error, data) {
                            if (!error) {
                                _.each(data.toString().split('\n'), function(line) {
                                    var entry = safeParseJson(line);
                                    if (entry) {
                                        callbacks.entry(entry);
                                    }
                                });
                                callbacks.done();
                            } else {
                                callbacks.done(error, data);
                            }
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

    var fetcher = new Fetcher({
        verbose: config.verbose,
        fetcher_dir: config.fetcher_dir,
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
                    callback([]);
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
                            if ((query.begin_ms < f.ms.last && query.end_ms >= f.ms.first)) {
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
                        if (error) throw error;
                        result.sort(function(a, b) {
                            return a.ms - b.ms;
                        });
                        callback(result);
                    }
                );
            }
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
    module.exports.spawn(config, function(fetcher) {
        function tearDown() {
            LOG('Tearing down.');
            fetcher.shutdown(function() {
                LOG('Done.');
                process.exit(0);
            });
        };
        var rl = readline.createInterface(process.stdin, process.stdout);

        function printSynopsis() {
            console.log(
                'Type in empty string or "ALL" without quotes to fetch all records,' +
                ' one timestamp in ms to fetch all records after that timestamp' +
                ' or two timestamps to fetch all records between those timestamps.');
        };

        function wrapOutputIntoCallback(callback) {
            return function(data) {
                _.each(data, function(e) {
                    assert(_.isObject(e));
                    console.log(JSON.stringify(e));
                });
                LOG(data.length + ' entries.');
                callback();
            };
        };

        var lock;
        rl.on('line', function(untrimmed_line) {
            synchronized(lock, function(callback) {
                var line = untrimmed_line.trim();
                if (line === '' || line === 'ALL') {
                    fetcher.fetch({}, wrapOutputIntoCallback(callback));
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
