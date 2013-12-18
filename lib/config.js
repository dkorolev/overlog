'use strict';

var _ = require('underscore');

var version = JSON.parse(require('fs').readFileSync(__dirname + '/../package.json').toString()).version;

var spec = {
    verbose: {
        name: 'Output debug information to the console.',
    },
    storer_workdir: {
        name: 'Storer working directory. By default, the lock file, intermediate and destination directories are located in it.',
        value: '/tmp/'
    },
    storer_intermediate_dir: {
        name: 'Intermediate dir to use, derived from the workdir by default.'
    },
    storer_destination_dir: {
        name: 'Destination dir to use, derived from the workdir by default.'
    },
    storer_lockdir: {
        name: 'Storer lock directory, same as the workdir by default.',
    },
    storer_lockname: {
        name: 'Storer lock file name, the unique ID of the application.',
        value: 'overlog_storer'
    },
    storer_log_frequency: {
        name: 'How often should the message about logged entries count be dumped to the console.',
        value: 1000
    },
    storer_max_time_discrepancy_ms: {
        name: 'Maxiumum discrepancy, in milliseconds, between entry ms and current wall time. Other entries are dropped.',
        value: 5000,
    },
    storer_max_entries_per_file: {
        name: 'Maximum number of entries per file. If set, existing file is flushed and a new one is started upon reaching this number.',
    },
    storer_max_file_age_ms: {
        name: 'Maximum content age in intermediate file, in milliseconds. If set, forces an intermediate file to be flushed by this timeout.',
    },
    storer_debug: {
        name: 'Allow debugging commands. Also used by the test.',
        value: false
    },
    fetcher_dir: {
        name: 'The directory files from which should be accessed by the fetcher.',
        value: '/tmp/destination'
    },
    fetcher_debug: {
        name: 'Allow debugging commands. Also used by the test.',
        value: false
    },
};

module.exports.defaults = function() {
    var config = {};
    for (var key in spec) {
        config[key] = spec[key].defaults;
    }
    return config;
};

module.exports.fromCommandLine = (function() {
    var config = null;
    return function() {
        if (!config) {
            var commander = require('commander');
            commander.version(version);
            for (var key in spec) {
                commander.option('--' + key + ' [value]', spec[key].flag, spec[key].value);
            }
            commander.parse(process.argv);
            config = {};
            for (var key in spec) {
                config[key] = commander[key];
            }
        }
        return config;
    };
})();
