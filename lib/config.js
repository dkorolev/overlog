'use strict';

var _ = require('underscore');

var version = '0.1.0';

var spec = {
    verbose: {
        name: 'Output debug information to the console.',
    },
    storer_lockfile: {
        name: 'Lockfile for the storer to use.',
        value: '/tmp/node-simple-logs-manager/lock'
    },
    storer_intermediate_dir: {
        name: 'Intermediate dir to use, derived from the lockfile by default.'
    },
    storer_destination_dir: {
        name: 'Destination dir to use, derived from the lockfile by default.'
    },
    storer_log_frequency: {
        name: 'How often should the message about logged entries count be dumped to the console.',
        value: 1000
    },
    storer_version_requirements: {
        name: 'Version requirements on the "v" field of the entry for it to be logged.',
        value: ">=1.0.0",
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
