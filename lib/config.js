'use strict';

var _ = require('underscore');

var version = JSON.parse(require('fs').readFileSync(__dirname + '/../package.json').toString()).version;

var spec = {
  verbose: {
    description: 'Output debug information to the console.',
  },
  storer_workdir: {
    description: 'Storer working directory. By default, the lock file, intermediate and destination directories are located in it.',
    value: '/tmp/'
  },
  storer_intermediate_dir: {
    description: 'Intermediate dir to use, derived from the workdir by default.'
  },
  storer_destination_dir: {
    description: 'Destination dir to use, derived from the workdir by default.'
  },
  storer_lockdir: {
    description: 'Storer lock directory, same as the workdir by default.',
  },
  storer_lockname: {
    description: 'Storer lock file name, the unique ID of the application.',
    value: 'overlog_storer'
  },
  pubsub_port: {
    description: 'Port for PubSub HTTP server.',
    value: 3506,
  },
  pubsub_mount: {
    description: 'PubSub mount point.',
    value: '/pubsub',
  },
  pubsub_channel: {
    description: 'PubSub channel name. When in PubSub mode, lock file is not used in favor of an HTTP socket.',
  },
  storer_pubsub_server_teardown_delay_ms: {
    description: 'Time, in milliseconds, after which the server should be terminated forcefully if a graceful shutdown did not work.',
    value: 2000,
  },
  storer_filename_dateformat: {
    description: 'Date format in final log entries file naming schema. "dateformat" friendly.',
    value: 'yyyy-mm-dd-HH',
  },
  storer_mock_time: {
    description: 'Take log entry timestamps as true time. Useful when replaying logs.',
  },
  storer_log_frequency: {
    description: 'How often should the message about logged entries count be dumped to the console.',
    value: 1000
  },
  storer_max_time_discrepancy_ms: {
    description: 'Maxiumum discrepancy, in milliseconds, between entry ms and current wall time. Other entries are dropped.',
    value: 5000,
  },
  storer_max_entries_per_file: {
    description: 'Maximum number of entries per file. If set, existing file is flushed and a new one is started upon reaching this number.',
  },
  storer_max_file_age_ms: {
    description: 'Maximum content age in intermediate file, in milliseconds. If set, forces an intermediate file to be flushed by this timeout.',
  },
  storer_debug: {
    description: 'Allow debugging commands. Also used by the test.',
    value: false
  },
  fetcher_dir: {
    description: 'The directory files from which should be accessed by the fetcher.',
    value: '/tmp/destination'
  },
  fetcher_max_open_files: {
    description: 'Maximum number of open files a fetcher should be attempting to read simultaneously.',
    value: 250
  },
  fetcher_pubsub_server: {
    description: 'For following mode, PubSub server to use. Defaults to local.',
    value: 'http://0.0.0.0',
  },
  fetcher_pubsub_client_teardown_delay_ms: {
    description: 'Time, in milliseconds, after which the client should be terminated forcefully if a graceful shutdown did not work.',
    value: 2000,
  },
  since_ms: {
    description: 'Dump the entries starting from this millisecond timestamp.'
  },
  last_ms: {
    description: 'Dump the entries from this number of past milliseconds.'
  },
  last_days: {
    description: 'Dump the entries from this number of past days.'
  },
  follow: {
    description: 'Have fetcher follow the log, mimics "tail -f".'
  },
  notify_before_following: {
    description: "Print FOLLOWING before switching to follow mode. To separate client's replay from real-time operation.",
    value: true,
  },
  following_keepalive_period_ms: {
    descripton: 'How often to print a KEEPALIVE message in follow mode.',
    value: 3000,
  },
  following_keepalive_simple_format: {
    description: 'Set to 1 to revert to the old-school "KEEPALIVE" message instead of a JSON entry with timestamp.',
    value: 0,
  },
  fetcher_use_watcher: {
    description: "Have watcher listen to filesystem changes to allow interactive querying.",
  },
  fetcher_debug: {
    description: 'Allow debugging commands. Also used by the test.',
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
        commander.option('--' + key + ' [value]', spec[key].description, spec[key].value);
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
