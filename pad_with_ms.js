// A simple tool to pad log entries with millisecond values from them.
// Used to compactify logs.
// cat /somedir/logs/* | node pad_with_ms.js | sort -g | cut -f 2 >all.txt
// cat all.txt | node lib/storer.js --storer_destination_dir=/somedir/resharded/ --storer_mock_time --storer_filename_dateformat=yyyy-mm-dd >/dev/null
var lines = require('fs').readFileSync('/dev/stdin').toString().split('\n');
for (var i in lines) {
  var s = lines[i];
  try {
    var e = JSON.parse(s);
    console.log(e.ms + '\t' + s);
  } catch (e) {}
}
