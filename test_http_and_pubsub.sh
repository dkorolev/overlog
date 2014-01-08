#!/bin/bash
#
# Unit test for PubSub and HTTP server of the Storer module.

TMPDIR=$(mktemp -d)
echo -e "Working directory: \e[1;34m$TMPDIR\e[0m"

DIFF="diff -c -w"
JSON=./node_modules/json/bin/json.js

STORER_PIPE=$TMPDIR/pipe
mkfifo $STORER_PIPE

TEST_PORT=9999
CHANNEL=unittest

OUTPUT_STORER=$TMPDIR/output_storer.txt
OUTPUT_FETCHER=$TMPDIR/output_fetcher.txt


echo -n 'Starting storer in background: '
tail -f $STORER_PIPE \
| node lib/storer.js  --storer_workdir=$TMPDIR --pubsub_port=$TEST_PORT --storer_debug --storer_max_time_discrepancy_ms=1e15 --pubsub_channel=$CHANNEL \
>$OUTPUT_STORER &
STORER_PID=$!
echo -e "\e[1;32mPID $STORER_PID\e[0m"

echo -n 'Waiting for storer to get healthy: '
echo -n .
while ! [[ $(curl -s localhost:$TEST_PORT) == 'OK' ]] ; do echo -n . ; sleep 0.2 ; done
echo -e " \e[1;32mOK\e[0m"


echo -n 'Status page reflects a freshly started server: '
if ! echo '{"total_consumed":0,"total_file_renames":0}' | $DIFF - <(curl -s localhost:$TEST_PORT/statusz | $JSON -u total_consumed total_file_renames) ; then
    echo -e '\e[1;31mFAIL\e[0m'
    echo STOP >> $STORER_PIPE
    kill -INT $FETCHER_PID
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'


echo -n 'Adding a few entries:'
echo '{"ms":12001,"data":"first"}' >> $STORER_PIPE
echo '{"ms":12002,"data":"batch"}' >> $STORER_PIPE
echo -e ' \e[1;32mOK\e[0m'


echo -n 'Status page reflects newly added entries: '
if ! echo '{"total_consumed":2,"total_file_renames":0}' | $DIFF - <(curl -s localhost:$TEST_PORT/statusz | $JSON -u total_consumed total_file_renames) ; then
    echo -e '\e[1;31mFAIL\e[0m'
    echo STOP >> $STORER_PIPE
    kill -INT $FETCHER_PID
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'


echo -n 'Pending entries page returns newly added uncommitted entries: '
if ! echo '[{"ms":12001,"data":"first"},{"ms":12002,"data":"batch"}]' | $DIFF - <(curl -s localhost:$TEST_PORT/pending) ; then
    echo -e '\e[1;31mFAIL\e[0m'
    echo STOP >> $STORER_PIPE
    kill -INT $FETCHER_PID
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'


echo -n 'Starting PubSub fetcher in background: '
node lib/fetcher.js --follow  --fetcher_dir=$TMPDIR/destination/ --pubsub_port=$TEST_PORT --pubsub_channel=$CHANNEL >>$OUTPUT_FETCHER &
FETCHER_PID=$!
echo -e "\e[1;32mPID $FETCHER_PID\e[0m"


echo -n 'Waiting for the fetcher to start up and grab pending entries: '
cat >$TMPDIR/golden.txt <<EOF
{"ms":12001,"data":"first"}
{"ms":12002,"data":"batch"}
EOF
echo -n '.'
while ! cat $OUTPUT_FETCHER | $DIFF - $TMPDIR/golden.txt >/dev/null ; do echo -n . ; sleep 0.2 ; done
echo -e ' \e[1;32mOK\e[0m'


echo -n 'Flushing the entries: '
echo FLUSH >> $STORER_PIPE
echo -e '\e[1;32mOK\e[0m'


echo -n 'Status page reflects recent flush: '
if ! echo '{"total_consumed":2,"total_file_renames":1}' | $DIFF - <(curl -s localhost:$TEST_PORT/statusz | $JSON -u total_consumed total_file_renames) ; then
    echo -e '\e[1;31mFAIL\e[0m'
    echo STOP >> $STORER_PIPE
    kill -INT $FETCHER_PID
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'


echo -n 'Pending entries page no longer returns entries since they have been committed: '
if ! echo '[]' | $DIFF - <(curl -s localhost:$TEST_PORT/pending) ; then
    echo -e '\e[1;31mFAIL\e[0m'
    echo STOP >> $STORER_PIPE
    kill -INT $FETCHER_PID
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'


echo -n 'Adding a few more entries: '
echo '{"ms":11001,"data":"second"}' >> $STORER_PIPE
echo '{"ms":11002,"data":"batch"}' >> $STORER_PIPE
echo -e ' \e[1;32mOK\e[0m'


echo -n 'Waiting for the fetcher to grab new entries via PubSub: '
cat >$TMPDIR/golden.txt <<EOF
{"ms":12001,"data":"first"}
{"ms":12002,"data":"batch"}
{"ms":11001,"data":"second"}
{"ms":11002,"data":"batch"}
EOF
echo -n '.'
while ! cat $OUTPUT_FETCHER | $DIFF - $TMPDIR/golden.txt >/dev/null ; do echo -n . ; sleep 0.2 ; done
echo -e ' \e[1;32mOK\e[0m'


echo -n 'Stopping background fetcher: '
kill -INT $FETCHER_PID
while ps -p $FETCHER_PID >/dev/null ; do echo -n . ; sleep 0.2 ;  done
echo -e ' \e[1;32mOK\e[0m'


echo -n 'Stopping background storer: '
echo STOP >> $STORER_PIPE
while ps -p $STORER_PID >/dev/null ; do sleep 0.1 ;  done
echo -e '\e[1;32mOK\e[0m'


echo -e '\e[1;32mPASS\e[0m'
rm -rf $TMPDIR

exit 0
