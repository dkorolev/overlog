#!/bin/bash
#
# Unit test for the Storer module.
# Uses its command-line interface.

TMPDIR=$(mktemp -d -u)
echo -e "Working directory: \e[1;34m$TMPDIR\e[0m"

DIFF="diff -c"
BINARY="node lib/storer.js --verbose --storer_debug --storer_workdir=$TMPDIR"


echo -n 'Responds with INVALID_JSON: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
This is not a valid JSON.
EOF
cat $TMPDIR/input.txt | $BINARY >$TMPDIR/output.txt
if ! grep INVALID_JSON $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with NEED_MS_FIELD: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"foo": "bar"}
EOF
cat $TMPDIR/input.txt | $BINARY >$TMPDIR/output.txt
if ! grep NEED_MS_FIELD $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with LARGE_TIME_DISCREPANCY: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY >$TMPDIR/output.txt
if ! grep LARGE_TIME_DISCREPANCY $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Writes an entry: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"data": 42, "ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":42,"ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 1 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Writes an entry and keeps stats: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"data": 42, "ms":0}
UNITTEST_STATS
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":42,"ms":0}
EOF
cat >$TMPDIR/golden_output.txt <<EOF
{"total_consumed":1,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":1,"ms":{"earliest":0,"latest":0}}}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 1 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/output.txt | grep "^UNITTEST" | cut -f 2 | $DIFF - $TMPDIR/golden_output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Fails if intermediate directory can not be written into: '
mkdir -p $TMPDIR
mkdir -p $TMPDIR/intermediate
chmod -w $TMPDIR/intermediate
echo '{"ms":0}' | $BINARY --storer_max_time_discrepancy_ms=1e15 >/dev/null 2>$TMPDIR/output.txt
if ! grep EACCES $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Uses overridden intermediate directory: '
mkdir -p $TMPDIR
mkdir -p $TMPDIR/intermediate
chmod -w $TMPDIR/intermediate
cat >$TMPDIR/input.txt <<EOF
{"ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_intermediate_dir=$TMPDIR/intermediate2 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 1 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Fails if destination directory can not be written into: '
mkdir -p $TMPDIR
mkdir -p $TMPDIR/destination
chmod -w $TMPDIR/destination
echo '{"ms":0}' | $BINARY --storer_max_time_discrepancy_ms=1e15 >/dev/null 2>$TMPDIR/output.txt
if ! grep EACCES $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Uses overridden destination directory: '
mkdir -p $TMPDIR
mkdir -p $TMPDIR/destination
chmod -w $TMPDIR/destination
cat >$TMPDIR/input.txt <<EOF
{"ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_destination_dir=$TMPDIR/destination2 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination2/* | wc -l) != 1 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination2/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Explicitly flushes: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"ms":0,"data":"1foo"}
UNITTEST_STATS
FLUSH
UNITTEST_STATS
{"ms":1,"data":"2bar"}
UNITTEST_STATS
EOF
cat >$TMPDIR/golden.txt <<EOF
{"ms":0,"data":"1foo"}
{"ms":1,"data":"2bar"}
EOF
cat >$TMPDIR/golden_output.txt <<EOF
{"total_consumed":1,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":1,"ms":{"earliest":0,"latest":0}}}
{"total_consumed":1,"total_replayed":0,"total_file_renames":1,"current_file":{}}
{"total_consumed":2,"total_replayed":0,"total_file_renames":1,"current_file":{"entries":1,"ms":{"earliest":1,"latest":1}}}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 2 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/output.txt | grep "^UNITTEST" | cut -f 2 | $DIFF - $TMPDIR/golden_output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR

echo -n 'Implicitly flushes by maximum number of entires per file: '
mkdir -p $TMPDIR
cat >$TMPDIR/input.txt <<EOF
UNITTEST_STATS
{"ms":0,"data":"1foo1"}
UNITTEST_STATS
{"ms":0,"data":"2bar1"}
UNITTEST_STATS
{"ms":0,"data":"1foo2"}
UNITTEST_STATS
{"ms":0,"data":"2bar2"}
UNITTEST_STATS
EOF
cat >$TMPDIR/golden.txt <<EOF
{"ms":0,"data":"1foo1"}
{"ms":0,"data":"1foo2"}
{"ms":0,"data":"2bar1"}
{"ms":0,"data":"2bar2"}
EOF
cat >$TMPDIR/golden_output.txt <<EOF
{"total_consumed":0,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":0,"ms":{"earliest":null,"latest":null}}}
{"total_consumed":1,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":1,"ms":{"earliest":0,"latest":0}}}
{"total_consumed":2,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":2,"ms":{"earliest":0,"latest":0}}}
{"total_consumed":3,"total_replayed":0,"total_file_renames":1,"current_file":{"entries":0,"ms":{"earliest":null,"latest":null}}}
{"total_consumed":4,"total_replayed":0,"total_file_renames":1,"current_file":{"entries":1,"ms":{"earliest":0,"latest":0}}}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_max_entries_per_file=2 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 2 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/output.txt | grep "^UNITTEST" | cut -f 2 | $DIFF - $TMPDIR/golden_output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Replays pre-existing intermediate files on startup: '
mkdir -p $TMPDIR
mkdir -p $TMPDIR/intermediate
cat >$TMPDIR/intermediate/foo.txt <<EOF
{"data":"foo1","ms":10001}
{"data":"foo3","ms":10002}
{"data":"foo2","ms":10003}
EOF
cat >$TMPDIR/intermediate/bar.txt <<EOF
{"data":"bar1","ms":20001}
{"data":"bar3","ms":20002}
{"data":"bar2","ms":20003}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":"bar1","ms":20001}
{"data":"bar2","ms":20003}
{"data":"bar3","ms":20002}
{"data":"foo1","ms":10001}
{"data":"foo2","ms":10003}
{"data":"foo3","ms":10002}
EOF
echo | $BINARY >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 2 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Implicitly flushes because of the timeout (flaky, uses sleep): '
mkdir -p $TMPDIR
cat >$TMPDIR/i1.txt <<EOF
{"ms":0,"data":"foo"}
UNITTEST_STATS
EOF
cat >$TMPDIR/i2.txt <<EOF
UNITTEST_STATS
{"ms":1,"data":"bar"}
UNITTEST_STATS
EOF
cat >$TMPDIR/golden.txt <<EOF
{"ms":0,"data":"foo"}
{"ms":1,"data":"bar"}
EOF
cat >$TMPDIR/golden_output.txt <<EOF
{"total_consumed":1,"total_replayed":0,"total_file_renames":0,"current_file":{"entries":1,"ms":{"earliest":0,"latest":0}}}
{"total_consumed":1,"total_replayed":0,"total_file_renames":1,"current_file":{"entries":0,"ms":{"earliest":null,"latest":null}}}
{"total_consumed":2,"total_replayed":0,"total_file_renames":1,"current_file":{"entries":1,"ms":{"earliest":1,"latest":1}}}
EOF
(cat $TMPDIR/i1.txt ; sleep 1 ; cat $TMPDIR/i2.txt) | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_max_file_age_ms=200 >$TMPDIR/output.txt
if [ $(ls $TMPDIR/destination/* | wc -l) != 2 ] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/destination/* | sort | $DIFF - $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
if ! cat $TMPDIR/output.txt | grep "^UNITTEST" | cut -f 2 | $DIFF - $TMPDIR/golden_output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -e '\e[1;32mPASS\e[0m'
exit 0
