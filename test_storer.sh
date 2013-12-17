#!/bin/bash
#
# Unit test for the Storer interface.
# Uses its command-line interface.

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

TMPDIR=$(mktemp -d -u)
echo -e "Working directory: \e[1;34m$TMPDIR\e[0m"

DIFF="diff"
BINARY="node lib/storer.js --verbose --storer_debug --storer_lockfile=$TMPDIR/lock"


echo -n 'Does not start if the lock is acquired: '
mkdir $TMPDIR
touch $TMPDIR/lock
echo | $BINARY >/dev/null 2>$TMPDIR/output.txt
if ! grep EEXIST $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Uses the lockfile: '
mkdir $TMPDIR
mkdir $TMPDIR-writable
chmod -w $TMPDIR
echo | $BINARY >/dev/null 2>$TMPDIR-writable/output.txt
if ! grep EACCES $TMPDIR-writable/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR-writable
rm -rf $TMPDIR


echo -n 'Responds with INVALID_JSON: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
This is not a valid JSON.
EOF
cat $TMPDIR/input.txt | $BINARY > $TMPDIR/output.txt
if ! grep INVALID_JSON $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with NEED_V_FIELD: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"foo": "bar"}
EOF
cat $TMPDIR/input.txt | $BINARY > $TMPDIR/output.txt
if ! grep NEED_V_FIELD $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with NEED_MS_FIELD: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","foo": "bar"}
EOF
cat $TMPDIR/input.txt | $BINARY > $TMPDIR/output.txt
if ! grep NEED_MS_FIELD $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with LARGE_TIME_DISCREPANCY: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY > $TMPDIR/output.txt
if ! grep LARGE_TIME_DISCREPANCY $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Responds with VERSION_OUT_OF_BOUNDS: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 > $TMPDIR/output.txt
if ! grep VERSION_OUT_OF_BOUNDS $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Writes an entry: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" > $TMPDIR/output.txt
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


echo -n 'Fails if intermediate directory can not be written into: '
mkdir $TMPDIR
mkdir $TMPDIR/intermediate
chmod -w $TMPDIR/intermediate
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" >/dev/null 2> $TMPDIR/output.txt
if ! grep EACCES $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Uses overridden intermediate directory: '
mkdir $TMPDIR
mkdir $TMPDIR/intermediate
chmod -w $TMPDIR/intermediate
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" --storer_intermediate_dir=$TMPDIR/intermediate2 > $TMPDIR/output.txt
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
mkdir $TMPDIR
mkdir $TMPDIR/destination
chmod -w $TMPDIR/destination
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" >/dev/null 2> $TMPDIR/output.txt
if ! grep EACCES $TMPDIR/output.txt >/dev/null ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Uses overridden destination directory: '
mkdir $TMPDIR
mkdir $TMPDIR/destination
chmod -w $TMPDIR/destination
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" --storer_destination_dir=$TMPDIR/destination2 > $TMPDIR/output.txt
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


echo -n 'Discards entries because of version: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
{"v":"0.1.0","ms":0,"data":"blah"}
{"v":"0.9.1","ms":0,"data":"bar"}
{"v":"0.1.1","ms":0,"data":"buzz"}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
{"v":"0.9.1","ms":0,"data":"bar"}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" > $TMPDIR/output.txt
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


echo -n 'Explicitly flushes: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
FLUSH
{"v":"0.9.1","ms":0,"data":"bar"}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
{"v":"0.9.1","ms":0,"data":"bar"}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" > $TMPDIR/output.txt
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

echo -n 'Implicitly flushes by maximum number of entires per file: '
mkdir $TMPDIR
cat >$TMPDIR/input.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo1"}
{"v":"0.9.1","ms":0,"data":"bar1"}
{"v":"0.9.0","ms":0,"data":"foo2"}
{"v":"0.9.1","ms":0,"data":"bar2"}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo1"}
{"v":"0.9.0","ms":0,"data":"foo2"}
{"v":"0.9.1","ms":0,"data":"bar1"}
{"v":"0.9.1","ms":0,"data":"bar2"}
EOF
cat $TMPDIR/input.txt | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" --storer_max_entries_per_file=2 > $TMPDIR/output.txt
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


echo -n 'Replays pre-existing intermediate files on startup: '
mkdir $TMPDIR
mkdir $TMPDIR/intermediate
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
echo | $BINARY > $TMPDIR/output.txt
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
mkdir $TMPDIR
cat >$TMPDIR/i1.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
EOF
cat >$TMPDIR/i2.txt <<EOF
{"v":"0.9.1","ms":0,"data":"bar"}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"v":"0.9.0","ms":0,"data":"foo"}
{"v":"0.9.1","ms":0,"data":"bar"}
EOF
(cat $TMPDIR/i1.txt ; sleep 1 ; cat $TMPDIR/i2.txt) | $BINARY --storer_max_time_discrepancy_ms=1e15 --storer_version_requirements=">=0.8.0" --storer_max_file_age_ms=200 > $TMPDIR/output.txt
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


echo -e '\e[1;32mPASS\e[0m'
exit 0
