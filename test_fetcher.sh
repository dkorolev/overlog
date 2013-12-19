#!/bin/bash
#
# Unit test for the Fetcher module.
# Uses its command-line interface.
#
# Tests potential off-by one issues with queries being [begin, end)
# and log files naming schema being "*:first:last.log".

TMPDIR=$(mktemp -d -u)
echo -e "Working directory: \e[1;34m$TMPDIR\e[0m"

DIFF="diff -c"
BINARY="node lib/fetcher --verbose --fetcher_dir=$TMPDIR/data"


echo -n 'Returns no records: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/input.txt <<EOF
ALL
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if [[ -s $TMPDIR/output.txt ]] ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns all records from one input file: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:1:5.log <<EOF
{"data":1,"ms":1}
{"data":2,"ms":2}
{"data":3,"ms":3}
{"data":4,"ms":4}
{"data":5,"ms":5}
EOF
cat >$TMPDIR/input.txt <<EOF
ALL
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! cat $TMPDIR/data/* | sort | $DIFF - $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Can run multiple queries: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:101:105.log <<EOF
{"data":1,"ms":101}
{"data":2,"ms":102}
{"data":3,"ms":103}
{"data":4,"ms":104}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":3,"ms":103}
{"data":1,"ms":101}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/input.txt <<EOF
103 104
101 102
105 106
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Sorts the output by "ms" before returning: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:1:5.log <<EOF
{"data":1,"ms":1}
{"data":4,"ms":4}
{"data":2,"ms":2}
{"data":5,"ms":5}
{"data":3,"ms":3}
EOF
cat >$TMPDIR/input.txt <<EOF
ALL
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":1,"ms":1}
{"data":2,"ms":2}
{"data":3,"ms":3}
{"data":4,"ms":4}
{"data":5,"ms":5}
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns range of records from one input file: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:101:105.log <<EOF
{"data":1,"ms":101}
{"data":2,"ms":102}
{"data":3,"ms":103}
{"data":4,"ms":104}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":2,"ms":102}
{"data":3,"ms":103}
EOF
cat >$TMPDIR/input.txt <<EOF
102 104
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns range of records from one input file II: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:101:105.log <<EOF
{"data":1,"ms":101}
{"data":2,"ms":102}
{"data":3,"ms":103}
{"data":4,"ms":104}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":1,"ms":101}
{"data":2,"ms":102}
{"data":3,"ms":103}
EOF
cat >$TMPDIR/input.txt <<EOF
50 104
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns range of records from one input file III: '
mkdir -p $TMPDIR/data
cat >$TMPDIR/data/ENTRY:test:101:105.log <<EOF
{"data":1,"ms":101}
{"data":2,"ms":102}
{"data":3,"ms":103}
{"data":4,"ms":104}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":3,"ms":103}
{"data":4,"ms":104}
{"data":5,"ms":105}
EOF
cat >$TMPDIR/input.txt <<EOF
103 10000
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns all records from multiple input files: '
mkdir -p $TMPDIR/data
echo '{"data":1,"ms":1}' >$TMPDIR/data/ENTRY:t1:1:2.log
echo '{"data":2,"ms":2}' >$TMPDIR/data/ENTRY:t2:2:3.log
echo '{"data":3,"ms":3}' >$TMPDIR/data/ENTRY:t3:3:4.log
echo '{"data":4,"ms":4}' >$TMPDIR/data/ENTRY:t4:4:5.log
echo '{"data":5,"ms":5}' >$TMPDIR/data/ENTRY:t5:5:6.log
cat >$TMPDIR/input.txt <<EOF
ALL
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! cat $TMPDIR/data/* | sort | $DIFF - $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n 'Returns results from a range of input files: '
mkdir -p $TMPDIR/data
echo '{"data":1,"ms":1}' >$TMPDIR/data/ENTRY:t1:1:2.log
echo '{"data":2,"ms":2}' >$TMPDIR/data/ENTRY:t2:2:3.log
echo '{"data":3,"ms":3}' >$TMPDIR/data/ENTRY:t3:3:4.log
echo '{"data":4,"ms":4}' >$TMPDIR/data/ENTRY:t4:4:5.log
echo '{"data":5,"ms":5}' >$TMPDIR/data/ENTRY:t5:5:6.log
cat >$TMPDIR/input.txt <<EOF
1 3
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":1,"ms":1}
{"data":2,"ms":2}
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/output.txt $TMPDIR/golden.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -n "Prioritizes timestamps in filename over timestamps within the file: "
mkdir -p $TMPDIR/data
echo '{"data":"foo","ms":5}' >$TMPDIR/data/ENTRY:foo:1:5.log
echo '{"data":"bar","ms":5}' >$TMPDIR/data/ENTRY:bar:2:4.log
echo '{"data":"wow","ms":5}' >$TMPDIR/data/ENTRY:wow:4:4.log
echo '{"data":"baz","ms":5}' >$TMPDIR/data/ENTRY:baz:5:4.log
echo '{"data":"boo","ms":5}' >$TMPDIR/data/ENTRY:boo:4:6.log
echo '{"data":"meh","ms":5}' >$TMPDIR/data/ENTRY:meh:5:6.log
echo '{"data":"ble","ms":5}' >$TMPDIR/data/ENTRY:meh:6:6.log
echo '{"data":"bah","ms":5}' >$TMPDIR/data/ENTRY:bah:6:7.log
echo '{"data":"should not","ms":4}' >$TMPDIR/data/ENTRY:a:1:9.log
echo '{"data":" be there ","ms":6}' >$TMPDIR/data/ENTRY:b:1:9.log
echo '{"data":"because of","ms":6}' >$TMPDIR/data/ENTRY:c:1:9.log
echo '{"data":"the query!", ms":6}' >$TMPDIR/data/ENTRY:d:1:9.log
cat >$TMPDIR/input.txt <<EOF
5 6
EOF
cat >$TMPDIR/golden.txt <<EOF
{"data":"boo","ms":5}
{"data":"foo","ms":5}
{"data":"meh","ms":5}
EOF
cat $TMPDIR/input.txt | $BINARY | grep -v "^DEBUG" | sort >$TMPDIR/output.txt
if ! $DIFF $TMPDIR/golden.txt $TMPDIR/output.txt ; then
    echo -e '\e[1;31mFAIL\e[0m'
    exit 1
fi
echo -e '\e[1;32mOK\e[0m'
rm -rf $TMPDIR


echo -e '\e[1;32mPASS\e[0m'
exit 0
