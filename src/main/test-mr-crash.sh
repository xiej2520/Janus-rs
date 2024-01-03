#!/usr/bin/env bash

ISQUIET=$1
maybe_quiet() {
    if [ "$ISQUIET" == "quiet" ]; then
      "$@" > /dev/null 2>&1
    else
      "$@"
    fi
}


TIMEOUT=timeout
TIMEOUT2=""
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT2=$TIMEOUT
  TIMEOUT2+=" -k 2s 120s "
  TIMEOUT+=" -k 2s 45s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cargo build --bin mrcoordinator) || exit 1
(cargo build --bin mrworker) || exit 1
(cargo build --bin mrsequential) || exit 1

mrcoordinator=../target/debug/mrcoordinator
mrworker=../target/debug/mrworker
mrsequential=../target/debug/mrsequential

failed_any=0

#########################################################
echo '***' Starting crash test.

# generate the correct output
$mrsequential nocrash ../rsrc/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
((maybe_quiet $TIMEOUT2 $mrcoordinator ../rsrc/pg*txt); touch mr-done ) &
sleep 1

# start multiple workers
maybe_quiet $TIMEOUT2 $mrworker crash &

# check if netcat is available
if ! command -v nc &> /dev/null; then
    echo "Error: netcat (nc) is not installed."
    exit 1
fi
## tonic can work with unix domain socket (see uds example, try later)
#SOCKNAME=/var/tmp/5840-mr-`id -u`

# check if tonic server is running on [::1]:50051
( while nc -z ::1 50051 && [ ! -f mr-done ]
  do
    echo "STARTING NEW WORKER"
    maybe_quiet $TIMEOUT2 $mrworker crash
    sleep 1
  done ) &

( while nc -z ::1 50051 && [ ! -f mr-done ]
  do
    echo "STARTING NEW WORKER"
    maybe_quiet $TIMEOUT2 $mrworker crash
    sleep 1
  done ) &

while nc -z ::1 50051 && [ ! -f mr-done ]
do
  echo "STARTING NEW WORKER"
  maybe_quiet $TIMEOUT2 $mrworker crash
  sleep 1
done

wait

#rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi
