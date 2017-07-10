#!/bin/bash
echo "BROCCOLI CONSUMER"
echo "-------------"
echo "In another terminal, run 'python main.py'"
echo "Stop the consumer using Ctrl+C"
PYTHONPATH=.:$PYTHONPATH
export WORKER_CLASS=${1:-thread}
python ../../broccoli/bin/broccoli_consumer.py main.broccoli #--workers=4 -v -s 10 -k $WORKER_CLASS -c 60
