#!/bin/bash
x=1
while [ $x -le 100 ]
do
  python3.6 pywren_replica_exchange.py
  x=$(( $x + 1 ))
  rm -R __pycache__/
  rm -R simfiles
  sleep 5
done
