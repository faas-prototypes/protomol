#!/bin/bash
x=1
PROTOTYPE=$1"-Prototype"
ITERATIONS=$2
MAX_TEMP=$3
MIN_TEMP=$4
NUM_REPLICAS=$5

cd $PROTOTYPE

while [ $x -le $ITERATIONS ]
do

  python3.6 replica_exchange.py ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp  $MAX_TEMP $MIN_TEMP  $NUM_REPLICAS
  x=$(( $x + 1 ))
  rm -R __pycache__/
  rm -R simfiles
  sleep 2
done
