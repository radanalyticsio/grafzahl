#!/bin/bash

if [ $# -ne 4 ]; then
	echo "Usage: $0 <Input Filename> <Output Filename Prefix> <Lines Per Interval> <Seconds Per Interval>"
	exit
fi

NUMLINES=`wc -l $1 | cut -f1 -d' '`
LASTINTERVAL=$(($NUMLINES-$3))
LINE=$3

until [ $LINE -gt $LASTINTERVAL  ]; do	
	SET=`head -n$LINE $1 | tail -n$3`
	echo "$SET" > $2-$LINE.out
	let LINE=$(($LINE+$3))
	sleep $4
done
