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
  # add 'BV' to each chunk (to have something more frequent)
	echo -e "BV\n$SET" > $2-$LINE.out
  # and also randomly add some other records w/ probability 1/5
  [ $(( ( RANDOM % 5 )  + 1 )) == 1 ] && echo "1-$2-$LINE.out" && echo "NECO" >> $2-$LINE.out
  [ $(( ( RANDOM % 5 )  + 1 )) == 1 ] && echo "2-$2-$LINE.out" && echo "NIC" >> $2-$LINE.out
  [ $(( ( RANDOM % 5 )  + 1 )) == 1 ] && echo "3-$2-$LINE.out" && echo "COZE" >> $2-$LINE.out
  [ $(( ( RANDOM % 5 )  + 1 )) == 1 ] && echo "4-$2-$LINE.out" && echo "PIVO" >> $2-$LINE.out
	let LINE=$(($LINE+$3))
	sleep $4
done
