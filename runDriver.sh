#!/bin/sh

CLASSPATH="$CLASSPATH:./lib/db_driver/*"
CLASSPATH="$CLASSPATH:./distrib"

java -cp "$CLASSPATH" Driver $1 $2 $3 $4 $5 $6 $7 $8
