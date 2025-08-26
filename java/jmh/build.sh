#!/bin/bash
set -e
mydir=$(realpath $(dirname $0))
cd $mydir/../target
cp rocksdbjni-8.10.2-linux64.jar rocksdbjni-8.10.2-SNAPSHOT-linux64.jar
mvn install:install-file -Dfile=rocksdbjni-8.10.2-SNAPSHOT-linux64.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=8.10.2-SNAPSHOT -Dpackaging=jar
cd $mydir
mvn clean package -T 1C
