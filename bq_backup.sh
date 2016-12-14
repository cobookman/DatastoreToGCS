#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# BUILD java ubjer jar
gradle clean build uberjar

# MAKE SURE TO EDIT THE BQ TABLE SCHEMA IN
# java/com/google/datastorebackup/BQSchema.java

# Deploy streaming pipelien
java -jar $DIR/build/libs/*.jar \
   bqbackup \
   --project=strong-moose \
   --stagingLocation=gs://strong-moose.appspot.com/staging/ \
   --tempLocation=gs://strong-moose.appspot.com/temp/ \
   --datastoreEntityKind=Drawing \
   --BQDataset=backups \
   --isBlocking

