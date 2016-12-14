#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# BUILD java ubjer jar
gradle clean build uberjar

# Deploy streaming pipelien
java -jar $DIR/build/libs/*.jar \
   gcsbackup \
   --project=some-project-id \
   --stagingLocation=gs://some-gcs-bucket-of-mine/staging/ \
   --tempLocation=gs://some-gcs-bucket-of-mine/temp/ \
   --backupGCSPrefix=gs://some-gcs-bucket-of-mine/path-to-where-to-store-backups/ \
   --datastoreEntityKind=some-datastore-entity \
   --isBlocking

