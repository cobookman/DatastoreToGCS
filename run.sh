#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# BUILD java ubjer jar
gradle clean build uberjar

# Deploy streaming pipelien
java -jar $DIR/build/libs/*.jar \
   --project=project-foo \
   --stagingLocation=gs://project-foo-dataflow/staging/ \
   --tempLocation=gs://project-foo-datalow/temp/ \
   --outputLocation=gs://project-foos-datastore-backups/ \
   --datastoreEntityKind=People \
   --isBlocking

