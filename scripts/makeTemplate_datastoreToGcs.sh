#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

PROJECT="teleport-test-170818"
TEMP="gs://teleport-test/temp/"
TEMPLATE="gs://teleport-test/templates/datastoreToGcs"

if [[ -z $PROJECT ]]; then
  echo -n "What is the project Id: "
  read PROJECT
fi

if [[ -z $TEMP ]]; then
  echo -n "What is the temp location: "
  read TEMP
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where to store this template stored: "
  read TEMPLATE
fi

gradle clean build shadowJar

java -jar build/libs/shadow-1.0-Alpha.jar \
  datastore_to_gcs \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --gcpTempLocation=$TEMP \
  --templateLocation=$TEMPLATE

if [ $? -eq 0 ]; then
  echo "Success! Built Template"
else
  echo "Failed to build Template :'("
fi