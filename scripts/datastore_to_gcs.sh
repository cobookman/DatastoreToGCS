#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

PROJECT="strong-moose"
STAGING="gs://strong-moose.appspot.com/staging/"
TEMP="gs://strong-moose.appspot.com/temp/"
TEMPLATE="gs://strong-moose.appspot.com/templates3/"

if [[ -z $PROJECT ]]; then
  echo -n "What is the project Id: "
  read PROJECT
fi

if [[ -z $STAGING ]]; then
  echo -n "What is the GCS staging location: "
  read STAGING
fi

if [[ -z $TEMP ]]; then
  echo -n "What is the temp location: "
  read TEMP
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where are templates stored: "
  read TEMPLATE
fi

gradle clean build uberjar

JOB_FILE="${JOB_FILE%/}/Datastore.$ENTITY_KIND.to.GCS"
echo "Saving template to: $JOB_FILE"

java -jar build/libs/*.jar \
  datastore_to_gcs \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --gcpTempLocation=$TEMP \
  --stagingLocation=$STAGING \
  --tempLocation=$TEMP 
#  --templateLocation=$TEMPLATE 
