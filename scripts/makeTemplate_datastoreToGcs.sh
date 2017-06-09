#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

PROJECT="strong-moose"
TEMP="gs://strong-moose.appspot.com/temp/"
TEMPLATE="gs://strong-moose.appspot.com/templates4/datastoreToGcs"

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

JOB_FILE="${JOB_FILE%/}/Datastore.$ENTITY_KIND.to.GCS"
echo "Saving template to: $JOB_FILE"

java -jar build/libs/shadow-1.0-Alpha.jar \
  datastore_to_gcs \
  --runner=DataflowRunner \
  --project=$PROJECT \
  --gcpTempLocation=$TEMP \
  --templateLocation=$TEMPLATE
