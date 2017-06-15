#!/usr/bin/env bash

DATAFLOW_PROJECT="teleport-test-170818"
DATASTORE_PROJECT="teleport-test-170818"
TEMPLATE="gs://teleport-test/templates/datastoreToGcs"
SAVE_PATH="gs://teleport-test/backups/"
TRANSFORM_PATH="gs://teleport-test/transforms/"
GQL="SELECT * FROM SomeKind"
JOB_NAME=""


if [[ -z $DATAFLOW_PROJECT ]]; then
  echo -n "Project to run dataflow job in: "
  read DATAFLOW_PROJECT
fi

if [[ -z $DATASTORE_PROJECT ]]; then
  echo -n "Project to pull Datastore Entities From: "
  read DATASTORE_PROJECT
fi

if [[ -z $TEMPLATE ]]; then
  echo -n "Where is the Dataflow Template Located: "
  read TEMPLATE
fi

if [[ -z $SAVE_PATH ]]; then
  echo -n "Where to save datstore entities: "
  read SAVE_PATH
fi

if [[ -z $GCS_TRANSFORM ]]; then
  echo -n "What is the GCS path of the javascript transform: "
  read GCS_TRANSFORM
fi

if [[ -z $GQL ]]; then
  echo -n "GQL Query of datastore entities to fetch: "
  read GQL
fi

if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi


gcloud beta dataflow jobs run $JOB_NAME \
  --gcs-location="$TEMPLATE" \
  --project=$DATAFLOW_PROJECT \
  --parameters gcsSavePath="$SAVE_PATH",gqlQuery="$GQL",datastoreProject=$DATASTORE_PROJECT,gcsJsTransformFns=$GCS_TRANSFORM
