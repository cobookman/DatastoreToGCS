#!/usr/bin/env bash

PROJECT="strong-moose"
GCS_TEMPLATE="gs://strong-moose.appspot.com/templates4/datastoreToGcs"
GCS_SAVE="gs://strong-moose.appspot.com/backups/my-backups3/"
GCS_TRANSFORM="gs://strong-moose.appspot.com/transform.js"
GQL="SELECT * FROM Drawing"
JOB_NAME=""

if [[ -z $PROJECT ]]; then
  echo -n "What is the project Id: "
  read PROJECT
fi

if [[ -z $GCS_TEMPLATE ]]; then
  echo -n "Where is the Dataflow Template Located: "
  read GCS_TEMPLATE
fi

if [[ -z $GQL ]]; then
  echo -n "GQL Query of datastore entities to fetch: "
  read GQL
fi

if [[ -z $GCS_SAVE ]]; then
  echo -n "Where to save datstore entities: "
  read GCS_SAVE
fi

if [[ -z $JOB_NAME ]]; then
  echo -n "What should the job name be (no spaces): "
  read JOB_NAME
fi

if [[ -z $GCS_TRANSFORM ]]; then
  echo -n "What is the GCS path of the javascript transform: "
  read GCS_TRANSFORM
fi

gcloud beta dataflow jobs run $JOB_NAME --gcs-location="$GCS_TEMPLATE" --project=$PROJECT --parameters gcsSavePath="$GCS_SAVE",gqlQuery="$GQL",project=$PROJECT,gcsJsTransformFns=$GCS_TRANSFORM
