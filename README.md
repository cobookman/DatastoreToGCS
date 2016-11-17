# Datastore to GCS
Dataflow job which reads from Datastore, converts entities to json,
and writes the newline seperated json to a GCS folder.

# How do I use this?
Simply install gradle, then modify & execute the `run.sh` script.

You'll need to specify your own:
 - project (GCP Project ID)
 - staging location (for where the dataflow code is staged)
 - temp location (a temporary gcs write disk)
 - datastore entity kind (Name of the datastore entity you are backing up)
 - output filepath (Where to output data)
 - isBlocking (if put in the command, the java binary will block until the
   dataflow job finishes).

