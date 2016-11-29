# Datastore to GCS
Dataflow job which reads from Datastore, converts entities to json,
and writes the newline seperated json to a GCS folder.

# How do I use this?

## Install gradle & jdk

Not going to cover the specifics here, just google jdk + gradle installation for
your specific platform.


## Backing up datastore entities

Take a look at the backup.sh script. The following flags need to be set:

* project, your gcp project id
* stagingLocation, gcs path to a place gcs can use to store staging files
* tempLocation, gcs path to a scratch disk for the dataflow job
* backupGCSPrefix, where your backups should be placed
* datastoreEntityKind, the Datastore Entity [Kind](https://cloud.google.com/datastore/docs/concepts/entities#kinds_and_identifiers)

If you'd like the java program to block until the dataflow job is complete add
the `--isBlocking` flag.

Once the job is done you'll find newline seperated json sharded between a few
files located under the specified backupGCSPrefix.

For example say you had the following command:
```bash
java -jar build/libs/*.jar backup \
  --project=superman
  --stagingLocation=gs://superman-backups/staging/ \
  --tempLocation=gs://superman-backups/temp/ \
  --backupGCSPrefix=gs://superman-backups/datastore/ \
  --datastoreEntityKind=ComicBooks \
```

And you ran this command on 11/29/2016 on 11:10:55 am, your backups would in the
directory:
`gs://superman-backups/datastore/ComicBooks.2016.11.29_11.10.55/`

And that directory would have files with a naming scheme of something like:
```
gs://superman-backups/datastore/ComicBooks.2016.11.29_11.10.55/-00000-of-00002.json
gs://superman-backups/datastore/ComicBooks.2016.11.29_11.10.55/-00001-of-00002.json
```

## Restoring a backup

Take a look at the restore.sh script. The following flags need to be set:

* project, your gcp project id
* stagingLocation, gcs path to a place gcs can use to store staging files
* tempLocation, gcs path to a scratch disk for the dataflow job
* backupGCSPrefix, where your backups are stored
* datastoreEntityKind, the Datastore Entity
  [Kind](https://cloud.google.com/datastore/docs/concepts/entities#kinds_and_identifiers)

If you'd like the java program to block until the dataflow job is complete add
the `--isBlocking` flag.

