# es-curator-cleanup
This script looks for es indexes that are older than x days (using the index name).

It then generates a curator action yml file to move them into a monthly index and removes the old index.

For local dev look at the pythonSetup.sh

For running on a live server use the docker image

```
timhaak/es-curator-cleanup
```

  https://github.com/timhaak/docker-es-curator-cleanup

