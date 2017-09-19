# qdb-spark-conenctor
Official quasardb Spark Connector

# Tests

First launch a temporary spark cluster using docker-compose:

```docker-compose -f vendor/docker-spark/docker-compose.yml up -d```

Then launch the integration test using sbt:


```sbt test```
