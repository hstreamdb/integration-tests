# HStream integration tests

This repository contains integration tests for HStreamDB based
on [Testcontainers](https://www.testcontainers.org/).

## How to run the cases?

Run Single case:

```
./gradlew :app:test --tests "io.hstream.testing.ProducerTest.testWriteRaw"
```

Re-run tasks:

```
./gradlew :app:test --rerun-tasks --tests "io.hstream.testing.ProducerTest.testWriteRaw"
```

You can use ``--info`` to show info logs.

Run the cases of a Class (or file):

```
./gradlew :app:test  --tests "io.hstream.testing.ProducerTest" --info
```

Run the cases by Tag:

```
./gradlew test -Dtag='basicTest' --info
```

## How to run the cases using local Java Client

If you have no idea about the signing stuff,
you can run ``./gradlew publishToMavenLocal -PdisableSigning`` in Java Client Project to publish to local maven repository.

After that, change the repositories part of [app/build.gradle](app/build.gradle) to use local Java Client:

```
repositories {
    // Search dependencies by order,
    // when Java Client is published to mavenLocal,
    // the project will use the local Java Client first.
    mavenLocal()
    mavenCentral()
    maven {
        url 'https://s01.oss.sonatype.org/content/repositories/snapshots/'
    }
}
```

Note: you should check whether the versions between published Java Client and dependencies in project are same,
which version and source of Java Client is used will be printed in the building logs.

## How to run the cases using local hstream images

By default, the project will use hstreamdb/hstream:latest for servers,
you can export ``HSTREAM_IMAGE_NAME`` variables if you want use a specific image:

```
export HSTREAM_IMAGE_NAME=my_hstream_image
```

You can `dev-tools` in hstream project to generate a local image:
```
script/dev-tools quick-build-dev-image \
            --builder-image docker.io/hstreamdb/haskell \
            -t my_hstream_image
```


## How to check the logs

After running cases, it will generate a report for the testing,
the report includes logs of Java Client and integration-tests,
you can find them from app/build/reports/tests/test/index.html,
and for each case, the logs path of servers will be printed in the report logs:

```
14:22:25.992 [Test worker] INFO  io.hstream.testing.TestUtils - log to .logs/ClusterTest/testMultiThreadDeleteSameStream/f13a413c-4c00-48d1-a6ca-6ba5e276fc01/hserver-0
14:22:26.148 [Test worker] INFO  io.hstream.testing.TestUtils - log to .logs/ClusterTest/testMultiThreadDeleteSameStream/f13a413c-4c00-48d1-a6ca-6ba5e276fc01/hserver-1
14:22:26.264 [Test worker] INFO  io.hstream.testing.TestUtils - log to .logs/ClusterTest/testMultiThreadDeleteSameStream/f13a413c-4c00-48d1-a6ca-6ba5e276fc01/hserver-2
14:22:26.354 [Test worker] INFO  io.hstream.testing.TestUtils - log to .logs/ClusterTest/testMultiThreadDeleteSameStream/f13a413c-4c00-48d1-a6ca-6ba5e276fc01/hstore
14:22:26.449 [Test worker] INFO  io.hstream.testing.TestUtils - log to .logs/ClusterTest/testMultiThreadDeleteSameStream/f13a413c-4c00-48d1-a6ca-6ba5e276fc01/zk
```

For CI in Github, you can download the report and .logs(the logs of server) from Summary
and check the failed cases in report to find out the logs path of servers in the report logs.
