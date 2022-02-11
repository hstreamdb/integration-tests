package io.hstream.testing;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.ReceivedRawRecord;
import io.hstream.Record;
import io.hstream.RecordId;
import io.hstream.Subscription;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {

  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
  private static final DockerImageName defaultHstreamImageName =
      DockerImageName.parse("hstreamdb/hstream:latest");

  public static String randText() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static byte[] randBytes() {
    return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
  }

  public static Record randRawRec() {
    return buildRecord(randBytes());
  }

  public static Record buildRecord(byte[] xs) {
    return Record.newBuilder().rawRecord(xs).build();
  }

  public static Record buildRecord(HRecord xs) {
    return Record.newBuilder().hRecord(xs).build();
  }

  public static String randStream(HStreamClient c) {
    String streamName = "test_stream_" + randText();
    c.createStream(streamName, (short) 3);
    return streamName;
  }

  public static String randSubscriptionWithTimeout(
      HStreamClient c, String streamName, int timeout) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .ackTimeoutSeconds(timeout)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    final String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName).build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  // -----------------------------------------------------------------------------------------------

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse("zookeeper")).withNetworkMode("host");
  }

  private static DockerImageName getHstreamImageName() {
    String hstreamImageName = System.getenv("HSTREAM_IMAGE_NAME");
    if (hstreamImageName == null || hstreamImageName.equals("")) {
      logger.info(
          "No env variable HSTREAM_IMAGE_NAME found, use default name {}", defaultHstreamImageName);
      return defaultHstreamImageName;
    } else {
      logger.info("Found env variable HSTREAM_IMAGE_NAME = {}", hstreamImageName);
      return DockerImageName.parse(hstreamImageName);
    }
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer<>(getHstreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "127.0.0.1 "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static GenericContainer<?> makeHServer(
      String address,
      int port,
      int internalPort,
      Path dataDir,
      String zkHost,
      String hstoreHost,
      int serverId) {
    return new GenericContainer<>(getHstreamImageName())
        .withNetworkMode("host")
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withCommand(
            "bash",
            "-c",
            " hstream-server"
                + " --host "
                + "127.0.0.1 "
                + " --port "
                + port
                + " --internal-port "
                + internalPort
                + " --address "
                + address
                + " --server-id "
                + serverId
                + " --zkuri "
                + zkHost
                + ":2181"
                + " --store-config "
                + "/data/hstore/logdevice.conf "
                + " --store-admin-port "
                + "6440"
                + " --log-level "
                + "debug"
                + " --log-with-color"
                + " --store-log-level "
                + "error")
        .waitingFor(Wait.forLogMessage(".*Server is starting on port.*", 1));
  }

  // -----------------------------------------------------------------------------------------------

  public static void writeLog(ExtensionContext context, String entryName, String grp, String logs)
      throws Exception {
    String testClassName = context.getRequiredTestClass().getSimpleName();
    String testName = context.getTestMethod().get().getName();
    String fileName = "../.logs/" + testClassName + "/" + testName + "/" + grp + "/" + entryName;
    logger.info("log to " + fileName);

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(logs);
    writer.close();
  }

  // -----------------------------------------------------------------------------------------------

  public static boolean isAscending(List<RecordId> input) {
    if (input.isEmpty()) {
      return true;
    }
    if (input.size() == 1) {
      return true;
    }

    for (int i = 1; i < input.size(); i++) {
      if (input.get(i - 1).compareTo(input.get(i)) >= 0) {
        return false;
      }
    }
    return true;
  }

  public static void assertRecordIdsAscending(List<ReceivedRawRecord> input) {
    Assertions.assertTrue(
        isAscending(
            input.stream().map(ReceivedRawRecord::getRecordId).collect(Collectors.toList())),
        "is not ascending");
  }

  public static Consumer createConsumer(
      HStreamClient client,
      String subscription,
      String name,
      List<ReceivedRawRecord> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    return client
        .newConsumer()
        .subscription(subscription)
        .name(name)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              lock.lock();
              records.add(receivedRawRecord);
              lock.unlock();
              responder.ack();
              latch.countDown();
            })
        .build();
  }

  public static Consumer createConsumerWithFixNumsRecords(
      HStreamClient client,
      int nums,
      String subscription,
      String name,
      Set<RecordId> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    final int maxReceivedCount = nums;
    AtomicInteger receivedRecordCount = new AtomicInteger(0);
    return client
        .newConsumer()
        .subscription(subscription)
        .name(name)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              if (receivedRecordCount.get() < maxReceivedCount) {
                lock.lock();
                var success = records.add(receivedRawRecord.getRecordId());
                lock.unlock();
                responder.ack();
                if (success && receivedRecordCount.incrementAndGet() == maxReceivedCount) {
                  latch.countDown();
                }
              }
            })
        .build();
  }

  public static Consumer createConsumerCollectStringPayload(
      HStreamClient client,
      String subscription,
      String name,
      List<String> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    return client
        .newConsumer()
        .subscription(subscription)
        .name(name)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              lock.lock();
              records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
              lock.unlock();
              responder.ack();
              latch.countDown();
            })
        .build();
  }

  public static ArrayList<String> doProduce(Producer producer, int payloadSize, int recordsNums) {
    Random rand = new Random();
    byte[] rRec = new byte[payloadSize];
    var records = new ArrayList<String>();
    var xs = new CompletableFuture[recordsNums];
    for (int i = 0; i < recordsNums; i++) {
      rand.nextBytes(rRec);
      records.add(Arrays.toString(rRec));
      xs[i] = producer.write(Record.newBuilder().rawRecord(rRec).build());
    }
    CompletableFuture.allOf(xs).join();
    Assertions.assertEquals(recordsNums, records.size());
    return records;
  }

  public static ArrayList<RecordId> doProduceAndGatherRid(
      Producer producer, int payloadSize, int recordsNums) {
    var rids = new ArrayList<RecordId>();
    Random rand = new Random();
    byte[] rRec = new byte[payloadSize];
    var writes = new ArrayList<CompletableFuture<RecordId>>();
    for (int i = 0; i < recordsNums; i++) {
      rand.nextBytes(rRec);
      writes.add(producer.write(buildRecord(rRec)));
    }
    writes.forEach(w -> rids.add(w.join()));
    Assertions.assertEquals(recordsNums, rids.size());
    return rids;
  }

  public static void restartServer(GenericContainer<?> server) throws Exception {
    Thread.sleep(1000);
    server.close();
    Thread.sleep(5000); // need time to let zk clear old data
    logger.info("begin restart!");
    try {
      if (server.isRunning()) Thread.sleep(2000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
    } catch (ContainerLaunchException e) {
      logger.info("start hserver failed, try another restart.");
      server.close();
      Thread.sleep(5000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
      Thread.sleep(2000);
    }
  }

  private static void printFlag(String flag, ExtensionContext context) {
    logger.info(
        "=====================================================================================");
    logger.info(
        "{} {} {} {}",
        flag,
        context.getRequiredTestInstance().getClass().getSimpleName(),
        context.getTestMethod().get().getName(),
        context.getDisplayName());
    logger.info(
        "=====================================================================================");
  }

  public static void printBeginFlag(ExtensionContext context) {
    printFlag("begin", context);
  }

  public static void printEndFlag(ExtensionContext context) {
    printFlag("end", context);
  }
}
