package io.hstream.testing;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.ReceivedRawRecord;
import io.hstream.RecordId;
import io.hstream.Subscription;
import io.hstream.SubscriptionOffset;
import io.hstream.SubscriptionOffset.SpecialOffset;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {

  public static String randText() {
    return "test_stream_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static String randStream(HStreamClient c) {
    String streamName = randText();
    c.createStream(streamName, (short) 3);
    return streamName;
  }

  public static String randSubscriptionWithOffset(
      HStreamClient c, String streamName, SubscriptionOffset offset) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(offset)
            .ackTimeoutSeconds(5)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscriptionWithTimeout(
      HStreamClient c, String streamName, int timeout) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(new SubscriptionOffset(SpecialOffset.EARLIEST))
            .ackTimeoutSeconds(timeout)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    return randSubscriptionWithOffset(c, streamName, new SubscriptionOffset(SpecialOffset.LATEST));
  }

  public static String randSubscriptionFromEarliest(HStreamClient c, String streamName) {
    return randSubscriptionWithOffset(
        c, streamName, new SubscriptionOffset(SpecialOffset.EARLIEST));
  }

  // -----------------------------------------------------------------------------------------------

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse("zookeeper")).withNetworkMode("host");
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer<>(DockerImageName.parse("hstreamdb/hstream:latest"))
        // .withNetwork(network)
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
                // + "$(hostname -I | cut -f1 -d' ') "
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
    return new GenericContainer<>(DockerImageName.parse("hstreamdb/hstream:latest"))
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
    System.out.println("[DEBUG]: log to " + fileName);

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(logs);
    writer.close();
  }

  // -----------------------------------------------------------------------------------------------

  public static boolean isAscending(List<RecordId> input) {
    if (input.isEmpty()) {
      return false;
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
            input.stream().map(ReceivedRawRecord::getRecordId).collect(Collectors.toList())));
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
      xs[i] = producer.write(rRec);
    }
    CompletableFuture.allOf(xs).join();
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
      writes.add(producer.write(rRec));
    }
    writes.forEach(w -> rids.add(w.join()));
    return rids;
  }

  public static void restartServer(GenericContainer<?> server) throws Exception {
    server.close();
    Thread.sleep(5000); // need time to let zk clear old data
    System.out.println("begin restart!");
    try {
      if (server.isRunning()) Thread.sleep(2000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
    } catch (ContainerLaunchException e) {
      System.out.println("start hserver failed, try another restart.");
      server.close();
      Thread.sleep(5000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
    }
  }
}
