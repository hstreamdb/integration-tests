package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.restartServer;
import static io.hstream.testing.TestUtils.writeLog;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterKillNodeTest {

  private static final Logger logger = LoggerFactory.getLogger(ClusterKillNodeTest.class);
  private final Random random = new Random(System.currentTimeMillis());
  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private String logMsgPathPrefix;
  private ExtensionContext context;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setLogMsgPathPrefix(String logMsgPathPrefix) {
    this.logMsgPathPrefix = logMsgPathPrefix;
  }

  public void setExtensionContext(ExtensionContext context) {
    this.context = context;
  }

  private void terminateHServerWithLogs(int turn, int serverId) throws Exception {
    logger.debug("terminate HServer" + serverId);
    String logs = hServers.get(serverId).getLogs();
    Assertions.assertNotNull(logs);
    writeLog(context, "hserver-" + serverId + "-turn-" + turn, logMsgPathPrefix, logs);
    hServers.get(serverId).close();
  }

  @BeforeEach
  public void setup() throws Exception {
    logger.debug("hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  @Test
  @Timeout(60)
  void listStreamAfterKillNodes() {
    String stream = randStream(hStreamClient);
    hServers.get(0).close();
    hServers.get(1).close();
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
  }

  @Test
  @Timeout(60)
  void testListSubscriptionAfterKillNodes() {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hServers.get(0).close();
    hServers.get(1).close();
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
  }

  @Test
  @Timeout(60)
  void testListStreamsShouldFailWhenAllNodesAreUnavailable() throws Exception {
    for (int i = 0; i < 3; i++) {
      terminateHServerWithLogs(0, i);
    }
    Assertions.assertThrows(Exception.class, hStreamClient::listStreams);
  }

  @Test
  @Timeout(60)
  void listSubscriptionAfterKillNodes() {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hServers.get(0).close();
    hServers.get(1).close();
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
  }

  @RepeatedTest(5)
  @Timeout(60)
  void testWrite() throws Exception {
    String streamName = TestUtils.randText();
    logger.debug("HServer cluster size is " + hServers.size());
    int luckyServer = random.nextInt(hServers.size());
    System.out.println("lucky server is " + luckyServer);
    hStreamClient.createStream(streamName);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    new Thread(
            () -> {
              for (int i = 0; i < hServers.size(); ++i) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                if (i != luckyServer) {
                  try {
                    terminateHServerWithLogs(0, i);
                  } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                }
              }
            })
        .start();

    for (int i = 0; i < hServers.size() * 20; ++i) {
      System.out.println("ready for writing record " + i);
      var recordId = producer.write(("hello" + i).getBytes(StandardCharsets.UTF_8)).join();
      System.out.println(recordId);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @RepeatedTest(3)
  void testReadHalfWayDropNodes() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int cnt = 32;

    ArrayList<Integer> xs = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      xs.add(i);
    }
    Collections.shuffle(xs);

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    ArrayList<RecordId> recordIds0 = new ArrayList<>();

    for (int i = 0; i < cnt; ++i) {
      recordIds0.add(
          producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    }
    Assertions.assertEquals(cnt, recordIds0.size());

    CountDownLatch countDown = new CountDownLatch(cnt);
    ArrayList<RecordId> recordIds1 = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .name("newConsumer")
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recordIds1.add(recs.getRecordId());
                  recv.ack();
                  countDown.countDown();
                  if (countDown.getCount() == 10 || countDown.getCount() == 20) {
                    try {
                      terminateHServerWithLogs(0, xs.get((int) (countDown.getCount() / 10)));
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                })
            .build();

    consumer.startAsync().awaitRunning();
    countDown.await();
    consumer.stopAsync().awaitTerminated();

    Assertions.assertEquals(recordIds0, recordIds1);
  }

  @Test
  void testWriteAfterKillStreamHost() throws Exception {
    String stream = randStream(hStreamClient);

    Random rand = new Random();
    byte[] randRecs = new byte[128];

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id0 = producer.write(randRecs).join();

    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);

    producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id1 = producer.write(randRecs).join();

    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }

  @Test
  void testRestartNodeJoinCluster() throws Exception {
    terminateHServerWithLogs(0, 2);
    Thread.sleep(10 * 1000);
    String stream = randStream(hStreamClient);
    Thread.sleep(5 * 1000);
    hServers.get(2).start();
    Thread.sleep(15 * 1000);
    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);
    Thread.sleep(5 * 1000);

    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());

    Random rand = new Random();
    byte[] randRecs = new byte[128];
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id0 = producer.write(randRecs).join();
    rand.nextBytes(randRecs);
    RecordId id1 = producer.write(randRecs).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }

  @Test
  void testWriteAfterKillNodes() throws Exception {
    terminateHServerWithLogs(0, 1);
    Thread.sleep(5 * 1000);
    terminateHServerWithLogs(0, 2);
    Thread.sleep(5 * 1000);
    String stream = randStream(hStreamClient);

    restartServer(hServers.get(1));
    Assertions.assertTrue(hServers.get(1).isRunning());
    Assertions.assertNotNull(hServers.get(1).getLogs());
    Thread.sleep(5 * 1000);
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
    terminateHServerWithLogs(0, 0);
    Thread.sleep(5 * 1000);
    Assertions.assertFalse(hServers.get(0).isRunning());
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
    String subscription = randSubscription(hStreamClient, stream);

    restartServer(hServers.get(2));
    Thread.sleep(5 * 1000);
    Assertions.assertTrue(hServers.get(2).isRunning());
    Assertions.assertNotNull(hServers.get(2).getLogs());
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
    terminateHServerWithLogs(1, 1);
    Thread.sleep(5 * 1000);
    Assertions.assertFalse(hServers.get(1).isRunning());
    hStreamClient.close();
    hStreamClient = HStreamClient.builder().serviceUrl("127.0.0.1:6572").build();
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
    Producer producer = hStreamClient.newProducer().stream(stream).build();

    restartServer(hServers.get(0));
    Thread.sleep(5 * 1000);
    Assertions.assertTrue(hServers.get(0).isRunning());
    Assertions.assertNotNull(hServers.get(0).getLogs());
    ArrayList<RecordId> recordIds0 = new ArrayList<>();
    recordIds0.add(
        producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    recordIds0.add(
        producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());

    terminateHServerWithLogs(1, 2);
    Thread.sleep(5 * 1000);
    Assertions.assertFalse(hServers.get(2).isRunning());
    recordIds0.add(
        producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    recordIds0.add(
        producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());

    ArrayList<RecordId> recordIds1 = new ArrayList<>(recordIds0);
    recordIds1.sort(RecordId::compareTo);
    Assertions.assertEquals(recordIds0, recordIds1);
  }

  @Test
  void testJoinConsumerGroupBeforeAndAfterKillNodes() throws Exception {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    ArrayList<RecordId> recordIds = new ArrayList<>();
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    for (int i = 0; i < 32; ++i) {
      recordIds.add(
          producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    }

    ArrayList<RecordId> recordIds1 = new ArrayList<>();
    CountDownLatch countDownLatch1 = new CountDownLatch(32);
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recordIds1.add(recs.getRecordId());
                  countDownLatch1.countDown();
                })
            .build();
    consumer1.startAsync().awaitRunning();
    countDownLatch1.await();
    consumer1.stopAsync().awaitTerminated();

    List<Integer> serverIds =
        Arrays.stream(new int[] {0, 1, 2}).boxed().collect(Collectors.toList());
    Collections.shuffle(serverIds);
    terminateHServerWithLogs(0, serverIds.get(0));
    terminateHServerWithLogs(0, serverIds.get(1));
    Thread.sleep(2000);

    ArrayList<RecordId> recordIds2 = new ArrayList<>();
    CountDownLatch countDownLatch2 = new CountDownLatch(32);
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recordIds2.add(recs.getRecordId());
                  countDownLatch2.countDown();
                })
            .build();
    consumer2.startAsync().awaitRunning();
    countDownLatch2.await();
    consumer2.stopAsync().awaitTerminated();

    Assertions.assertEquals(recordIds, recordIds1);
    Assertions.assertEquals(recordIds, recordIds2);
  }
}
