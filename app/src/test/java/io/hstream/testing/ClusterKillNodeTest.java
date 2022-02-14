package io.hstream.testing;

import static io.hstream.testing.TestUtils.buildRecord;
import static io.hstream.testing.TestUtils.doProduceAndGatherRid;
import static io.hstream.testing.TestUtils.randRawRec;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.writeLog;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
  void testListStreamAfterKillNodes() {
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

  @Disabled("create log group issue")
  @RepeatedTest(5)
  @Timeout(60)
  void testWrite() throws Exception {
    String streamName = TestUtils.randText();
    logger.debug("HServer cluster size is " + hServers.size());
    int luckyServer = random.nextInt(hServers.size());
    logger.info("lucky server is " + luckyServer);
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
      logger.info("ready for writing record " + i);
      var recordId =
          producer.write(buildRecord(("hello" + i).getBytes(StandardCharsets.UTF_8))).join();
      logger.info("recordId is " + String.valueOf(recordId));
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Disabled("HS-946")
  @RepeatedTest(5)
  @Timeout(90)
  void testReadHalfWayDropNodes() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int cnt = 10000;

    ArrayList<Integer> xs = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      xs.add(i);
    }
    Collections.shuffle(xs);

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    ArrayList<RecordId> recordIds0 = new ArrayList<>();

    for (int i = 0; i < cnt; ++i) {
      recordIds0.add(producer.write(randRawRec()).join());
    }
    Assertions.assertEquals(cnt, recordIds0.size());

    AtomicReference<Exception> e = new AtomicReference<>();
    e.set(null);

    CountDownLatch countDown = new CountDownLatch(cnt);
    Set<RecordId> recordIds1 = new HashSet<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .name("newConsumer")
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    countDown.countDown();
                  }
                  recv.ack();

                  if (countDown.getCount() == 1000 || countDown.getCount() == 2000) {
                    try {
                      terminateHServerWithLogs(0, xs.get((int) (countDown.getCount() / 1000)));
                    } catch (Exception curE) {
                      curE.printStackTrace();
                      logger.error(curE.getMessage());
                      e.set(curE);
                    }
                  }
                })
            .build();

    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDown.await(60, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();

    Assertions.assertNull(e.get());
    Assertions.assertEquals(new HashSet<>(recordIds0), recordIds1);
  }

  @Test
  @Timeout(60)
  void testStreamCanBeListWriteFromServerWithDifferentLifetime() throws Exception {
    terminateHServerWithLogs(0, 2);
    Thread.sleep(10 * 1000);
    String stream = randStream(hStreamClient);
    Thread.sleep(5 * 1000);

    hServers.get(2).start();

    Thread.sleep(10 * 1000);

    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);

    Thread.sleep(5 * 1000);
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
    Random rand = new Random();
    byte[] randRecs = new byte[128];
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id0 = producer.write(buildRecord(randRecs)).join();
    rand.nextBytes(randRecs);
    RecordId id1 = producer.write(buildRecord(randRecs)).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }

  @Test
  @Timeout(60)
  void testJoinConsumerGroupBeforeAndAfterKillNodes() throws Exception {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    Set<RecordId> recordIds0 = new HashSet<>();
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    for (int i = 0; i < 32; ++i) {
      recordIds0.add(producer.write(randRawRec()).join());
    }

    List<Integer> serverIds =
        Arrays.stream(new int[] {0, 1, 2}).boxed().collect(Collectors.toList());
    Collections.shuffle(serverIds);
    terminateHServerWithLogs(0, serverIds.get(0));
    terminateHServerWithLogs(0, serverIds.get(1));
    Thread.sleep(2000);

    Set<RecordId> recordIds1 = new HashSet<>();
    CountDownLatch countDownLatch = new CountDownLatch(32);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    recv.ack();
                    countDownLatch.countDown();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDownLatch.await(20, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(recordIds0, recordIds1);
  }

  @Disabled("HS-946")
  @Test
  @Timeout(150)
  void testKillAllNodesThenRestartOneShouldConsumeAll() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int msgCnt = 2000;

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    Set<RecordId> recs0 = new HashSet<>(doProduceAndGatherRid(producer, 1, msgCnt));
    Set<RecordId> recs1 = new HashSet<>();
    CountDownLatch countDown0 = new CountDownLatch(msgCnt / 2);

    Consumer consumer0 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer0")
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (countDown0.getCount() <= msgCnt / 2) {
                    if (recs1.add(recs.getRecordId())) {
                      countDown0.countDown();
                    }
                    recv.ack();
                  }
                })
            .build();

    consumer0.startAsync().awaitRunning();
    Assertions.assertTrue(countDown0.await(20, TimeUnit.SECONDS));
    consumer0.stopAsync().awaitTerminated();

    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);
    terminateHServerWithLogs(0, 2);
    Thread.sleep(5 * 1000);
    hServers.get(2).start();
    Thread.sleep(1000);
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());

    CountDownLatch countDown1 = new CountDownLatch(msgCnt - recs1.size());
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer1")
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recs1.add(recs.getRecordId())) {
                    countDown1.countDown();
                    logger.debug("current size is {}", recs1.size());
                  } else {
                    logger.debug("dup rec");
                  }
                  recv.ack();
                })
            .build();
    consumer1.startAsync().awaitRunning();
    Assertions.assertTrue(countDown1.await(90, TimeUnit.SECONDS));
    consumer1.stopAsync().awaitTerminated();

    Assertions.assertEquals(recs0, recs1);
  }
}
