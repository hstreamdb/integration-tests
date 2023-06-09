package io.hstream.testing;

import static io.hstream.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.testing.TestUtils.buildRecord;
import static io.hstream.testing.TestUtils.randRawRec;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.writeLog;
import static org.assertj.core.api.Assertions.*;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  private List<GenericContainer<?>> hservers;
  private List<String> hserverUrls;
  private String logMsgPathPrefix;
  private ExtensionContext context;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hservers) {
    this.hservers = hservers;
  }

  public void setHServerUrls(List<String> hserverUrls) {
    this.hserverUrls = hserverUrls;
  }

  public void setLogMsgPathPrefix(String logMsgPathPrefix) {
    this.logMsgPathPrefix = logMsgPathPrefix;
  }

  public void setExtensionContext(ExtensionContext context) {
    this.context = context;
  }

  private void terminateHServerWithLogs(int turn, int serverId) throws Exception {
    logger.info("terminate HServer" + serverId);
    String logs = hservers.get(serverId).getLogs();
    Assertions.assertNotNull(logs);
    writeLog(context, "hserver-" + serverId + "-turn-" + turn, logMsgPathPrefix, logs);
    hservers.get(serverId).close();
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
    hservers.get(0).close();
    hservers.get(1).close();
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
  }

  @Test
  @Timeout(60)
  void testListSubscriptionAfterKillNodes() {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hservers.get(0).close();
    hservers.get(1).close();
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
  }

  @Test
  @Timeout(60)
  void testListStreamsShouldFailWhenAllNodesAreUnavailable() throws Exception {
    for (int i = 0; i < CLUSTER_SIZE; i++) {
      terminateHServerWithLogs(0, i);
    }
    Assertions.assertThrows(Exception.class, hStreamClient::listStreams);
  }

  // @Disabled("create log group issue")
  @RepeatedTest(5)
  @Timeout(60)
  void testWrite() {
    String streamName = TestUtils.randText();
    logger.debug("HServer cluster size is " + hservers.size());
    int luckyServer = random.nextInt(hservers.size());
    logger.info("lucky server is " + luckyServer);
    hStreamClient.createStream(streamName);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    new Thread(
            () -> {
              for (int i = 0; i < hservers.size(); ++i) {
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

    for (int i = 0; i < hservers.size() * 20; ++i) {
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

  @Test
  @Timeout(90)
  void testReadHalfWayDropNodes() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int cnt = 10000;

    ConcurrentLinkedQueue<Integer> xs = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 3; ++i) {
      xs.add(i);
    }

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    var recordIds0 = new HashSet<>();

    for (int i = 0; i < cnt; ++i) {
      recordIds0.add(producer.write(randRawRec()).join());
    }
    Assertions.assertEquals(cnt, recordIds0.size());

    CountDownLatch countDown = new CountDownLatch(cnt);
    Set<String> recordIds1 = new HashSet<>();
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

                  if (countDown.getCount() == 8000 || countDown.getCount() == 4000) {
                    assertThatNoException()
                        .isThrownBy(() -> terminateHServerWithLogs(0, xs.poll()));
                  }
                })
            .build();

    consumer.startAsync().awaitRunning();
    assertThat(countDown.await(60, TimeUnit.SECONDS)).isTrue();
    consumer.stopAsync().awaitTerminated();
    assertThat(recordIds1).isEqualTo(recordIds0);
  }

  @Test
  @Timeout(60)
  void testStreamCanBeListWriteFromServerWithDifferentLifetime() throws Exception {
    terminateHServerWithLogs(0, 2);
    Thread.sleep(10 * 1000);
    String stream = randStream(hStreamClient);
    Thread.sleep(5 * 1000);

    hservers.get(2).start();

    Thread.sleep(10 * 1000);

    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);

    Thread.sleep(5 * 1000);
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
    Random rand = new Random();
    byte[] randRecs = new byte[128];
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    String id0 = producer.write(buildRecord(randRecs)).join();
    rand.nextBytes(randRecs);
    String id1 = producer.write(buildRecord(randRecs)).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }
}
