package io.hstream.testing;

import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.makeBufferedProducer;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.restartServer;
import static io.hstream.testing.Utils.ConsumerService.createConsumerCollectStringPayload;

import io.hstream.BufferedProducer;
import io.hstream.Consumer;
import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(BasicExtension.class)
class SingleNodeTest {

  private static final Logger logger = LoggerFactory.getLogger(SingleNodeTest.class);
  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private GenericContainer<?> server;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setServer(GenericContainer<?> s) {
    this.server = s;
  }

  @BeforeEach
  public void setup() throws Exception {
    logger.debug("db url: " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Disabled("HS-946")
  @Test
  @Timeout(60)
  void testGetResourceAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    restartServer(server);
    var streams = hStreamClient.listStreams();
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    var subscriptions = hStreamClient.listSubscriptions();
    Assertions.assertEquals(subscription, subscriptions.get(0).getSubscriptionId());
  }

  @Disabled("restart may cause test fail, disable")
  @Test
  @Timeout(60)
  void testReconsumeAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    var records = doProduce(producer, 128, 100);
    producer.close();
    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);

    restartServer(server);
    res.clear();
    CountDownLatch notify2 = new CountDownLatch(records.size());

    final String subscription1 = randSubscription(hStreamClient, streamName);
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription1, "test-consumer", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(20, TimeUnit.SECONDS);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }

  @Disabled("HS-946")
  @Test
  @Timeout(60)
  void testConsumeAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    var records = doProduce(producer, 128, 100);
    producer.close();

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);

    restartServer(server);
    res.clear();

    BufferedProducer producer2 = makeBufferedProducer(hStreamClient, streamName, 10);
    records = doProduce(producer2, 1, 10);
    producer2.close();
    CountDownLatch notify2 = new CountDownLatch(records.size());
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer-new", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(20, TimeUnit.SECONDS);
    Thread.sleep(1000);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }
}
