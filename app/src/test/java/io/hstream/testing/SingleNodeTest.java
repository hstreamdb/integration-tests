package io.hstream.testing;

import static io.hstream.testing.TestUtils.createConsumerCollectStringPayload;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscriptionFromEarliest;
import static io.hstream.testing.TestUtils.restartServer;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
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
import org.testcontainers.containers.GenericContainer;

@ExtendWith(BasicExtension.class)
class SingleNodeTest {

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
    System.out.println("db url: " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Disabled("restart may cause test fail, disable")
  @Test
  @Timeout(60)
  void testGetResourceAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
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
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    var records = doProduce(producer, 128, 100);
    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);

    restartServer(server);
    res.clear();
    CountDownLatch notify2 = new CountDownLatch(records.size());

    final String subscription1 = randSubscriptionFromEarliest(hStreamClient, streamName);
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription1, "test-consumer", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(10, TimeUnit.SECONDS);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }

  @Disabled("enable after HS-806 fix.")
  @Test
  @Timeout(60)
  void testConsumeAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    var records = doProduce(producer, 128, 100);

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);

    restartServer(server);
    res.clear();

    Producer producer2 =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(10).build();
    records = doProduce(producer2, 1, 10);
    CountDownLatch notify2 = new CountDownLatch(records.size());
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer-new", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(10, TimeUnit.SECONDS);
    Thread.sleep(1000);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }
}
