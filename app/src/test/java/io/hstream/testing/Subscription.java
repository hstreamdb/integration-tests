package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Subscription {
  private static final Logger logger = LoggerFactory.getLogger(Subscription.class);
  private HStreamClient client;

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(60)
  void testListSubscriptions() {
    String streamName = randStream(client);
    var subscriptions = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      subscriptions.add(randSubscription(client, streamName));
    }
    var res =
        client.listSubscriptions().parallelStream()
            .map(io.hstream.Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(subscriptions.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testGetSubscription() {
    String streamName = randStream(client);
    String subName = randSubscription(client, streamName);
    var sub = client.getSubscription(subName);
    Assertions.assertEquals(subName, sub.getSubscription().getSubscriptionId());
    Assertions.assertEquals(streamName, sub.getSubscription().getStreamName());
    Assertions.assertNotNull(sub.getOffsets());
  }

  @Test
  @Timeout(60)
  void testGetSubscriptionWithConsumer() throws Exception {
    String streamName = randStream(client);
    String subName = randSubscription(client, streamName);
    var producer = client.newBufferedProducer().stream(streamName).build();
    // write and consume some data
    TestUtils.produce(producer, 100, 10);
    var count = new AtomicInteger();
    TestUtils.consume(client, subName, 10, receivedRawRecord -> count.incrementAndGet() < 10);

    var sub = client.getSubscription(subName);
    Assertions.assertEquals(subName, sub.getSubscription().getSubscriptionId());
    Assertions.assertEquals(streamName, sub.getSubscription().getStreamName());
    Assertions.assertFalse(sub.getOffsets().isEmpty());
  }

  @Test
  @Timeout(20)
  void testDeleteNonActivatedSubscription() throws Exception {
    final String stream = randStream(client);
    final String subscription = randSubscription(client, stream);
    Assertions.assertEquals(subscription, client.listSubscriptions().get(0).getSubscriptionId());
    client.deleteSubscription(subscription);
    Assertions.assertEquals(0, client.listSubscriptions().size());
    Thread.sleep(1000);
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnNonExistStreamShouldFail() throws Exception {
    String stream = randText();
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(client, stream);
        });
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnDeletedStreamShouldFail() throws Exception {
    String stream = randStream(client);
    client.deleteStream(stream);
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(client, stream);
        });
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistSubscriptionShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            client.deleteSubscription(randText());
          } catch (Throwable e) {
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }

  @Test
  @Timeout(20)
  void testDeleteRunningSubscription() throws Exception {
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
    Assertions.assertEquals(subscription, client.listSubscriptions().get(0).getSubscriptionId());
    doProduce(producer, 100, 10);
    var consumer = activateSubscription(client, subscription);

    Assertions.assertThrows(Throwable.class, () -> client.deleteSubscription(subscription));
    logger.info("confirm delete a running sub will throw an exception");
    client.deleteSubscription(subscription, true);
    logger.info("confirm delete a running sub with force option will success");
    Thread.sleep(3000);
    Assertions.assertNotNull(consumer.failureCause());
    Assertions.assertEquals(0, client.listSubscriptions().size());
    Thread.sleep(100);
  }

  @Test
  @Timeout(20)
  void testForceDeleteWaitingSubscriptionShouldNotStuck() throws Exception {
    // Waiting subscription is the subscription that has consumption up to date with the data in the
    // stream
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
    Assertions.assertEquals(subscription, client.listSubscriptions().get(0).getSubscriptionId());
    doProduce(producer, 100, 1);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res.add(r.getRawRecord());
          return false;
        });
    Assertions.assertEquals(1, res.size());
    client.deleteSubscription(subscription, true);
    Assertions.assertEquals(0, client.listSubscriptions().size());
    Thread.sleep(100);
  }

  @Test
  @Timeout(20)
  void testCreateANewSubscriptionWithTheSameNameAsTheDeletedShouldBeIndependent() throws Exception {
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
    doProduce(producer, 100, 10);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res.add(r.getRawRecord());
          return res.size() < 10;
        });
    Assertions.assertEquals(10, res.size());
    client.deleteSubscription(subscription, true);
    Assertions.assertEquals(0, client.listSubscriptions().size());

    client.createSubscription(
        io.hstream.Subscription.newBuilder().subscription(subscription).stream(stream)
            .offset(io.hstream.Subscription.SubscriptionOffset.EARLIEST)
            .build());
    List<byte[]> res2 = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res2.add(r.getRawRecord());
          return res2.size() < 10;
        });
    Assertions.assertEquals(10, res2.size());
    Thread.sleep(100);
  }
}
