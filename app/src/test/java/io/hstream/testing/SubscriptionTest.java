package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;
import static io.hstream.testing.Utils.ConsumerService.activateSubscription;
import static io.hstream.testing.Utils.ConsumerService.consume;
import static org.assertj.core.api.Assertions.*;

import io.hstream.HServerException;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class SubscriptionTest {
  private static final Logger logger = LoggerFactory.getLogger(SubscriptionTest.class);
  private HStreamClient client;

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(60)
  void testSubBasicOperation() {
    String streamName = randStream(client);
    var sub1 =
        Subscription.newBuilder().subscription("test_subscription_" + randText()).stream(streamName)
            .build();
    var sub2 =
        Subscription.newBuilder().subscription("test_subscription_" + randText()).stream(streamName)
            .ackTimeoutSeconds(10)
            .build();
    var sub3 =
        Subscription.newBuilder().subscription("test_subscription_" + randText()).stream(streamName)
            .ackTimeoutSeconds(10)
            .maxUnackedRecords(100)
            .build();
    var sub4 =
        Subscription.newBuilder().subscription("test_subscription_" + randText()).stream(streamName)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .ackTimeoutSeconds(10)
            .maxUnackedRecords(100)
            .build();
    var sub5 =
        Subscription.newBuilder().subscription("test_subscription_" + randText()).stream(streamName)
            .offset(Subscription.SubscriptionOffset.LATEST)
            .ackTimeoutSeconds(10)
            .maxUnackedRecords(100)
            .build();
    var subs = Arrays.asList(sub1, sub2, sub3, sub4, sub5);
    for (var sub : subs) {
      assertThatNoException().isThrownBy(() -> client.createSubscription(sub));
    }
    var subscriptions =
        subs.stream().map(Subscription::getSubscriptionId).collect(Collectors.toList());

    // check list subscriptions
    var listSubs =
        client.listSubscriptions().stream()
            .map(io.hstream.Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    assertThat(listSubs)
        .containsExactlyElementsOf(subscriptions.stream().sorted().collect(Collectors.toList()));

    // check get subscriptions
    subscriptions.forEach(
        sub -> {
          var s = client.getSubscription(sub);
          assertThat(s.getSubscription().getSubscriptionId()).isEqualTo(sub);
          assertThat(s.getSubscription().getStreamName()).isEqualTo(streamName);
          assertThat(s.getOffsets()).isNotNull();
        });

    // check delete subscriptions
    for (int i = 0; i < subs.size(); i++) {
      int finalI = i;
      if (i % 2 == 0) {
        assertThatNoException()
            .isThrownBy(() -> client.deleteSubscription(subscriptions.get(finalI)));
      } else {
        assertThatNoException()
            .isThrownBy(() -> client.deleteSubscription(subscriptions.get(finalI), true));
      }
    }
    assertThat(client.listSubscriptions()).isEmpty();
  }

  @Test
  @Timeout(60)
  void testInvalidSubscriptionOperationShouldFail() {
    assertThatThrownBy(() -> randSubscription(client, randText()))
        .as("create sub with non-exist stream")
        .isInstanceOf(HServerException.class);
    String stream = randStream(client);
    var sub = randSubscription(client, stream);
    assertThatThrownBy(
            () ->
                client.createSubscription(
                    Subscription.newBuilder().stream(stream).subscription(sub).build()))
        .as("create sub with duplicate name")
        .isInstanceOf(HServerException.class);
    client.deleteSubscription(sub);
    client.deleteStream(stream, true);
    assertThatThrownBy(() -> randSubscription(client, stream))
        .as("create sub with deleted stream")
        .isInstanceOf(HServerException.class);
    assertThatThrownBy(() -> client.deleteSubscription(randText()))
        .as("delete non-exist sub")
        .isInstanceOf(Exception.class);
  }

  @Test
  @Timeout(20)
  void testDeleteRunningSubscription() throws Exception {
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
    assertThat(client.listSubscriptions().get(0).getSubscriptionId()).isEqualTo(subscription);
    doProduce(producer, 100, 10);
    activateSubscription(client, subscription);

    assertThatThrownBy(() -> client.deleteSubscription(subscription))
        .as("delete a running sub will throw an exception")
        .isInstanceOf(Exception.class);
    assertThatNoException()
        .as("confirm delete a running sub with force option will success")
        .isThrownBy(() -> client.deleteSubscription(subscription, true));
    assertThat(client.listSubscriptions()).isEmpty();
    //    Thread.sleep(100);
  }

  @Test
  @Timeout(20)
  void testForceDeleteWaitingSubscriptionShouldNotStuck() throws Exception {
    // Waiting subscription is the subscription that has consumption up to date with the data in the
    // stream
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
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
    assertThat(res).hasSize(1);
    assertThatNoException().isThrownBy(() -> client.deleteSubscription(subscription, true));
    assertThat(client.listSubscriptions()).isEmpty();
    //    Thread.sleep(100);
  }

  @Test
  @Timeout(30)
  void testCreateANewSubscriptionWithTheSameNameAsTheDeletedShouldBeIndependent() throws Exception {
    final String stream = randStream(client);
    var producer = client.newProducer().stream(stream).build();
    final String subscription = randSubscription(client, stream);
    var rids = doProduceAndGatherRid(producer, 100, 10);
    List<String> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res.add(r.getRecordId());
          return res.size() < 10;
        });
    assertThat(res).hasSize(10);
    assertThat(res).containsExactlyInAnyOrderElementsOf(rids);
    client.deleteSubscription(subscription, true);
    assertThat(client.listSubscriptions()).isEmpty();

    client.createSubscription(
        Subscription.newBuilder().subscription(subscription).stream(stream)
            .offset(io.hstream.Subscription.SubscriptionOffset.EARLIEST)
            .build());
    List<String> res2 = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res2.add(r.getRecordId());
          return res2.size() < 10;
        });
    assertThat(res2).hasSize(10);
    assertThat(res2).containsExactlyInAnyOrderElementsOf(rids);
    //    Thread.sleep(100);
  }

  @Test
  @Timeout(10)
  void testListConsumers() throws InterruptedException {
    var stream = randStream(client);
    var sub = randSubscription(client, stream);
    assertThat(client.listConsumers(sub)).as("List empty consumers").isEmpty();
    var consumer =
        client
            .newConsumer()
            .subscription(sub)
            .rawRecordReceiver((receivedRawRecord, responder) -> {})
            .build();
    consumer.startAsync().awaitRunning();
    Thread.sleep(1000);
    assertThat(client.listConsumers(sub)).as("List consumers").hasSize(1);
  }
}
