package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.BufferedProducer;
import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Consumer {
  HStreamClient client;
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(20)
  void testCreateConsumerOnDeletedSubscriptionShouldFail() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscription(client, stream);
    client.deleteSubscription(subscription);
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(client, subscription, "c1", 10, x -> false));

    String subscriptionNew = randSubscription(client, stream);
    var producer = client.newProducer().stream(stream).build();
    doProduce(producer, 100, 200);
    var consumer = activateSubscription(client, subscriptionNew);
    client.deleteSubscription(subscriptionNew, true);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(client, subscriptionNew, "c1", 10, x -> false));
    Thread.sleep(100);
  }

  @Tag("efg")
  @Test
  @Timeout(60)
  void testCreateConsumerWithoutSubscriptionNameShouldFail() {
    Assertions.assertThrows(
        NullPointerException.class, () -> client.newConsumer().name("test-consumer").build());
  }

  @Disabled("HS-937")
  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameShouldFail() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    var future1 = consumeAsync(client, subscription, "c1", receivedRawRecord -> false);
    Thread.sleep(1500);
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(client, subscription, "c1", 10, x -> false));
    future1.complete(null);
  }

  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameOnDifferentSubscription() throws Exception {
    // should be okay
    final String streamName = randStream(client);
    final String subscription0 = randSubscription(client, streamName);
    final String subscription1 = randSubscription(client, streamName);
    var future1 = consumeAsync(client, subscription0, "c1", receivedRawRecord -> false);
    var future2 = consumeAsync(client, subscription1, "c1", receivedRawRecord -> false);
    Thread.sleep(1500);
    Assertions.assertFalse(future1.isCompletedExceptionally());
    Assertions.assertFalse(future2.isCompletedExceptionally());
    future1.complete(null);
    future2.complete(null);
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawRecord() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[1024 * 4];
    rand.nextBytes(record);
    String rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(client, streamName);
    List<byte[]> res = new ArrayList<>();
    var lock = new ReentrantLock();
    consume(
        client,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(receivedRawRecord.getRawRecord());
          return false;
        });
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawBatchRecord() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    var records = doProduce(producer, 1024 * 4, 2700);
    producer.close();
    final String subscription = randSubscription(client, streamName);
    List<String> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        35,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    logger.info("records size = " + records.size());
    logger.info("res size = " + res.size());
    Assertions.assertEquals(records, res);
  }

  // TODO: serviceUrl
  @Disabled("Can't connect to a specific server")
  @Test
  @Timeout(60)
  void testWriteRawThenReadFromDifferentServerUrl() throws Exception {}

  @Test
  @Timeout(60)
  @Tag("ack")
  void testServerResend() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 1);
    produce(producer, 1024, 1);
    producer.close();

    var received = new TestUtils.RecordsPair();
    var future =
        consumeAsync(
            client,
            subscriptionName,
            "c1",
            receivedRawRecord -> {
              synchronized (received) {
                received.ids.add(receivedRawRecord.getRecordId());
                received.records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
                return true;
              }
            },
            null,
            responder -> {});
    Thread.sleep(4000);
    synchronized (received) {
      Assertions.assertEquals(1, received.ids.size());
    }
    Thread.sleep(5000);
    future.complete(null);
    Assertions.assertEquals(2, received.ids.size());
    Assertions.assertEquals(received.ids.get(0), received.ids.get(1));
    Assertions.assertEquals(received.records.get(0), received.records.get(1));
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testRandomlyDropACKs() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName);
    int recordCount = globalRandom.nextInt(100) + 50;
    produce(producer, 128, recordCount);
    producer.close();
    logger.info("wrote {} records", recordCount);

    var received = new AtomicInteger();
    var dropped = new AtomicInteger();
    var future =
        consumeAsync(
            client,
            subscriptionName,
            "c1",
            r -> true,
            null,
            responder -> {
              received.incrementAndGet();
              if (globalRandom.nextInt(2) == 0 && received.get() <= recordCount) {
                dropped.incrementAndGet();
              } else {
                responder.ack();
              }
            });
    Thread.sleep(9000);
    future.complete(null);
    logger.info("dropped:{}", dropped.get());
    Assertions.assertEquals(recordCount + dropped.get(), received.get());
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testBufferedACKs() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName);
    int recordCount = 999;
    produce(producer, 128, recordCount);
    producer.close();

    var received = new AtomicInteger();
    consume(client, subscriptionName, 20, r -> received.incrementAndGet() < recordCount);
    // after consuming all records, and stopping consumer, ACKs should be sent to servers,
    // so next consumer should not receive any new records except ackSender resend.
    Assertions.assertThrows(
        TimeoutException.class, () -> consume(client, subscriptionName, 6, r -> false));
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testACKsWhenStopConsumer() throws Exception {
    final String streamName = randStream(client);
    final String sub = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName);
    int recordCount = 999;
    produce(producer, 128, recordCount);
    producer.close();
    logger.info("wrote {} records", recordCount);

    var received = new AtomicInteger();
    var latch = new CountDownLatch(1);
    int c1 = 500;
    var consumer1 =
        client
            .newConsumer()
            .subscription(sub)
            .rawRecordReceiver(
                (a, responder) -> {
                  if (received.get() < c1) {
                    received.incrementAndGet();
                    responder.ack();
                  } else {
                    latch.countDown();
                  }
                })
            .build();
    consumer1.startAsync().awaitRunning();
    Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    // sleep for consumer to send all ACKs
    Thread.sleep(3000);
    consumer1.stopAsync().awaitTerminated();

    // waiting for server to handle ACKs
    Thread.sleep(6000);
    logger.info("received {} records", received.get());

    // after consuming some records, and stopping consumer, ACKs should be sent to servers,
    // so the count next consumer received should not greater than recordCount - c1.
    Assertions.assertThrows(
        TimeoutException.class,
        () -> consume(client, sub, "c2", 6, r -> received.incrementAndGet() < recordCount + 1));
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testIdempotentACKs() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 32);
    final int count = 99;
    produce(producer, 1024, count);
    producer.close();

    var received = new AtomicInteger();
    var future =
        consumeAsync(
            client,
            subscriptionName,
            "c1",
            receivedRawRecord -> received.incrementAndGet() < count,
            null,
            responder -> {
              // duplicate ACKs
              responder.ack();
              responder.ack();
            });
    future.get(20, TimeUnit.SECONDS);
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testAutoFlushACKs() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 10);
    final int count = 10;
    produce(producer, 1024, count);
    producer.close();

    var received = new AtomicInteger(0);
    var consumer =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received.incrementAndGet();
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    Thread.sleep(9000);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(count, received.get());
  }
}
