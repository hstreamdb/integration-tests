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
    Thread.sleep(3000);
    Assertions.assertNotNull(consumer.failureCause());
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
                received.insert(
                    receivedRawRecord.getRecordId(),
                    Arrays.toString(receivedRawRecord.getRawRecord()));
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

    var latch = new CountDownLatch(recordCount);
    var f1 =
        consumeAsync(
            client,
            subscriptionName,
            r -> {
              latch.countDown();
              return true;
            });
    Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    // waiting for consumer to flush ACKs
    Thread.sleep(3000);
    f1.complete(null);
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

  @Test
  @Timeout(120)
  //  (on the same stream & subscription)
  //
  //  0 --- 1 --- 2 -------- 9 --- 10 --- 11 ----- 16 ------ 21 ------ 26 -- 27 ------ 31
  //  |<-       consumer_1       ->|
  //  |     |<-        consumer_2       ->|        |<-   consumer_3  ->|
  //                                                        |<-      consumer_4      ->|
  //  |           |<-               produce 10 records every half second             ->|
  //
  void testLostMessage() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 1);
    final int write_times = 50;
    final int each_count = 10;

    var received_1 = new AtomicInteger(0);
    var consumer_1 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received_1.incrementAndGet();
                  responder.ack();
                })
            .build();

    var received_2 = new AtomicInteger(0);
    var consumer_2 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received_2.incrementAndGet();
                  responder.ack();
                })
            .build();

    var received_3 = new AtomicInteger(0);
    var consumer_3 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received_3.incrementAndGet();
                  responder.ack();
                })
            .build();

    var received_4 = new AtomicInteger(0);
    var consumer_4 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received_3.incrementAndGet();
                  responder.ack();
                })
            .build();

    consumer_1.startAsync().awaitRunning();
    Thread.sleep(1000);
    consumer_2.startAsync().awaitRunning();
    Thread.sleep(1000);

    Thread thread =
        new Thread() {
          public void run() {
            try {
              for (int i = 0; i < write_times; i++) {
                doProduce(producer, 1024, each_count);
                Thread.sleep(500);
              }
            } catch (InterruptedException e) {
              System.out.println(e);
            }
          }
        };
    thread.start();

    Thread.sleep(8000);
    consumer_1.stopAsync().awaitTerminated();
    Thread.sleep(1000);
    consumer_2.stopAsync().awaitTerminated();
    Thread.sleep(5000);
    consumer_3.startAsync().awaitRunning();
    Thread.sleep(5000);
    consumer_4.startAsync().awaitRunning();
    Thread.sleep(5000);
    consumer_3.stopAsync().awaitTerminated();
    Thread.sleep(5000);
    consumer_4.stopAsync().awaitTerminated();
    producer.close();
    System.out.println(
        "=============== "
            + received_1.get()
            + " "
            + received_2.get()
            + " "
            + received_3.get()
            + " "
            + received_4.get());
    Assertions.assertEquals(
        write_times * each_count,
        received_1.get() + received_2.get() + received_3.get() + received_4.get());
  }
}
