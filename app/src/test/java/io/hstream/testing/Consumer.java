package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.BufferedProducer;
import io.hstream.CompressionType;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
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
public class Consumer {
  HStreamClient client;
  private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(20)
  void testCreateConsumerOnNonExistedSubscriptionShouldFail() throws Exception {
    String subscription = "a_nonexisted_subscription_" + randText();
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(client, subscription, "c1", 10, x -> false));
    // Make sure running 'consume' twice will not block infinitely.
    // See: https://github.com/hstreamdb/hstream/pull/1086
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(client, subscription, "c1", 10, x -> false));
  }

  @Test
  @Timeout(20)
  void testResumeConsumptionLatest() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscription(client, stream, Subscription.SubscriptionOffset.LATEST);
    var producer = client.newProducer().stream(stream).build();
    doProduce(producer, 100, 200);
    var consumer = activateSubscription(client, subscription);
    try {
      // sleep 5s for consuming records
      consumer.awaitTerminated(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // stop consumer
      consumer.stopAsync().awaitTerminated();
    }
    doProduce(producer, 100, 200);
    var consumer2 = activateSubscription(client, subscription);
    try {
      // sleep 5s for consuming records
      consumer2.awaitTerminated(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // stop consumer
      consumer2.stopAsync().awaitTerminated();
    }
  }

  @Test
  @Timeout(20)
  void testResumeConsumptionEarliest() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscription(client, stream);
    var producer = client.newProducer().stream(stream).build();
    doProduce(producer, 100, 200);
    var consumer = activateSubscription(client, subscription);
    try {
      // sleep 5s for consuming records
      consumer.awaitTerminated(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // stop consumer
      consumer.stopAsync().awaitTerminated();
    }
    doProduce(producer, 100, 200);
    var consumer2 = activateSubscription(client, subscription);
    try {
      // sleep 5s for consuming records
      consumer2.awaitTerminated(5, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // stop consumer
      consumer2.stopAsync().awaitTerminated();
    }
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
  //  |           |<-        produce 10 records every half second          ->|
  //
  void testLostMessage() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 1);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 1);
    final int write_times = 50;
    final int each_count = 10;
    var writeValue = new AtomicInteger(0);

    TestUtils.RecordsPair pair = new TestUtils.RecordsPair();

    var ids_1 = new ArrayList<String>(1000);
    var recs_1 = new ArrayList<String>(1000);
    var consumer_1 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  ids_1.add(receivedRawRecord.getRecordId());
                  ByteBuffer wrapped = ByteBuffer.wrap(receivedRawRecord.getRawRecord());
                  int thisNum = wrapped.getInt();
                  recs_1.add(String.valueOf(thisNum));
                  responder.ack();
                })
            .build();

    var ids_2 = new ArrayList<String>(1000);
    var recs_2 = new ArrayList<String>(1000);
    var consumer_2 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  ids_2.add(receivedRawRecord.getRecordId());
                  ByteBuffer wrapped = ByteBuffer.wrap(receivedRawRecord.getRawRecord());
                  int thisNum = wrapped.getInt();
                  recs_2.add(String.valueOf(thisNum));
                  responder.ack();
                })
            .build();

    var ids_3 = new ArrayList<String>(1000);
    var recs_3 = new ArrayList<String>(1000);
    var consumer_3 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  ids_3.add(receivedRawRecord.getRecordId());
                  ByteBuffer wrapped = ByteBuffer.wrap(receivedRawRecord.getRawRecord());
                  int thisNum = wrapped.getInt();
                  recs_3.add(String.valueOf(thisNum));
                  responder.ack();
                })
            .build();

    var ids_4 = new ArrayList<String>(1000);
    var recs_4 = new ArrayList<String>(1000);
    var consumer_4 =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  ids_4.add(receivedRawRecord.getRecordId());
                  ByteBuffer wrapped = ByteBuffer.wrap(receivedRawRecord.getRawRecord());
                  int thisNum = wrapped.getInt();
                  recs_4.add(String.valueOf(thisNum));
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
                TestUtils.RecordsPair thisPair = produce(producer, writeValue, each_count);
                pair.extend(thisPair);
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

    var readIds = new HashSet<String>();
    var readRecs = new HashSet<String>();
    readIds.addAll(ids_1);
    readIds.addAll(ids_2);
    readIds.addAll(ids_3);
    readIds.addAll(ids_4);

    readRecs.addAll(recs_1);
    readRecs.addAll(recs_2);
    readRecs.addAll(recs_3);
    readRecs.addAll(recs_4);

    var writeIds = new HashSet<String>(pair.ids);
    var writeRecs = new HashSet<String>(pair.records);

    System.out.println("============== Write ===============");
    System.out.println("len=" + writeRecs.size() + ": " + writeRecs);
    System.out.println("==============  Read ===============");
    System.out.println("len=" + (recs_1.size() + recs_2.size() + recs_3.size() + recs_4.size()));
    System.out.println("len=" + recs_1.size() + ", Consumer 1: " + recs_1);
    System.out.println("len=" + recs_2.size() + ", Consumer 2: " + recs_2);
    System.out.println("len=" + recs_3.size() + ", Consumer 3: " + recs_3);
    System.out.println("len=" + recs_4.size() + ", Consumer 4: " + recs_4);

    Assertions.assertEquals(writeIds, readIds);
    Assertions.assertEquals(writeRecs, readRecs);
  }

  @Test
  @Timeout(60)
  void testResendJSONBatchWithCompression() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 10, CompressionType.GZIP);
    Random rand = new Random();
    var futures = new CompletableFuture[100];
    var records = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      HRecord hRec =
          HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
      futures[i] = producer.write(buildRecord(hRec));
      records.add(hRec);
    }
    CompletableFuture.allOf(futures).join();
    producer.close();

    final String subscription = randSubscriptionWithTimeout(client, streamName, 5);
    List<HRecord> res = new ArrayList<>();
    var received = new AtomicInteger();
    var latch = new CountDownLatch(1);
    var consumer1 =
        client
            .newConsumer()
            .subscription(subscription)
            .hRecordReceiver(
                (a, responder) -> {
                  if (received.get() < 100) {
                    if (globalRandom.nextInt(4) != 0) {
                      res.add(a.getHRecord());
                      responder.ack();
                    }
                    received.incrementAndGet();
                  } else {
                    latch.countDown();
                  }
                })
            .build();
    consumer1.startAsync().awaitRunning();
    Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    consumer1.stopAsync().awaitTerminated();

    consume(
        client,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return res.size() < records.size();
        });
    var input =
        records.parallelStream().map(HRecord::toString).sorted().collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).sorted().collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }
}
