package io.hstream.testing;

import static io.hstream.testing.Utils.ConsumerService.consume;
import static io.hstream.testing.Utils.TestUtils.*;
import static org.assertj.core.api.Assertions.*;

import io.hstream.*;
import io.hstream.testing.Utils.ConsumerService;
import io.hstream.testing.Utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class ConsumerTest {
  HStreamClient client;
  private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(20)
  void testCreateConsumerOnNonExistedSubscriptionShouldFail() {
    String subscription = "a_nonexisted_subscription_" + randText();
    assertThatThrownBy(() -> consume(client, subscription, "c1", 10, x -> false))
        .isInstanceOf(Exception.class);
    // Make sure running 'consume' twice will not block infinitely.
    // See: https://github.com/hstreamdb/hstream/pull/1086
    assertThatThrownBy(() -> consume(client, subscription, "c1", 10, x -> false))
        .isInstanceOf(Exception.class);
  }

  @Test
  @Timeout(30)
  void testCreateConsumer() {
    String stream = randStream(client);
    String sub = randSubscription(client, stream);
    assertThat(client.listSubscriptions()).hasSize(1);
    RawRecordReceiver receiver = (receivedRawRecord, responder) -> responder.ack();

    assertThatThrownBy(() -> client.newConsumer().rawRecordReceiver(receiver).build())
        .as("Create consumer without subscription name")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> client.newConsumer().subscription(sub).build())
        .as("Create consumer without receiver")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatNoException()
        .as("Create consumer success")
        .isThrownBy(() -> ConsumerService.startConsume(client, sub, "c1", x -> false));
    String sub1 = randSubscription(client, stream);
    assertThatNoException()
        .as("Create consumer with the same name on different subscriptions")
        .isThrownBy(() -> ConsumerService.startConsume(client, sub1, "c1", x -> false));

    // FIXME: check these tests
    //    assertThatThrownBy(() -> waitConsume(client, sub, "c1", x -> false).get())
    //        .as("Create consumer with existed consumer name")
    //        .hasCauseExactlyInstanceOf(Exception.class);
    //    client.deleteSubscription(sub, true);
    //    assertThatThrownBy(() -> waitConsume(client, sub, "c1", x -> false).get())
    //        .as("Create consumer on deleted subscription")
    //        .hasCauseExactlyInstanceOf(Exception.class);
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawRecord() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var record = randBytes(1024 * 4);
    String rId = producer.write(buildRecord(record)).join();
    assertThat(rId).isNotNull();

    final String subscription = randSubscription(client, streamName);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(receivedRawRecord.getRawRecord());
          return false;
        });
    assertThat(res).containsExactly(record);
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
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testServerResend() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 3);
    var producer = client.newProducer().stream(streamName).build();
    var record = randBytes(1024 * 4);
    producer.write(buildRecord(record)).join();

    var countDown = new CountDownLatch(1);
    var received = new TestUtils.RecordsPair();
    var consumer =
        ConsumerService.startConsume(
            client,
            subscriptionName,
            "c1",
            receivedRawRecord -> {
              synchronized (received) {
                received.insert(
                    receivedRawRecord.getRecordId(),
                    Arrays.toString(receivedRawRecord.getRawRecord()));
                countDown.countDown();
                return true;
              }
            },
            null,
            responder -> {});
    countDown.await();
    synchronized (received) {
      assertThat(received.ids).hasSize(1);
    }
    Thread.sleep(5000);
    consumer.stop();
    assertThat(received.ids).hasSize(2);
    assertThat(received.ids.get(0)).isEqualTo(received.ids.get(1));
    assertThat(received.records.get(0)).isEqualTo(received.records.get(1));
  }

  @Test
  @Timeout(60)
  @Tag("ack")
  void testRandomlyDropACKs() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 3);
    BufferedProducer producer = makeBufferedProducer(client, streamName);
    int recordCount = globalRandom.nextInt(100) + 50;
    produce(producer, 128, recordCount);
    producer.close();
    logger.info("wrote {} records", recordCount);

    var received = new AtomicInteger();
    var droppedRids = new ArrayList<String>();
    var retransRids = new ArrayList<String>();
    var countDown = new CountDownLatch(recordCount);
    var consumer =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .rawRecordReceiver(
                ((receivedRawRecord, responder) -> {
                  received.incrementAndGet();

                  if (countDown.getCount() == 0) {
                    // retransmitting
                    synchronized (retransRids) {
                      retransRids.add(receivedRawRecord.getRecordId());
                    }
                  } else {
                    // first round consume
                    countDown.countDown();
                  }

                  if (globalRandom.nextInt(2) == 0 && received.get() <= recordCount) {
                    synchronized (droppedRids) {
                      droppedRids.add(receivedRawRecord.getRecordId());
                    }
                  } else {
                    responder.ack();
                  }
                }))
            .build();
    consumer.startAsync().awaitRunning();

    // waiting for consumer consume all records except dropped,
    // then we can wait for retransmitting
    assertThat(countDown.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(5000);
    consumer.stopAsync().awaitTerminated();
    logger.info("dropped:{}", droppedRids.size());
    assertThat(received.get()).isEqualTo(recordCount + droppedRids.size());
    assertThat(retransRids).containsExactlyInAnyOrderElementsOf(droppedRids);
  }

  @Disabled("Very doubtful about the validity of this test, suggest moving to unit testing")
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
    var c1 =
        ConsumerService.startConsume(
            client,
            subscriptionName,
            r -> {
              latch.countDown();
              return true;
            });
    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    // waiting for consumer to flush ACKs
    Thread.sleep(3000);
    c1.stop();
    // after consuming all records, and stopping consumer, ACKs should be sent to servers,
    // so next consumer should not receive any new records except ackSender resend.
    assertThatThrownBy(() -> consume(client, subscriptionName, 6, r -> false))
        .isInstanceOf(TimeoutException.class);
  }

  @Disabled("move to unit testing")
  @Test
  @Timeout(60)
  @Tag("ack")
  void testACKsWhenStopConsumer() throws Exception {
    final String streamName = randStream(client);
    final String sub = randSubscriptionWithTimeout(client, streamName, 3);
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
    // no need sleep here, since latch.await has return, means most ACKs have been sent to server,
    // and remained acks will be sent when consumer stop
    consumer1.stopAsync().awaitTerminated();

    // waiting for server to handle ACKs
    Thread.sleep(4000);
    logger.info("received {} records", received.get());

    // after consuming some records, and stopping consumer, ACKs should be sent to servers,
    // so the count next consumer received should not greater than recordCount - c1.
    Assertions.assertThrows(
        TimeoutException.class,
        () -> consume(client, sub, "c2", 6, r -> received.incrementAndGet() < recordCount + 1));
  }

  //  @Disabled("There is no way to test the idempotency of ack from the client's point of view")
  //  @Test
  //  @Timeout(60)
  //  @Tag("ack")
  //  void testIdempotentACKs() throws Exception {
  //    final String streamName = randStream(client);
  //    final String subscriptionName = randSubscription(client, streamName);
  //    BufferedProducer producer = makeBufferedProducer(client, streamName, 32);
  //    final int count = 99;
  //    produce(producer, 1024, count);
  //    producer.close();
  //
  //    var received = new AtomicInteger();
  //    var future =
  //        consumeAsync(
  //            client,
  //            subscriptionName,
  //            "c1",
  //            receivedRawRecord -> received.incrementAndGet() < count,
  //            null,
  //            responder -> {
  //              // duplicate ACKs
  //              responder.ack();
  //              responder.ack();
  //            });
  //    future.get(20, TimeUnit.SECONDS);
  //  }

  @Disabled("remove to unit testing")
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

    assertThat(readIds).isEqualTo(writeIds);
    assertThat(readRecs).isEqualTo(writeRecs);
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
    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
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
    assertThat(output).isEqualTo(input);
  }

  @Test
  @Timeout(60)
  @Tag("resend")
  void testResendTooManyMessagesToAnotherConsumer() throws Exception {
    final String streamName = randStream(client);
    final String subscriptionName = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 10);
    final int count = 100;
    produce(producer, 10, count);
    producer.close();

    var received = new AtomicInteger(0);
    var latch = new CountDownLatch(count);
    var consumer_noack =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver((receivedRawRecord, responder) -> latch.countDown())
            .build();
    consumer_noack.startAsync().awaitRunning();
    assertThat(latch.await(6, TimeUnit.SECONDS)).isTrue();
    consumer_noack.stopAsync().awaitTerminated();

    var latch1 = new CountDownLatch(count);
    var consumer_ack =
        client
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received.incrementAndGet();
                  responder.ack();
                  latch1.countDown();
                })
            .build();
    consumer_ack.startAsync().awaitRunning();
    assertThat(latch1.await(15, TimeUnit.SECONDS)).isTrue();
    consumer_ack.stopAsync().awaitTerminated();

    Assertions.assertEquals(count, received.get());
    assertThat(count).isEqualTo(received.get());
  }
}
