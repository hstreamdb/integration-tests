package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(BasicExtension.class)
class BasicTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("db url: " + hStreamDBUrl);
    // Thread.sleep(1000000);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  void testCreateStream() throws Exception {
    final String streamName = randText();
    hStreamClient.createStream(streamName);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  void testDeleteStream() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    hStreamClient.deleteStream(streamName);
    streams = hStreamClient.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    RecordId rId = producer.write(record).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<byte[]> res = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((receivedRawRecord, responder) -> {
                  res.add(receivedRawRecord.getRawRecord());
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    notify.await();
    consumer.stopAsync().awaitTerminated();
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  void testWriteJSON() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = hStreamClient.newProducer().stream(streamName).build();
    HRecord hRec = HRecord.newBuilder().put("x", "y").put("acc", 0).put("init", false).build();
    RecordId rId = producer.write(hRec).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<HRecord> res = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((receivedHRecord, responder) -> {
                  res.add(receivedHRecord.getHRecord());
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    notify.await();
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
  }

  @Test
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    byte[] rRec = new byte[128];
    var records = new ArrayList<String>();
    var xs = new CompletableFuture[100];
    for (int i = 0; i < 100; i++) {
      rand.nextBytes(rRec);
      records.add(Arrays.toString(rRec));
      xs[i] = producer.write(rRec);
    }
    CompletableFuture.allOf(xs).join();
    for (int i = 0; i < 100; i++) {
      Assertions.assertNotNull(xs[i]);
    }

    CountDownLatch notify = new CountDownLatch(xs.length);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  res.add(Arrays.toString(rawRecord.getRawRecord()));
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    notify.await();
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(records, res);
  }

  @Test
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    var xs = new CompletableFuture[100];
    var records = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      HRecord hRec =
          HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
      xs[i] = producer.write(hRec);
      records.add(hRec);
    }
    CompletableFuture.allOf(xs).join();
    for (int i = 0; i < 100; i++) {
      Assertions.assertNotNull(xs[i]);
    }

    CountDownLatch notify = new CountDownLatch(xs.length);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<HRecord> res = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((hRecord, responder) -> {
                  res.add(hRecord.getHRecord());
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    notify.await();
    consumer.stopAsync().awaitTerminated();
    var input = records.parallelStream().map(HRecord::toString).toList();
    var output = res.parallelStream().map(HRecord::toString).toList();
    Assertions.assertEquals(input, output);
  }

  @Test
  void testWriteRawBatchMultiThread() throws Exception {
    Producer producer =
        hStreamClient.newProducer().stream(randStream(hStreamClient))
            .enableBatch()
            .recordCountLimit(10)
            .build();
    Random rand = new Random();
    final int cnt = 100;
    var xs = new CompletableFuture[100];

    Thread t0 =
        new Thread(
            () -> {
              for (int i = 0; i < cnt / 2; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                xs[i] = producer.write(rRec);
              }
            });

    Thread t1 =
        new Thread(
            () -> {
              for (int i = cnt / 2; i < cnt; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                xs[i] = producer.write(rRec);
              }
            });

    t0.start();
    t1.start();
    t0.join();
    t1.join();
    for (int i = 0; i < cnt; i++) {
      Assertions.assertNotNull(xs[i]);
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  void testConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    List<String> records = new ArrayList<>();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    final int count = 1000;
    for (int i = 0; i < count; ++i) {
      random.nextBytes(rawRecord);
      records.add(Arrays.toString(rawRecord));
      producer.write(rawRecord).join();
    }

    CountDownLatch signal = new CountDownLatch(count);
    List<ReceivedRawRecord> res1 = new ArrayList<>();
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  responder.ack();
                  res1.add(receivedRawRecord);
                  signal.countDown();
                })
            .build();

    List<ReceivedRawRecord> res2 = new ArrayList<>();
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  responder.ack();
                  res2.add(receivedRawRecord);
                  signal.countDown();
                })
            .build();

    List<ReceivedRawRecord> res3 = new ArrayList<>();
    Consumer consumer3 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-3")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  responder.ack();
                  res3.add(receivedRawRecord);
                  signal.countDown();
                })
            .build();

    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    consumer3.startAsync().awaitRunning();

    signal.await();

    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();

    Assertions.assertEquals(count, res1.size() + res2.size() + res3.size());
    java.util.stream.Stream.of(res1, res2, res3).forEach(TestUtils::assertRecordIdsAscending);
    var res =
        java.util.stream.Stream.of(res1, res2, res3)
            .flatMap(Collection::stream)
            .sorted(ReceivedRawRecordComparator())
            .map(r -> Arrays.toString(r.getRawRecord()))
            .toList();
    Assertions.assertEquals(records, res);
  }

  @Test
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 10;
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);

    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    List<String> records = new ArrayList<>();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    for (int i = 0; i < recordCount; ++i) {
      random.nextBytes(rawRecord);
      producer.write(rawRecord).join();
      records.add(Arrays.toString(rawRecord));
    }

    final int maxReceivedCountC1 = Math.max(1, random.nextInt(recordCount / 2));
    CountDownLatch latch1 = new CountDownLatch(1);
    AtomicInteger c1ReceivedRecordCount = new AtomicInteger(0);
    var res1 = new ArrayList<ReceivedRawRecord>();
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c1ReceivedRecordCount.get() < maxReceivedCountC1) {
                    responder.ack();
                    res1.add(receivedRawRecord);
                    if (c1ReceivedRecordCount.incrementAndGet() == maxReceivedCountC1) {
                      latch1.countDown();
                    }
                  }
                })
            .build();
    consumer1.startAsync().awaitRunning();
    latch1.await();
    consumer1.stopAsync().awaitTerminated();

    final int maxReceivedCountC2 = recordCount - maxReceivedCountC1;
    CountDownLatch latch2 = new CountDownLatch(1);
    AtomicInteger c2ReceivedRecordCount = new AtomicInteger(0);
    var res2 = new ArrayList<ReceivedRawRecord>();
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c2ReceivedRecordCount.get() < maxReceivedCountC2) {
                    responder.ack();
                    res2.add(receivedRawRecord);
                    if (c2ReceivedRecordCount.incrementAndGet() == maxReceivedCountC2) {
                      latch2.countDown();
                    }
                  }
                })
            .build();
    consumer2.startAsync().awaitRunning();
    latch2.await();
    consumer2.stopAsync().awaitTerminated();

    Assertions.assertEquals(recordCount, c1ReceivedRecordCount.get() + c2ReceivedRecordCount.get());
    var res =
        java.util.stream.Stream.of(res1, res2)
            .flatMap(Collection::stream)
            .sorted(ReceivedRawRecordComparator())
            .map(r -> Arrays.toString(r.getRawRecord()))
            .toList();
    Assertions.assertEquals(records, res);
  }
}
