package io.hstream.testing;

import static io.hstream.testing.TestUtils.createConsumerCollectStringPayload;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscriptionFromEarliest;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import io.hstream.Stream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(BasicExtension.class)
class BasicProducerTest {

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

  @Test
  void testWriteJSON() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    Producer producer = hStreamClient.newProducer().stream(streamName).build();
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
    boolean done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
  }

  @Test
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
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
    boolean done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Disabled
  void testWriteMixPayload() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    ArrayList<String> rawRecords = new ArrayList<String>();
    ArrayList<HRecord> hRecords = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      if (rand.nextInt() % 2 == 0) {
        rand.nextBytes(record);
        producer.write(record).join();
        rawRecords.add(Arrays.toString(record));
      } else {
        HRecord hRec =
            HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
        producer.write(hRec).join();
        hRecords.add(hRec);
      }
    }

    CountDownLatch notify = new CountDownLatch(rawRecords.size() + hRecords.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<HRecord> hres = new ArrayList<>();
    List<String> rawRes = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((hRecord, responder) -> {
                  hres.add(hRecord.getHRecord());
                  responder.ack();
                  notify.countDown();
                }))
            .rawRecordReceiver(
                ((receivedRawRecord, responder) -> {
                  rawRes.add(Arrays.toString(receivedRawRecord.getRawRecord()));
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    boolean done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    List<String> hRecordInput =
        hRecords.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    List<String> hOutputRecord =
        hres.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(hRecordInput, hOutputRecord);
    Assertions.assertEquals(rawRecords, rawRes);
  }

  @Test
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    var xs = new CompletableFuture[100];
    ArrayList<HRecord> records = new ArrayList<HRecord>();
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
    boolean done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    List<String> input =
        records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    List<String> output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }

  @Test
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    ArrayList<String> records = doProduce(producer, 128, 100);

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify);
    consumer.startAsync().awaitRunning();
    boolean done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }

  @Disabled
  @Test
  void testWriteBatchAndNoBatchRecords() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer batchProducer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(5).build();
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    byte[] rRec = new byte[128];
    var records = new ArrayList<String>();
    //      var xs = new CompletableFuture[100];
    var xs = new ArrayList<CompletableFuture<RecordId>>(1000);
    int cnt = 0;
    for (int i = 0; i < 10; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        cnt++;
        System.out.printf("[turn]: %d, batch write!!!!!\n", i);
        for (int j = 0; j < 5; j++) {
          rand.nextBytes(rRec);
          records.add(Arrays.toString(rRec));
          xs.add(batchProducer.write(rRec));
        }
      } else {
        System.out.printf("[turn]: %d, no batch write!!!!!\n", i);
        rand.nextBytes(rRec);
        records.add(Arrays.toString(rRec));
        xs.add(producer.write(rRec));
      }
    }
    System.out.printf("wait join !!!!! batch writes = %d, xs.size() = %d\n", cnt, xs.size());
    xs.forEach(CompletableFuture::join);

    for (CompletableFuture<RecordId> x : xs) {
      Assertions.assertNotNull(x);
    }

    CountDownLatch notify = new CountDownLatch(xs.size());
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
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
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

  @Test
  void testWriteRawOutOfPayloadLimit() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[max];
    rand.nextBytes(record);
    Assertions.assertThrows(Exception.class, () -> producer.write(record).join());
  }

  @Disabled
  @Test
  void testWriteRawRecordWithLoop() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
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
    notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(records, res);
  }

  @Disabled
  @Test
  void testBatchSizeZero() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(0).build();
    var records = doProduce(producer, 128, 100);

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify);
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }
}
