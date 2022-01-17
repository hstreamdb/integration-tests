package io.hstream.testing;

import static io.hstream.testing.TestUtils.createConsumer;
import static io.hstream.testing.TestUtils.createConsumerCollectStringPayload;
import static io.hstream.testing.TestUtils.createConsumerWithFixNumsRecords;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.doProduceAndGatherRid;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionFromEarliest;
import static io.hstream.testing.TestUtils.randSubscriptionWithOffset;
import static io.hstream.testing.TestUtils.randSubscriptionWithTimeout;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.HStreamDBClientException;
import io.hstream.Producer;
import io.hstream.ReceivedRawRecord;
import io.hstream.RecordId;
import io.hstream.Responder;
import io.hstream.Stream;
import io.hstream.Subscription;
import io.hstream.SubscriptionOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private String logMsgPathPrefix;
  private ExtensionContext context;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setLogMsgPathPrefix(String logMsgPathPrefix) {
    this.logMsgPathPrefix = logMsgPathPrefix;
  }

  public void setExtensionContext(ExtensionContext context) {
    this.context = context;
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("[DEBUG]: hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  void produce(Producer p, int tid) throws InterruptedException {
    Random rand = new Random();
    byte[] rRec = new byte[128];
    for (int i = 0; i < 1000; i++) {
      System.out.println("Thread " + tid + " write");
      rand.nextBytes(rRec);
      p.write(rRec).join();
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  @Timeout(20)
  void testConnections() throws Exception {

    for (var hServerUrl : hServerUrls) {
      System.out.println(hServerUrl);
      try (HStreamClient client = HStreamClient.builder().serviceUrl(hServerUrl).build()) {
        List<Stream> res = client.listStreams();
        Assertions.assertTrue(res.isEmpty());
      }
    }
  }

  @Test
  @Timeout(60)
  void testCreateStream() {
    final String streamName = randText();
    hStreamClient.createStream(streamName);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  @Timeout(60)
  void testDeleteStream() {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    hStreamClient.deleteStream(streamName);
    streams = hStreamClient.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  @Timeout(60)
  void testDeleteNonExistingStreamShouldFail() {
    Assertions.assertThrows(Exception.class, () -> hStreamClient.deleteStream("aaa"));
  }

  @Test
  @Timeout(60)
  void testListStreams() {
    Assertions.assertTrue(hStreamClient.listStreams().isEmpty());
    var streamNames = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      streamNames.add(randStream(hStreamClient));
    }
    var res =
        hStreamClient.listStreams().parallelStream()
            .map(Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(streamNames.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testListSubscriptions() {
    String streamName = randStream(hStreamClient);
    var subscriptions = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      subscriptions.add(randSubscription(hStreamClient, streamName));
    }
    var res =
        hStreamClient.listSubscriptions().parallelStream()
            .map(Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(subscriptions.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testCreateConsumerWithoutSubscriptionNameShouldFail() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> hStreamClient.newConsumer().name("test-consumer").build());
  }

  @Disabled("enable after HS-805 fix")
  @Test
  @Timeout(60)
  void testDeleteNonExistingSubscriptionShouldFail() {
    Assertions.assertThrows(Exception.class, () -> hStreamClient.deleteSubscription("aaa"));
  }

  @Test
  @Timeout(60)
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    RecordId rId = producer.write(record).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<byte[]> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((receivedRawRecord, responder) -> {
                  lock.lock();
                  res.add(receivedRawRecord.getRawRecord());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testWriteRawOutOfPayloadLimitShouldFailed() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[max];
    rand.nextBytes(record);
    Assertions.assertThrows(Exception.class, () -> producer.write(record).join());
  }

  @Test
  @Timeout(60)
  void testWriteMixPayload() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    var rawRecords = new ArrayList<String>();
    var hRecords = new ArrayList<HRecord>();
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
    List<HRecord> hRes = new ArrayList<>();
    List<String> rawRes = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((hRecord, responder) -> {
                  hRes.add(hRecord.getHRecord());
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
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    var hRecordInput =
        hRecords.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var hOutputRecord = hRes.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(hRecordInput, hOutputRecord);
    Assertions.assertEquals(rawRecords, rawRes);
  }

  @Test
  @Timeout(60)
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
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((receivedHRecord, responder) -> {
                  lock.lock();
                  res.add(receivedHRecord.getHRecord());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
  }

  @Test
  @Timeout(60)
  void testWriteRawBatch() throws Exception {
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
  }

  @Disabled("enable after HS-786 fix")
  @Test
  @Timeout(60)
  void testBatchSizeZero() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(0).build();
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
  }

  @Test
  @Timeout(60)
  void testNoBatchWriteInForLoopShouldNotStuck() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    var records = doProduce(producer, 128, 100);

    CountDownLatch notify = new CountDownLatch(records.size());
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
    Assertions.assertTrue(done, "consumer timeout");
    Assertions.assertEquals(
        records.stream().sorted().collect(Collectors.toList()),
        res.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  @Timeout(60)
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    var futures = new CompletableFuture[100];
    var records = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      HRecord hRec =
          HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
      futures[i] = producer.write(hRec);
      records.add(hRec);
    }
    CompletableFuture.allOf(futures).join();

    CountDownLatch notify = new CountDownLatch(futures.length);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<HRecord> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .hRecordReceiver(
                ((hRecord, responder) -> {
                  lock.lock();
                  res.add(hRecord.getHRecord());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    var input = records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatchMultiThread() throws Exception {
    Producer producer =
        hStreamClient.newProducer().stream(randStream(hStreamClient))
            .enableBatch()
            .recordCountLimit(10)
            .build();
    Random rand = new Random();
    final int cnt = 100;
    var futures = new CompletableFuture[100];

    Thread t0 =
        new Thread(
            () -> {
              for (int i = 0; i < cnt / 2; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                futures[i] = producer.write(rRec);
              }
            });

    Thread t1 =
        new Thread(
            () -> {
              for (int i = cnt / 2; i < cnt; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                futures[i] = producer.write(rRec);
              }
            });

    t0.start();
    t1.start();
    t0.join();
    t1.join();
    for (int i = 0; i < cnt; i++) {
      Assertions.assertNotNull(futures[i]);
    }
  }

  @Test
  @Timeout(60)
  void testMixWriteBatchAndNoBatchRecords() throws Exception {
    final String streamName = randStream(hStreamClient);
    int totalWrites = 10;
    int batchWrites = 0;
    int batchSize = 5;
    Producer batchProducer =
        hStreamClient.newProducer().stream(streamName)
            .enableBatch()
            .recordCountLimit(batchSize)
            .build();
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    var records = new ArrayList<String>();
    var recordIds = new ArrayList<RecordId>();
    var lock = new ReentrantLock();

    for (int i = 0; i < totalWrites; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        batchWrites++;
        System.out.printf("[turn]: %d, batch write!!!!!\n", i);
        var writes = new ArrayList<CompletableFuture<RecordId>>(5);
        for (int j = 0; j < batchSize; j++) {
          var rRec = new byte[] {(byte) i};
          records.add(Arrays.toString(rRec));
          writes.add(batchProducer.write(rRec));
        }
        writes.forEach(w -> recordIds.add(w.join()));
      } else {
        System.out.printf("[turn]: %d, no batch write!!!!!\n", i);
        var rRec = new byte[] {(byte) i};
        records.add(Arrays.toString(rRec));
        recordIds.add(producer.write(rRec).join());
      }
    }

    CountDownLatch notify =
        new CountDownLatch(batchWrites * batchSize + (totalWrites - batchWrites));
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    List<RecordId> receivedRecordIds = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  res.add(Arrays.toString(rawRecord.getRawRecord()));
                  receivedRecordIds.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    System.out.printf(
        "wait join !!!!! batch writes = %d, single writes = %d\n",
        batchWrites, totalWrites - batchWrites);
    System.out.println("send rid: ");
    Assertions.assertEquals(recordIds.size(), records.size());
    for (int i = 0; i < recordIds.size(); i++) {
      System.out.println(recordIds.get(i) + ": " + records.get(i));
    }
    System.out.println("received rid");
    for (int i = 0; i < receivedRecordIds.size(); i++) {
      System.out.println(receivedRecordIds.get(i) + ": " + res.get(i));
    }
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void createConsumerWithExistedConsumerNameShouldThrowException() throws InterruptedException {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(((receivedRawRecord, responder) -> responder.ack()))
            .build();
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(((receivedRawRecord, responder) -> responder.ack()))
            .build();
    consumer.startAsync().awaitRunning();
    Thread.sleep(1500);
    consumer1.startAsync().awaitRunning();
    Thread.sleep(1500);
    Assertions.assertNotNull(consumer1.failureCause());
    Assertions.assertTrue(consumer1.failureCause() instanceof HStreamDBClientException);
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Timeout(60)
  void testACK() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    var rids = doProduceAndGatherRid(producer, 10, 2500);
    CountDownLatch notify = new CountDownLatch(rids.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    Set<RecordId> res = new HashSet<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  if (rand.nextInt(10) % 2 == 0) {
                    lock.lock();
                    res.add(rawRecord.getRecordId());
                    lock.unlock();
                    responder.ack();
                  }
                  notify.countDown();
                }))
            .build();

    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);

    CountDownLatch notify1 = new CountDownLatch(rids.size() - res.size());
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer-1")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  boolean success = res.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  if (success) {
                    notify1.countDown();
                  }
                }))
            .build();

    consumer1.startAsync().awaitRunning();
    done = notify1.await(20, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    // This asserts may fail because we can not guarantee that consumer1
    // will stop after all ACKs are sent successfully, so there might be
    // some retrans happen. Also, we are now support at-least-once
    // consume, so we can ignore these duplicated retrans for now.
    // Assertions.assertTrue(Collections.disjoint(res, reTrans));
    Assertions.assertEquals(
        rids.stream().sorted().collect(Collectors.toList()),
        res.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[1024 * 4];
    rand.nextBytes(record);
    RecordId rId = producer.write(record).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<byte[]> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((receivedRawRecord, responder) -> {
                  lock.lock();
                  res.add(receivedRawRecord.getRawRecord());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawBatchRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    var records = doProduce(producer, 1024 * 4, 2700);
    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(35, TimeUnit.SECONDS);
    System.out.println("records size = " + records.size());
    System.out.println("res size = " + res.size());
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done, "consumer time out");
    Assertions.assertEquals(records, res);
  }

  @Disabled("enable after HS-809 fix.")
  @Test
  @Timeout(60)
  void testSubscribeInMiddleOfBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(2).build();
    var rids = new ArrayList<RecordId>();
    Random rand = new Random();
    byte[] rRec = new byte[2];
    var writes = new ArrayList<CompletableFuture<RecordId>>();
    var records = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      rand.nextBytes(rRec);
      records.add(Arrays.toString(rRec));
      writes.add(producer.write(rRec));
    }
    writes.forEach(w -> w.thenAccept(rids::add));

    for (int i = 0; i < 10; i++) {
      Assertions.assertNotNull(writes.get(i));
    }

    int randomIndex;
    while (true) {
      randomIndex = Math.max(rand.nextInt(rids.size()), 2);
      if (rids.get(randomIndex).getBatchIndex() == 1) {
        break;
      }
    }
    final String subscriptionInMiddle =
        randSubscriptionWithOffset(
            hStreamClient, streamName, new SubscriptionOffset(rids.get(randomIndex)));

    CountDownLatch notify = new CountDownLatch(records.size() - randomIndex);
    List<String> res = new ArrayList<>();
    List<RecordId> rec = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscriptionInMiddle)
            .name("test-consumer3")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  res.add(Arrays.toString(rawRecord.getRawRecord()));
                  rec.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();

    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    System.out.println(rids.get(randomIndex));
    System.out.println(rids);
    System.out.println(rec);
    Assertions.assertEquals((int) records.stream().skip(randomIndex).count(), res.size());
    Assertions.assertEquals(records.stream().skip(randomIndex).collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testSubscribeBeforeOrAfterProducedOffset() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(10).build();
    long minLSN = Integer.MAX_VALUE;
    int minBatchIndex = Integer.MAX_VALUE;
    long maxLSN = Integer.MIN_VALUE;
    int maxBatchIndex = Integer.MIN_VALUE;
    var rids = new ArrayList<RecordId>();
    Random rand = new Random();
    byte[] rRec = new byte[128];
    var writes = new ArrayList<CompletableFuture<RecordId>>();
    var records = new ArrayList<String>();
    for (int i = 0; i < 200; i++) {
      rand.nextBytes(rRec);
      records.add(Arrays.toString(rRec));
      writes.add(producer.write(rRec));
    }
    writes.forEach(w -> w.thenAccept(rids::add));
    for (RecordId rid : rids) {
      minLSN = Math.min(minLSN, rid.getBatchId());
      maxLSN = Math.max(maxLSN, rid.getBatchId());
      minBatchIndex = Math.min(minBatchIndex, rid.getBatchIndex());
      maxBatchIndex = Math.max(maxBatchIndex, rid.getBatchIndex());
    }

    for (int i = 0; i < 200; i++) {
      Assertions.assertNotNull(writes.get(i));
    }

    final String subscriptionBeforeMinLSN =
        randSubscriptionWithOffset(
            hStreamClient, streamName, new SubscriptionOffset(new RecordId(minLSN - 10, 0)));
    final String subscriptionAfterMaxLSN =
        randSubscriptionWithOffset(
            hStreamClient, streamName, new SubscriptionOffset(new RecordId(maxLSN + 10, 0)));

    CountDownLatch notify1 = new CountDownLatch(records.size());
    List<String> res1 = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer1 =
        createConsumerCollectStringPayload(
            hStreamClient, subscriptionBeforeMinLSN, "test-consumer1", res1, notify1, lock);
    List<RecordId> res2 = new ArrayList<>();
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscriptionAfterMaxLSN)
            .name("test-consumer2")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  res2.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                }))
            .build();

    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();

    var done = notify1.await(10, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res1);

    Thread.sleep(1000);
    Assertions.assertTrue(res2.isEmpty());
    List<RecordId> records2 = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      rand.nextBytes(rRec);
      writes.add(producer.write(rRec));
    }
    writes.forEach(w -> w.thenAccept(records2::add));
    Thread.sleep(3000);
    consumer2.stopAsync().awaitTerminated();
    final RecordId newRecordId = new RecordId(maxLSN + 10, 0);
    var expectation =
        records2.stream()
            .dropWhile(rid -> rid.compareTo(newRecordId) < 0)
            .collect(Collectors.toList());
    Assertions.assertEquals(expectation.size(), res2.size());
    Assertions.assertEquals(expectation, res2);
  }

  @Test
  @Timeout(60)
  void testRedundancyAndUnorderedAck() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(10).build();
    Random rand = new Random();
    var records = doProduce(producer, 128, 1000);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);

    CountDownLatch notify = new CountDownLatch(records.size());
    List<String> res1 = new ArrayList<>();
    var responders = new LinkedList<Responder>();
    var missCnt = new AtomicInteger(50);
    var lock = new ReentrantLock();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  if (rand.nextInt(10) % 2 != 0) {
                    lock.lock();
                    responders.push(responder);
                    lock.unlock();
                    var tmp = missCnt.getAcquire();
                    if (tmp > 0) {
                      missCnt.setRelease(tmp - 1);
                    } else {
                      responder.ack();
                    }
                  } else {
                    responder.ack();
                  }
                  lock.lock();
                  res1.add(Arrays.toString(rawRecord.getRawRecord()));
                  lock.unlock();
                  notify.countDown();
                }))
            .build();

    List<String> res2 = new ArrayList<>();
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer2")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  res2.add(Arrays.toString(rawRecord.getRawRecord()));
                  responder.ack();
                  if (rand.nextInt(10) % 2 != 0
                      && !responders.isEmpty()
                      && missCnt.getAcquire() <= 0) {
                    lock.lock();
                    Collections.shuffle(responders);
                    var respd = responders.poll();
                    lock.unlock();
                    respd.ack();
                  }
                  notify.countDown();
                }))
            .build();

    consumer.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    var done = notify.await(20, TimeUnit.SECONDS);
    while (!responders.isEmpty()) {
      var respd = responders.poll();
      respd.ack();
    }
    consumer.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    System.out.printf(
        "records.size = %d, res.size = %d\n", records.size(), res1.size() + res2.size());
    Assertions.assertEquals(records.size(), res1.size() + res2.size());
    res1.addAll(res2);
    Assertions.assertEquals(
        records.stream().sorted().collect(Collectors.toList()),
        res1.stream().sorted().collect(Collectors.toList()));
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  @Timeout(60)
  void testConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    final int count = 3500;
    var records = doProduce(producer, 100, count);

    CountDownLatch signal = new CountDownLatch(count);
    List<ReceivedRawRecord> res1 = new ArrayList<>();
    List<ReceivedRawRecord> res2 = new ArrayList<>();
    List<ReceivedRawRecord> res3 = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer1 =
        createConsumer(hStreamClient, subscription, "consumer-1", res1, signal, lock);
    Consumer consumer2 =
        createConsumer(hStreamClient, subscription, "consumer-2", res2, signal, lock);
    Consumer consumer3 =
        createConsumer(hStreamClient, subscription, "consumer-3", res3, signal, lock);
    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    consumer3.startAsync().awaitRunning();

    var done = signal.await(10, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);

    Assertions.assertEquals(count, res1.size() + res2.size() + res3.size());
    java.util.stream.Stream.of(res1, res2, res3).forEach(TestUtils::assertRecordIdsAscending);
    var res =
        java.util.stream.Stream.of(res1, res2, res3)
            .flatMap(Collection::stream)
            .sorted(Comparator.comparing(ReceivedRawRecord::getRecordId))
            .map(r -> Arrays.toString(r.getRawRecord()))
            .collect(Collectors.toList());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 2500;
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);

    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    List<RecordId> records = doProduceAndGatherRid(producer, 1, 2500);
    Random random = new Random();
    final int maxReceivedCountC1 = Math.max(1, random.nextInt(recordCount / 3));
    CountDownLatch latch1 = new CountDownLatch(1);
    var res1 = new HashSet<RecordId>();
    var lock = new ReentrantLock();
    var consumer1 =
        createConsumerWithFixNumsRecords(
            hStreamClient, maxReceivedCountC1, subscription, "consumer1", res1, latch1, lock);

    final int maxReceivedCountC2 = Math.max(1, random.nextInt(recordCount / 3));
    CountDownLatch latch2 = new CountDownLatch(1);
    var res2 = new HashSet<RecordId>();
    var consumer2 =
        createConsumerWithFixNumsRecords(
            hStreamClient, maxReceivedCountC2, subscription, "consumer2", res2, latch2, lock);

    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    var done1 = latch1.await(10, TimeUnit.SECONDS);
    var done2 = latch2.await(10, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    System.out.println("remove consumer1 and consumer2...");
    Assertions.assertTrue(done1);
    Assertions.assertTrue(done2);
    Thread.sleep(1000); // leave some time to server to complete ack

    var consumedRecordIds =
        java.util.stream.Stream.of(res1, res2)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    CountDownLatch latch3 = new CountDownLatch(recordCount - consumedRecordIds.size());
    var consumer3 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer3")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  lock.lock();
                  var success = consumedRecordIds.add(receivedRawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  if (success) {
                    latch3.countDown();
                  }
                })
            .build();

    consumer3.startAsync().awaitRunning();
    var done3 = latch3.await(10, TimeUnit.SECONDS);
    Thread.sleep(1000); // leave some time to server to complete ack
    consumer3.stopAsync().awaitTerminated();
    Assertions.assertTrue(done3);
    var res = consumedRecordIds.stream().sorted().collect(Collectors.toList());
    Assertions.assertEquals(
        records.size(),
        res.size(),
        "records.size = " + records.size() + ", res.size = " + res.size());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testAddConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    final int count = 5000;
    List<String> records = doProduce(producer, 100, count);
    CountDownLatch signal = new CountDownLatch(count);
    List<ReceivedRawRecord> res1 = new ArrayList<>();
    List<ReceivedRawRecord> res2 = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer1 =
        createConsumer(hStreamClient, subscription, "consumer-1", res1, signal, lock);
    Consumer consumer2 =
        createConsumer(hStreamClient, subscription, "consumer-2", res2, signal, lock);
    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();

    Thread.sleep(1000);

    List<ReceivedRawRecord> res3 = new ArrayList<>();
    Consumer consumer3 =
        createConsumer(hStreamClient, subscription, "consumer-3", res3, signal, lock);
    consumer3.startAsync().awaitRunning();

    boolean done = signal.await(10, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);

    Assertions.assertEquals(count, res1.size() + res2.size() + res3.size());
    java.util.stream.Stream.of(res1, res2, res3).forEach(TestUtils::assertRecordIdsAscending);
    var res =
        java.util.stream.Stream.of(res1, res2, res3)
            .flatMap(Collection::stream)
            .sorted(Comparator.comparing(ReceivedRawRecord::getRecordId))
            .map(r -> Arrays.toString(r.getRawRecord()))
            .collect(Collectors.toList());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testReduceConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    final int count = 5000;
    List<String> records = doProduce(producer, 100, count);
    CountDownLatch signal = new CountDownLatch(count);
    List<ReceivedRawRecord> res1 = new ArrayList<>();
    List<ReceivedRawRecord> res2 = new ArrayList<>();
    List<ReceivedRawRecord> res3 = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer1 =
        createConsumer(hStreamClient, subscription, "consumer-1", res1, signal, lock);
    Consumer consumer2 =
        createConsumer(hStreamClient, subscription, "consumer-2", res2, signal, lock);
    Consumer consumer3 =
        createConsumer(hStreamClient, subscription, "consumer-3", res3, signal, lock);
    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    consumer3.startAsync().awaitRunning();

    while (signal.getCount() > count / 2) {
      Thread.sleep(100);
    }
    consumer2.stopAsync().awaitTerminated();

    while (signal.getCount() > count / 3) {
      Thread.sleep(100);
    }
    consumer3.stopAsync().awaitTerminated();

    boolean done = signal.await(20, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);

    Assertions.assertEquals(count, res1.size() + res2.size() + res3.size());
    var res =
        java.util.stream.Stream.of(res1, res2, res3)
            .flatMap(Collection::stream)
            .sorted(Comparator.comparing(ReceivedRawRecord::getRecordId))
            .map(r -> Arrays.toString(r.getRawRecord()))
            .collect(Collectors.toList());
    Assertions.assertEquals(records, res);
  }

  @Timeout(60)
  @Test
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionWithTimeout(hStreamClient, streamName, 1);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    final int count = 20000;
    Random rand = new Random();
    CountDownLatch signal = new CountDownLatch(count);
    int consumerNameSuffix = 1;
    var lock = new ReentrantLock();
    List<RecordId> records = doProduceAndGatherRid(producer, 100, count);
    var receivedRecords = new ArrayList<ArrayList<ReceivedRawRecord>>();
    var consumers = new ArrayList<Consumer>();
    for (int i = 0; i < 3; i++) {
      receivedRecords.add(new ArrayList<>());
      consumers.add(
          createConsumer(
              hStreamClient,
              subscription,
              "consumer-" + consumerNameSuffix,
              receivedRecords.get(0),
              signal,
              lock));
      consumerNameSuffix++;
    }
    consumers.forEach(c -> c.startAsync().awaitRunning());

    int cnt = 8;
    int lastIdx = -1;
    int alive = consumers.size();
    while (signal.getCount() != 0 && cnt > 0) {
      Thread.sleep(2000);
      int idx = rand.nextInt(consumers.size());
      if (idx != lastIdx) {
        System.out.println("turn: " + (8 - cnt));
        if (consumers.get(idx).isRunning()) {
          consumers.get(idx).stopAsync().awaitTerminated();
          System.out.println("==================== stop consumer: " + (idx + 1));
          alive--;
          if (alive == 0) {
            System.out.println("no consumer alive!");
          }
        } else {
          var newConsumer =
              createConsumer(
                  hStreamClient,
                  subscription,
                  "consumer-" + consumerNameSuffix,
                  receivedRecords.get(idx),
                  signal,
                  lock);
          newConsumer.startAsync().awaitRunning();
          consumerNameSuffix++;
          consumers.set(idx, newConsumer);
          System.out.println("==================== start consumer: " + (idx + 1));
          alive++;
        }
        lastIdx = idx;
        cnt--;
      }
      System.out.println("countDownLatch.count = " + signal.getCount());
    }
    System.out.println("Dynamic adjustment done. consumer stats: ");
    for (int i = 0; i < consumers.size(); i++) {
      String state;
      if (consumers.get(i).isRunning()) {
        state = "Running";
      } else {
        state = "Stop";
      }
      System.out.printf("Conumer %d: %s\n", i, state);
    }

    if (signal.getCount() != 0) {
      for (int i = 0; i < consumers.size(); i++) {
        if (!consumers.get(i).isRunning()) {
          var newConsumer =
              createConsumer(
                  hStreamClient,
                  subscription,
                  "consumer-" + consumerNameSuffix,
                  receivedRecords.get(i),
                  signal,
                  lock);
          newConsumer.startAsync().awaitRunning();
          consumerNameSuffix++;
          consumers.set(i, newConsumer);
          System.out.println("==================== start consumer: " + (i + 1));
        }
      }
    }

    boolean done = signal.await(20, TimeUnit.SECONDS);
    System.out.println(signal.getCount());
    Assertions.assertTrue(
        done,
        "timeout, total received: "
            + receivedRecords.stream().map(ArrayList::size).reduce(0, Integer::sum));

    Assertions.assertEquals(
        count, receivedRecords.stream().map(ArrayList::size).reduce(0, Integer::sum));
    var res =
        receivedRecords.stream()
            .flatMap(Collection::stream)
            .map(ReceivedRawRecord::getRecordId)
            .sorted()
            .distinct()
            .collect(Collectors.toList());
    Assertions.assertEquals(records, res);

    for (int i = 0; i < consumers.size(); i++) {
      consumers.get(i).stopAsync().awaitTerminated();
    }
  }
}
