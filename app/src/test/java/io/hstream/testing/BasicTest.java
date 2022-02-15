package io.hstream.testing;

import static io.hstream.testing.TestUtils.buildRecord;
import static io.hstream.testing.TestUtils.createConsumer;
import static io.hstream.testing.TestUtils.createConsumerCollectStringPayload;
import static io.hstream.testing.TestUtils.createConsumerWithFixNumsRecords;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.doProduceAndGatherRid;
import static io.hstream.testing.TestUtils.randRawRec;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionWithTimeout;
import static io.hstream.testing.TestUtils.randText;
import static java.util.stream.Stream.of;

import io.hstream.BufferedProducer;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicTest {

  private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);
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
    logger.debug("hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  @Timeout(20)
  void testConnections() throws Exception {
    for (var hServerUrl : hServerUrls) {
      logger.info("hServerUrl is " + hServerUrl);
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
  @Timeout(20)
  void testDeleteSubscription() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
    hStreamClient.deleteSubscription(subscription);
    Assertions.assertEquals(0, hStreamClient.listSubscriptions().size());
    Thread.sleep(1000);
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnNonExistStreamShouldFail() throws Exception {
    String stream = randText();
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(hStreamClient, stream);
        });
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnDeletedStreamShouldFail() throws Exception {
    String stream = randStream(hStreamClient);
    hStreamClient.deleteStream(stream);
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(hStreamClient, stream);
        });
  }

  @Disabled("FIXME")
  @Test
  @Timeout(20)
  void testCreateConsumerOnDeletedSubscriptionShouldFail() throws Exception {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hStreamClient.deleteSubscription(subscription);

    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  logger.debug("get id = {}", recs.getRecordId());
                  recv.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    Thread.sleep(5000);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertNotNull(consumer.failureCause());
  }

  @Test
  @Timeout(60)
  void testCreateConsumerWithoutSubscriptionNameShouldFail() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> hStreamClient.newConsumer().name("test-consumer").build());
  }

  @Test
  @Timeout(60)
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    RecordId rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscription(hStreamClient, streamName);
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
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testWriteRawOutOfPayloadLimitShouldFail() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[max];
    rand.nextBytes(record);
    Assertions.assertThrows(Exception.class, () -> producer.write(buildRecord(record)).join());
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
    RecordId rId = producer.write(buildRecord(hRec)).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscription(hStreamClient, streamName);
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
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
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
        producer.write(buildRecord(record)).join();
        rawRecords.add(Arrays.toString(record));
      } else {
        HRecord hRec =
            HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
        producer.write(buildRecord(hRec)).join();
        hRecords.add(hRec);
      }
    }

    CountDownLatch notify = new CountDownLatch(rawRecords.size() + hRecords.size());
    final String subscription = randSubscription(hStreamClient, streamName);
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
    var done = notify.await(20, TimeUnit.SECONDS);
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
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(100).build();
    var records = doProduce(producer, 128, 100);
    producer.close();

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(30)
  void testBatchSizeZero() throws Exception {
    // recordCountLimit is not allowed to be less than 1
    final String streamName = randStream(hStreamClient);
    Assertions.assertThrows(
        HStreamDBClientException.class,
        () -> {
          Producer producer =
              hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(0).build();
        });
  }

  @Test
  @Timeout(60)
  void testNoBatchWriteInForLoopShouldNotStuck() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    var records = doProduce(producer, 128, 100);

    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscription(hStreamClient, streamName);
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
    Assertions.assertTrue(
        notify.await(10, TimeUnit.SECONDS),
        "consumer timeout, get size = " + res.size() + " but should be " + records.size());
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(
        records.stream().sorted().collect(Collectors.toList()),
        res.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  @Timeout(60)
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(100).build();
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

    CountDownLatch notify = new CountDownLatch(futures.length);
    final String subscription = randSubscription(hStreamClient, streamName);
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
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    var input = records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatchMultiThread() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .recordCountLimit(10)
            .flushIntervalMs(10)
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
                futures[i] = producer.write(buildRecord(rRec));
              }
            });

    Thread t1 =
        new Thread(
            () -> {
              for (int i = cnt / 2; i < cnt; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                futures[i] = producer.write(buildRecord(rRec));
              }
            });

    t0.start();
    t1.start();
    t0.join();
    t1.join();
    producer.close();
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
    BufferedProducer batchProducer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(batchSize).build();
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    var records = new ArrayList<String>();
    var recordIds = new ArrayList<RecordId>();
    var lock = new ReentrantLock();

    for (int i = 0; i < totalWrites; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        batchWrites++;
        logger.info("[turn]: {}, batch write!!!!!\n", i);
        var writes = new ArrayList<CompletableFuture<RecordId>>(5);
        for (int j = 0; j < batchSize; j++) {
          var rRec = new byte[] {(byte) i};
          records.add(Arrays.toString(rRec));
          writes.add(batchProducer.write(buildRecord(rRec)));
        }
        writes.forEach(w -> recordIds.add(w.join()));
      } else {
        logger.info("[turn]: {}, no batch write!!!!!\n", i);
        var rRec = new byte[] {(byte) i};
        records.add(Arrays.toString(rRec));
        recordIds.add(producer.write(buildRecord(rRec)).join());
      }
    }

    batchProducer.close();

    CountDownLatch notify =
        new CountDownLatch(batchWrites * batchSize + (totalWrites - batchWrites));
    final String subscription = randSubscription(hStreamClient, streamName);
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
    logger.info(
        "wait join !!!!! batch writes = {}, single writes = {}\n",
        batchWrites,
        totalWrites - batchWrites);
    logger.info("send rid: ");
    Assertions.assertEquals(recordIds.size(), records.size());
    for (int i = 0; i < recordIds.size(); i++) {
      logger.info(recordIds.get(i) + ": " + records.get(i));
    }
    logger.info("received rid");
    for (int i = 0; i < receivedRecordIds.size(); i++) {
      logger.info(receivedRecordIds.get(i) + ": " + res.get(i));
    }
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordAndClose() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .recordCountLimit(100)
            .flushIntervalMs(-1)
            .build();
    Random random = new Random();
    final int count = 10;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.write(buildRecord(rawRecord));
      recordIdFutures[i] = future;
    }
    // flush and close producer
    producer.close();
    CompletableFuture.allOf(recordIdFutures).join();
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordBasedTimer() throws Exception {
    try (BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .recordCountLimit(100)
            .flushIntervalMs(100)
            .build()) {
      Random random = new Random();
      final int count = 10;
      CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
      for (int i = 0; i < count; ++i) {
        byte[] rawRecord = new byte[100];
        random.nextBytes(rawRecord);
        CompletableFuture<RecordId> future = producer.write(buildRecord(rawRecord));
        recordIdFutures[i] = future;
      }
      CompletableFuture.allOf(recordIdFutures).join();
    }
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordBasedBytesSize() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .recordCountLimit(100)
            .flushIntervalMs(-1)
            .maxBytesSize(4096)
            .build();
    Random random = new Random();
    final int count = 42;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<RecordId> future = producer.write(buildRecord(rawRecord));
      recordIdFutures[i] = future;
    }
    for (int i = 0; i < count - 1; ++i) {
      recordIdFutures[i].join();
    }

    Assertions.assertThrows(
        TimeoutException.class, () -> recordIdFutures[41].get(3, TimeUnit.SECONDS));
    producer.close();
    recordIdFutures[41].join();
  }

  @Disabled("HS-937")
  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameShouldFail() throws InterruptedException {
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

  @Disabled("HS-937")
  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameOnDifferentSubscriptionShouldNotFail()
      throws InterruptedException {
    final String streamName = randStream(hStreamClient);
    final String subscription0 = randSubscription(hStreamClient, streamName);
    final String subscription1 = randSubscription(hStreamClient, streamName);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription0)
            .name("test-consumer")
            .rawRecordReceiver(((receivedRawRecord, responder) -> responder.ack()))
            .build();
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription1)
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
    // FIXME: multiple key/consumer version

    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscription(hStreamClient, streamName);
    final int msgCnt = 2048;
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(32).build();
    Set<RecordId> recordIds0 = new HashSet<>(doProduceAndGatherRid(producer, 1, msgCnt));
    producer.close();
    Assertions.assertEquals(msgCnt, recordIds0.size());
    CountDownLatch countDown = new CountDownLatch(msgCnt);
    Set<RecordId> recordIds1 = new HashSet<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .name("newConsumer")
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    recv.ack();
                    countDown.countDown();
                  }
                })
            .subscription(subscriptionName)
            .build();
    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDown.await(20, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[1024 * 4];
    rand.nextBytes(record);
    RecordId rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    CountDownLatch notify = new CountDownLatch(1);
    final String subscription = randSubscription(hStreamClient, streamName);
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
    var done = notify.await(20, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawBatchRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(100).build();
    var records = doProduce(producer, 1024 * 4, 2700);
    producer.close();
    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    var lock = new ReentrantLock();
    Consumer consumer =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer", res, notify, lock);
    consumer.startAsync().awaitRunning();
    var done = notify.await(35, TimeUnit.SECONDS);
    logger.info("records size = " + records.size());
    logger.info("res size = " + res.size());
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done, "consumer time out");
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testRedundancyAndUnorderedAck() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(10).build();
    Random rand = new Random();
    var records = doProduce(producer, 128, 1000);
    producer.close();
    final String subscription = randSubscription(hStreamClient, streamName);

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
    logger.info("records.size = {}, res.size = {}\n", records.size(), res1.size() + res2.size());
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
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(50).build();
    final int count = 3500;
    var records = doProduce(producer, 100, count);
    producer.close();

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

    var done = signal.await(20, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);

    Assertions.assertEquals(count, res1.size() + res2.size() + res3.size());
    Assertions.assertEquals(1, of(res1, res2, res3).filter(x -> !x.isEmpty()).count());
    of(res1, res2, res3).forEach(TestUtils::assertRecordIdsAscending);
    var res =
        java.util.stream.Stream.of(res1, res2, res3)
            .flatMap(Collection::stream)
            .sorted(Comparator.comparing(ReceivedRawRecord::getRecordId))
            .map(r -> Arrays.toString(r.getRawRecord()))
            .collect(Collectors.toList());
    Assertions.assertEquals(records, res);
  }

  @Disabled("consumer-key issue")
  @Test
  @Timeout(60)
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 2500;
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);

    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(50).build();
    List<RecordId> records = doProduceAndGatherRid(producer, 1, 2500);
    producer.close();
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
    var done1 = latch1.await(20, TimeUnit.SECONDS);
    var done2 = latch2.await(20, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    logger.info("remove consumer1 and consumer2...");
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
    var done3 = latch3.await(20, TimeUnit.SECONDS);
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

  @Disabled("consumer-key issue")
  @Test
  @Timeout(60)
  void testAddConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(50).build();
    final int count = 5000;
    List<String> records = doProduce(producer, 100, count);
    producer.close();
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

    boolean done = signal.await(20, TimeUnit.SECONDS);
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

  @Disabled("consumer-key issue")
  @Test
  @Timeout(60)
  void testReduceConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(50).build();
    final int count = 5000;
    List<String> records = doProduce(producer, 100, count);
    producer.close();
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

  @Disabled("consumer-key issue")
  @Timeout(60)
  @Test
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionWithTimeout(hStreamClient, streamName, 1);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(streamName).recordCountLimit(50).build();
    final int count = 20000;
    Random rand = new Random();
    CountDownLatch signal = new CountDownLatch(count);
    int consumerNameSuffix = 1;
    var lock = new ReentrantLock();
    List<RecordId> records = doProduceAndGatherRid(producer, 100, count);
    producer.close();
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
        logger.info("turn: " + (8 - cnt));
        if (consumers.get(idx).isRunning()) {
          consumers.get(idx).stopAsync().awaitTerminated();
          logger.info("==================== stop consumer: " + (idx + 1));
          alive--;
          if (alive == 0) {
            logger.info("no consumer alive!");
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
          logger.info("==================== start consumer: " + (idx + 1));
          alive++;
        }
        lastIdx = idx;
        cnt--;
      }
      logger.info("countDownLatch.count = " + signal.getCount());
    }
    logger.info("Dynamic adjustment done. consumer stats: ");
    for (int i = 0; i < consumers.size(); i++) {
      String state;
      if (consumers.get(i).isRunning()) {
        state = "Running";
      } else {
        state = "Stop";
      }
      logger.info("Consumer {}: {}\n", i, state);
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
          logger.info("==================== start consumer: " + (i + 1));
        }
      }
    }

    boolean done = signal.await(20, TimeUnit.SECONDS);
    logger.info("signal count = " + signal.getCount());
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

    for (Consumer consumer : consumers) {
      consumer.stopAsync().awaitTerminated();
    }
  }

  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() throws Exception {
    String stream = randStream(hStreamClient);

    Producer producer = hStreamClient.newProducer().stream(stream).build();

    RecordId id0 = producer.write(randRawRec()).join();
    RecordId id1 = producer.write(randRawRec()).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);

    hStreamClient.deleteStream(stream);
    Assertions.assertThrows(Exception.class, () -> producer.write(randRawRec()).join());
  }

  @Test
  @Timeout(60)
  void testConsumeFromToDeletedSubscriptionShouldFail() throws Exception {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    Set<RecordId> recordIds0 = new HashSet<>();
    Set<RecordId> recordIds1 = new HashSet<>();
    Producer producer = hStreamClient.newProducer().stream(stream).build();

    int msgCnt = 2000;
    for (int i = 0; i < msgCnt; ++i) {
      recordIds0.add(producer.write(randRawRec()).join());
    }
    hStreamClient.deleteSubscription(subscription);

    CountDownLatch countDown = new CountDownLatch(msgCnt);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    recv.ack();
                  }
                })
            .build();
    consumer.startAsync().awaitRunning();
    countDown.await(30, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertNotEquals(recordIds0, recordIds1);
  }

  @Test
  @Timeout(60)
  void testMultiThreadListStream() throws Exception {
    randStream(hStreamClient);

    ExecutorService executor = Executors.newCachedThreadPool();
    for (String hServerUrl : hServerUrls) {
      executor.execute(
          () -> {
            HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();
            Assertions.assertNotNull(c.listStreams());
          });
    }
  }

  @Test
  @Timeout(60)
  void testMultiThreadCreateSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randText();

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.createStream(stream);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  @Test
  @Timeout(60)
  void testCreateThenDeleteStreamFromDifferentServerUrl() throws Exception {
    ArrayList<HStreamClient> clients = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      clients.add(HStreamClient.builder().serviceUrl(hServerUrl).build());
    }
    String stream = randStream(clients.get(0));
    clients.get(1).deleteStream(stream);
    for (int i = 2; i < clients.size(); i++) {
      int finalI = i;
      Assertions.assertThrows(Exception.class, () -> clients.get(finalI).deleteStream(stream));
    }
  }

  @Disabled("unstable")
  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randStream(hStreamClient);

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.deleteStream(stream);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  @Test
  @Timeout(60)
  void testWriteRawThenReadFromDifferentServerUrl() throws Exception {
    Random rand = new Random();
    byte[] randRecs = new byte[128];
    rand.nextBytes(randRecs);
    final int total = 64;

    HStreamClient hStreamClient1 = HStreamClient.builder().serviceUrl(hServerUrls.get(1)).build();
    String stream = randStream(hStreamClient1);
    hStreamClient1.close();

    Set<RecordId> recordIds0 = new HashSet<>();
    String subscription = randSubscription(hStreamClient, stream);
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    for (int i = 0; i < total; ++i) {
      recordIds0.add(producer.write(buildRecord(randRecs)).join());
    }

    Set<RecordId> recordIds1 = new HashSet<>();
    CountDownLatch countDown = new CountDownLatch(total);
    HStreamClient hStreamClient2 = HStreamClient.builder().serviceUrl(hServerUrls.get(2)).build();
    Consumer consumer =
        hStreamClient2
            .newConsumer()
            .name("test-newConsumer-" + UUID.randomUUID())
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, receiver) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    countDown.countDown();
                  }
                  logger.debug("read, size = {}", recordIds1.size());
                  receiver.ack();
                })
            .build();

    Assertions.assertEquals(total, recordIds0.size());
    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDown.await(20, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(recordIds0, recordIds1);
  }

  @Test
  @Timeout(60)
  void testOrdKey() throws Exception {
    final int msgCnt = 128;
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    Set<RecordId> recordIds = new HashSet<>();
    for (int i = 0; i < msgCnt; ++i) {
      Assertions.assertTrue(recordIds.add(producer.write(randRawRec()).join()));
    }

    AtomicReference<Integer> which = new AtomicReference<>(null);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Assertions.assertEquals(msgCnt, recordIds.size());
    CountDownLatch countDownLatch0 = new CountDownLatch(msgCnt);
    Set<RecordId> recordIds0 = new HashSet<>();
    Consumer consumer0 =
        hStreamClient
            .newConsumer()
            .name("consumer0")
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds0.add(recs.getRecordId())) {
                    countDownLatch0.countDown();
                    if (countDownLatch0.getCount() == 0) {
                      countDownLatch.countDown();
                      which.set(0);
                    }
                  }
                  recv.ack();
                })
            .build();
    Set<RecordId> recordIds1 = new HashSet<>();
    CountDownLatch countDownLatch1 = new CountDownLatch(msgCnt);
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .name("consumer1")
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  if (recordIds1.add(recs.getRecordId())) {
                    countDownLatch1.countDown();
                    if (countDownLatch1.getCount() == 0) {
                      countDownLatch.countDown();
                      which.set(1);
                    }
                    recv.ack();
                  }
                })
            .build();

    consumer0.startAsync().awaitRunning();
    consumer1.startAsync().awaitRunning();
    Assertions.assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    consumer0.stopAsync().awaitTerminated();
    consumer0.stopAsync().awaitTerminated();

    logger.debug("size = {}, {}", recordIds0.size(), recordIds1.size());
    if (which.get().equals(0)) {
      Assertions.assertEquals(msgCnt, recordIds0.size());
      Assertions.assertEquals(0, recordIds1.size());
    } else {
      Assertions.assertEquals(0, recordIds0.size());
      Assertions.assertEquals(msgCnt, recordIds1.size());
    }
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistSubscriptionShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            hStreamClient.deleteSubscription(randText());
          } catch (Throwable e) {
            e.printStackTrace();
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistStreamShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            hStreamClient.deleteStream(randText());
          } catch (Throwable e) {
            e.printStackTrace();
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }
}
