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
import static io.hstream.testing.TestUtils.restartServer;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(BasicExtension.class)
class BasicTest {

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
    // Thread.sleep(1000000);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

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

  @Disabled("restart may cause test fail, disable")
  @Test
  @Timeout(60)
  void testGetResourceAfterRestartServer() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    restartServer(server);
    var streams = hStreamClient.listStreams();
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    var subscriptions = hStreamClient.listSubscriptions();
    Assertions.assertEquals(subscription, subscriptions.get(0).getSubscriptionId());
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

  @Test
  @Timeout(60)
  void testMultiProducer() throws Exception {
    var threads = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      int finalI = i;
      threads.add(
          new Thread(
              () -> {
                final String streamName = randStream(hStreamClient);
                Producer producer = hStreamClient.newProducer().stream(streamName).build();
                try {
                  produce(producer, finalI);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }));
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }

  @Disabled("restart may cause test fail, disable")
  @Test
  @Timeout(60)
  void testReconsumeAfterRestartServer() throws Exception {
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

    restartServer(server);
    res.clear();
    CountDownLatch notify2 = new CountDownLatch(records.size());

    final String subscription1 = randSubscriptionFromEarliest(hStreamClient, streamName);
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription1, "test-consumer", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(10, TimeUnit.SECONDS);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records, res);
  }

  @Disabled("enable after HS-806 fix.")
  @Test
  @Timeout(60)
  void testConsumeAfterRestartServer() throws Exception {
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

    restartServer(server);
    res.clear();

    Producer producer2 =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(10).build();
    records = doProduce(producer2, 1, 10);
    CountDownLatch notify2 = new CountDownLatch(records.size());
    Consumer consumer2 =
        createConsumerCollectStringPayload(
            hStreamClient, subscription, "test-consumer-new", res, notify2, lock);
    consumer2.startAsync().awaitRunning();
    done = notify2.await(10, TimeUnit.SECONDS);
    Thread.sleep(1000);
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
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
  void testWriteRawOutOfPayloadLimit() {
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
    var done = notify.await(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    var hRecordInput =
        hRecords.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var hOutputRecord = hres.parallelStream().map(HRecord::toString).collect(Collectors.toList());
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

  @Disabled("enable after HS-783 fix")
  @Test
  @Timeout(60)
  void testWriteBatchAndNoBatchRecords() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer batchProducer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(5).build();
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    byte[] rRec = new byte[128];
    var records = new ArrayList<String>();
    var rids = new ArrayList<RecordId>();
    var xs = new ArrayList<CompletableFuture<RecordId>>(1000);
    int cnt = 0;
    var txs = new ArrayList<CompletableFuture<RecordId>>(5);
    var lock = new ReentrantLock();
    for (int i = 0; i < 5; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        cnt++;
        System.out.printf("[turn]: %d, batch write!!!!!\n", i);
        for (int j = 0; j < 5; j++) {
          //          rand.nextBytes(rRec);
          rRec = new byte[] {(byte) i};
          records.add(Arrays.toString(rRec));
          xs.add(batchProducer.write(rRec));
          //          batchProducer.write(rRec).join();
          txs.add(batchProducer.write(rRec));
        }
        //        txs.forEach(CompletableFuture::join);
        txs.forEach(
            w ->
                w.thenAccept(
                    rid -> {
                      lock.lock();
                      rids.add(rid);
                      lock.unlock();
                    }));
        txs.clear();
      } else {
        System.out.printf("[turn]: %d, no batch write!!!!!\n", i);
        //        rand.nextBytes(rRec);
        rRec = new byte[] {(byte) i};
        records.add(Arrays.toString(rRec));
        xs.add(producer.write(rRec));
        rids.add(producer.write(rRec).join());
      }
    }
    System.out.printf("wait join !!!!! batch writes = %d, xs.size() = %d\n", cnt, xs.size());
    //    xs.forEach(CompletableFuture::join);

    //    for (CompletableFuture<RecordId> x : xs) {
    //      Assertions.assertNotNull(x);
    //    }

    CountDownLatch notify = new CountDownLatch(xs.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    List<RecordId> rrid = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  res.add(Arrays.toString(rawRecord.getRawRecord()));
                  rrid.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  notify.countDown();
                }))
            .build();
    consumer.startAsync().awaitRunning();
    var done = notify.await(10, TimeUnit.SECONDS);
    Thread.sleep(5000);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    System.out.println("send rid: ");
    for (int i = 0; i < rids.size(); i++) {
      System.out.println(rids.get(i) + ": " + records.get(i));
    }
    //      System.out.println(rids.stream().sorted().collect(Collectors.toList()));
    System.out.println("received rid");
    for (int i = 0; i < rrid.size(); i++) {
      System.out.println(rrid.get(i) + ": " + res.get(i));
    }
    System.out.println(server.getLogs());
    //      System.out.println(rrid);
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }

  @Disabled("enable after client support dump failure.")
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
    Assertions.assertThrows(
        Exception.class,
        () -> {
          var exp = consumer1.failureCause().toString();
          System.err.println("exp: " + exp);
          throw consumer1.failureCause().getCause();
        });

    Assertions.fail("create consumer with existed consumer name should fail.");
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
    List<RecordId> res = new ArrayList<>();
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

    List<RecordId> reTrans = new ArrayList<>();
    CountDownLatch notify1 = new CountDownLatch(rids.size() - res.size());
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("test-consumer-1")
            .rawRecordReceiver(
                ((rawRecord, responder) -> {
                  lock.lock();
                  reTrans.add(rawRecord.getRecordId());
                  lock.unlock();
                  responder.ack();
                  notify1.countDown();
                }))
            .build();

    consumer1.startAsync().awaitRunning();
    done = notify1.await(10, TimeUnit.SECONDS);
    consumer1.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    // This assert may fail because we can not guarantee that consumer1
    // will stop after all acks are send successfully, so there might be
    // some retrans happen. Also, we are now support at-least-once
    // consume, so we can ignore these duplicated retrans for now.
    // Assertions.assertTrue(Collections.disjoint(res, reTrans));
    res.addAll(reTrans);
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
  void testSubscribeInMiddle() throws Exception {
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
    Assertions.assertEquals(
        records.stream().skip(randomIndex).collect(Collectors.toList()).size(), res.size());
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
    CountDownLatch notify = new CountDownLatch(records.size());
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    List<String> res1 = new ArrayList<>();
    AtomicInteger i = new AtomicInteger();
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
                    i.getAndIncrement();
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
      i.getAndIncrement();
    }
    consumer.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    Assertions.assertTrue(done);
    System.out.printf("===========resend: %d\n", i.get());
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
    List<String> records = doProduce(producer, 100, 2500);
    Random random = new Random();
    final int maxReceivedCountC1 = Math.max(1, random.nextInt(recordCount / 3));
    CountDownLatch latch1 = new CountDownLatch(1);
    var res1 = new ArrayList<ReceivedRawRecord>();
    var lock = new ReentrantLock();
    var consumer1 =
        createConsumerWithFixNumsRecords(
            hStreamClient, maxReceivedCountC1, subscription, "consumer1", res1, latch1, lock);

    final int maxReceivedCountC2 = Math.max(1, random.nextInt(recordCount / 3));
    CountDownLatch latch2 = new CountDownLatch(1);
    var res2 = new ArrayList<ReceivedRawRecord>();
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

    Thread.sleep(3000);

    final int maxReceivedCountC3 = recordCount - maxReceivedCountC1 - maxReceivedCountC2;
    CountDownLatch latch3 = new CountDownLatch(1);
    var res3 = new ArrayList<ReceivedRawRecord>();
    var consumer3 =
        createConsumerWithFixNumsRecords(
            hStreamClient, maxReceivedCountC3, subscription, "consumer2", res3, latch3, lock);

    consumer3.startAsync().awaitRunning();
    var done3 = latch3.await(10, TimeUnit.SECONDS);
    consumer3.stopAsync().awaitTerminated();
    Assertions.assertTrue(done3);

    System.out.printf(
        "c1 consume: %d. c2 consume: %d, c3 consume: %d\n", res1.size(), res2.size(), res3.size());
    Assertions.assertEquals(recordCount, res1.size() + res2.size() + res3.size());
    // we support at-least-once consume now, some duplicated retrans is allowed.
    //    var set1 = res1.stream().map(ReceivedRawRecord::getRecordId).collect(Collectors.toSet());
    //    var set2 = res2.stream().map(ReceivedRawRecord::getRecordId).collect(Collectors.toSet());
    //    var set3 = res3.stream().map(ReceivedRawRecord::getRecordId).collect(Collectors.toSet());
    //    Assertions.assertTrue(
    //        Collections.disjoint(set1, set3), "consumer1 and consumer3 received same recordId");
    //    Assertions.assertTrue(
    //        Collections.disjoint(set2, set3), "consumer2 and consumer3 received same recordId");
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

  @Disabled("enable after HS-810 fix.")
  @Test
  @Timeout(60)
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionWithTimeout(hStreamClient, streamName, 1);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(50).build();
    final int count = 10000;
    Random rand = new Random();
    List<RecordId> records = doProduceAndGatherRid(producer, 100, count);
    CountDownLatch signal = new CountDownLatch(count);
    var receivedRecords = new ArrayList<ArrayList<ReceivedRawRecord>>();
    for (int i = 0; i < 4; i++) {
      receivedRecords.add(new ArrayList<>());
    }
    var consumers = new ArrayList<Consumer>();
    var lock = new ReentrantLock();
    consumers.add(
        createConsumer(
            hStreamClient, subscription, "consumer-1", receivedRecords.get(0), signal, lock));
    consumers.add(
        createConsumer(
            hStreamClient, subscription, "consumer-2", receivedRecords.get(1), signal, lock));
    consumers.add(
        createConsumer(
            hStreamClient, subscription, "consumer-3", receivedRecords.get(2), signal, lock));
    consumers.add(
        createConsumer(
            hStreamClient, subscription, "consumer-4", receivedRecords.get(3), signal, lock));
    consumers.forEach(c -> c.startAsync().awaitRunning());

    int cnt = 10;
    int lastIdx = -1;
    int alive = 4;
    while (cnt > 0) {
      Thread.sleep(1000);
      int idx = rand.nextInt(4);
      if (idx != lastIdx) {
        if (consumers.get(idx).isRunning()) {
          consumers.get(idx).stopAsync().awaitTerminated();
          System.out.println("==================== stop consumer: " + (idx + 1));
          alive--;
          if (alive == 0) {
            System.out.println("!!!!!!!!!!!!!!!! noConsumer");
          }
        } else {
          consumers.set(
              idx,
              createConsumer(
                  hStreamClient,
                  subscription,
                  "consumer-" + (idx + 1),
                  receivedRecords.get(idx),
                  signal,
                  lock));
          System.out.println("==================== start consumer: " + (idx + 1));
          alive++;
        }
        lastIdx = idx;
      }
      cnt--;
    }

    for (int i = 0; i < consumers.size(); i++) {
      if (!consumers.get(i).isRunning()) {
        consumers.set(
            i,
            createConsumer(
                hStreamClient,
                subscription,
                "consumer-" + (i + 1),
                receivedRecords.get(i),
                signal,
                lock));
      }
    }

    boolean done = signal.await(20, TimeUnit.SECONDS);
    System.out.println(server.getLogs());
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
  }
}
