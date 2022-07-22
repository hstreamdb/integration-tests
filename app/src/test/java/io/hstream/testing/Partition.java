package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;
import static io.hstream.testing.TestUtils.consume;
import static io.hstream.testing.TestUtils.consumeAsync;
import static io.hstream.testing.TestUtils.diffAndLogResultSets;
import static io.hstream.testing.TestUtils.generateKeysIncludingDefaultKey;
import static io.hstream.testing.TestUtils.handleForKeys;
import static io.hstream.testing.TestUtils.handleForKeysSync;
import static io.hstream.testing.TestUtils.makeBufferedProducer;
import static io.hstream.testing.TestUtils.produce;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionWithTimeout;
import static io.hstream.testing.TestUtils.receiveNRawRecords;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.Record;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Partition {
  HStreamClient client;
  private static final Logger logger = LoggerFactory.getLogger(Partition.class);
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(10)
  void testListShards() throws Throwable {
    int ShardCnt = 5;
    String streamName = randStream(client, ShardCnt);
    var shards = client.listShards(streamName);
    Assertions.assertEquals(shards.size(), ShardCnt);
    // listShards should be an idempotent API
    assertExceptions(
        runWithThreads(
            5,
            () -> {
              var res = client.listShards(streamName);
              Assertions.assertEquals(shards, res);
            }));
  }

  @Test
  @Timeout(60)
  void testWriteToMultiPartition() throws Throwable {
    int ShardCnt = 5;
    int threadCount = 10;
    int count = 1000;
    int keys = 16;
    String streamName = randStream(client, ShardCnt);
    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(10).build())
            .build();
    HashMap<String, TestUtils.RecordsPair> produced =
        batchAppendConcurrentlyWithRandomKey(
            producer, threadCount, count, 128, new RandomKeyGenerator(keys));
    producer.close();
    // check same key should be appended to same shard
    produced.forEach((k, v) -> assertShardId(v.ids));
    String subscription = randSubscription(client, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        client,
        subscription,
        streamName,
        20,
        receiveNRawRecords(count * threadCount, received, receivedCount));
    // check all appended records should be fetched.
    Assertions.assertTrue(diffAndLogResultSets(produced, received));
  }

  @Test
  void testOrder() throws Exception {
    String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    int count = 1000;
    TestUtils.RecordsPair pair = produce(producer, 1024, count);
    producer.close();
    String subscription = randSubscription(client, streamName);
    var ids = new ArrayList<String>(count);
    var records = new ArrayList<String>(count);
    consume(
        client,
        subscription,
        streamName,
        10,
        receivedRawRecord -> {
          ids.add(receivedRawRecord.getRecordId());
          records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return ids.size() < count;
        });
    Assertions.assertEquals(pair.ids, ids);
    Assertions.assertEquals(pair.records, records);
  }

  @Test
  void testOrderWithKeys() throws Exception {
    String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    int count = 1000;
    int keys = 5;
    var pairs = produce(producer, 1024, count, keys);
    producer.close();
    String subscription = randSubscription(client, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        client, subscription, streamName, 10, receiveNRawRecords(count, received, receivedCount));
    Assertions.assertEquals(pairs, received);
  }

  @Test
  @Timeout(60)
  void testOrderWithRandomKeys() throws Exception {
    String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    int count = 1000;
    int keys = 5;
    var pairs = produce(producer, 128, count, new TestUtils.RandomKeyGenerator(keys));
    producer.close();
    String subscription = randSubscription(client, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        client, subscription, streamName, 10, receiveNRawRecords(count, received, receivedCount));
    Assertions.assertTrue(diffAndLogResultSets(pairs, received));
  }

  @Test
  @Timeout(60)
  void testConsumerGroup() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 500;
    final int keysSize = 3;
    // write
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();

    // read
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);

    consumeAsync(client, subscription, handleForKeys(received, latch));
    consumeAsync(client, subscription, handleForKeys(received, latch));
    consumeAsync(client, subscription, handleForKeys(received, latch));

    Assertions.assertTrue(latch.await(20, TimeUnit.SECONDS));
    Assertions.assertTrue(diffAndLogResultSets(pairs, received));
  }

  @Test
  @Timeout(60)
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 2500;
    final String streamName = randStream(client);

    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    int keysSize = 1;
    var wrote = produce(producer, 1, 2500, keysSize);
    logger.info("wrote:{}", wrote);
    producer.close();

    var sub = randSubscriptionWithTimeout(client, streamName, 5);
    // receive part of records and stop consumers
    final int maxReceivedCountC1 = Math.max(1, globalRandom.nextInt(recordCount / 3));
    final int maxReceivedCountC2 = Math.max(1, globalRandom.nextInt(recordCount / 3));
    var rest = recordCount - maxReceivedCountC1 - maxReceivedCountC2;
    logger.info(
        "maxReceivedCountC1:{}, C2:{}, rest:{}", maxReceivedCountC1, maxReceivedCountC2, rest);
    var received = new HashMap<String, TestUtils.RecordsPair>();

    // consumer 1
    consume(client, sub, "c1", 10, handleForKeysSync(received, maxReceivedCountC1));
    logger.info("received:{}", received);
    // waiting for server to handler ACKs
    Thread.sleep(7000);

    // consumer 2
    consume(client, sub, "c2", 10, handleForKeysSync(received, maxReceivedCountC2));
    logger.info("received:{}", received);
    // waiting for server to handler ACKs
    Thread.sleep(7000);

    // start a new consumer to consume the rest records.
    consume(client, sub, "c3", 10, handleForKeysSync(received, rest));
    Assertions.assertTrue(diffAndLogResultSets(wrote, received));
  }

  @Test
  @Timeout(60)
  void testAddConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 5000;
    final int keysSize = 5;
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();
    var res = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);
    var f1 = consumeAsync(client, subscription, handleForKeys(res, latch));
    var f2 = consumeAsync(client, subscription, handleForKeys(res, latch));

    Thread.sleep(1000);
    var f3 = consumeAsync(client, subscription, handleForKeys(res, latch));
    Assertions.assertTrue(latch.await(20, TimeUnit.SECONDS));
    CompletableFuture.allOf(f1, f2, f3).complete(null);
    Assertions.assertTrue(diffAndLogResultSets(pairs, res));
  }

  @Test
  @Timeout(60)
  void testReduceConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 5000;
    final int keysSize = 5;
    var wrote = produce(producer, 100, count, keysSize);
    producer.close();
    CountDownLatch signal = new CountDownLatch(count);
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var f1 = consumeAsync(client, subscription, handleForKeys(received, signal));
    var f2 = consumeAsync(client, subscription, handleForKeys(received, signal));
    var f3 = consumeAsync(client, subscription, handleForKeys(received, signal));

    while (signal.getCount() > count / 3) {
      Thread.sleep(10);
    }
    f1.complete(null);

    while (signal.getCount() > count / 2) {
      Thread.sleep(10);
    }
    f2.complete(null);

    boolean done = signal.await(20, TimeUnit.SECONDS);
    f3.complete(null);
    Assertions.assertTrue(done);

    Assertions.assertTrue(diffAndLogResultSets(wrote, received));
  }

  @Timeout(60)
  @Test
  void testLargeConsumerGroup() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    io.hstream.Producer producer = client.newProducer().stream(streamName).build();
    int count = 200;
    byte[] rRec = new byte[100];
    for (int i = 0; i < count; i++) {
      producer.write(Record.newBuilder().rawRecord(rRec).orderingKey("k_" + i % 10).build()).join();
    }
    CountDownLatch signal = new CountDownLatch(count);
    // start 5 consumers
    for (int i = 0; i < 5; i++) {
      client
          .newConsumer()
          .subscription(subscription)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                logger.info("received:{}", receivedRawRecord.getRecordId());
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }

    for (int i = 0; i < 15; i++) {
      Thread.sleep(100);
      client
          .newConsumer()
          .subscription(subscription)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                logger.info("received:{}", receivedRawRecord.getRecordId());
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }

    Assertions.assertTrue(signal.await(20, TimeUnit.SECONDS), "failed to receive all records");
  }

  @Timeout(60)
  @Test
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 20000;
    final int keysSize = 10;
    CountDownLatch signal = new CountDownLatch(count);
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();
    var res = new HashMap<String, TestUtils.RecordsPair>();
    var futures = new LinkedList<CompletableFuture<Void>>();
    // start 5 consumers
    for (int i = 0; i < 5; i++) {
      futures.add(consumeAsync(client, subscription, handleForKeys(res, signal)));
    }

    // randomly kill and start some consumers
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      if (globalRandom.nextInt(4) == 0) {
        futures.pop().complete(null);
        logger.info("stopped a consumer");
      } else {
        futures.add(consumeAsync(client, subscription, handleForKeys(res, signal)));
        logger.info("started a new consumer");
      }
    }

    Assertions.assertTrue(signal.await(20, TimeUnit.SECONDS), "failed to receive all records");
    futures.forEach(it -> it.complete(null));
    Assertions.assertTrue(diffAndLogResultSets(pairs, res));
  }

  @Disabled("Can't confirm assign shard balance now.")
  @Test
  @Timeout(60)
  void testShardBalance() throws Exception {
    var stream = randStream(client);
    final String subscription = randSubscription(client, stream);
    int shardCount = 10;
    int recordCount = 100;
    int consumerCount = 7;

    // Async Read
    List<List<String>> readRes = new ArrayList<>();
    var futures = new CompletableFuture[consumerCount];
    var receivedKeys = new ArrayList<HashSet<String>>();
    var latch = new CountDownLatch(recordCount);
    for (int i = 0; i < consumerCount; ++i) {
      var records = new LinkedList<String>();
      readRes.add(records);
      var keys = new HashSet<String>();
      receivedKeys.add(keys);
      futures[i] =
          consumeAsync(
              client,
              subscription,
              "c" + i,
              receivedRawRecord -> {
                synchronized (keys) {
                  records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
                  keys.add(receivedRawRecord.getHeader().getOrderingKey());
                  latch.countDown();
                  return true;
                }
              });
    }

    Thread.sleep(10000);
    // Write
    Producer producer = client.newProducer().stream(stream).build();
    var keys = generateKeysIncludingDefaultKey(shardCount);
    var writeRes = produce(producer, 32, recordCount, new TestUtils.RandomKeyGenerator(keys));
    Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    CompletableFuture.allOf(futures).complete(null);

    // Analysis
    // Keys balancing part
    logger.info("===== Keys Stats =====");

    HashSet<String> unionOfKeys = new HashSet<>();
    for (int i = 0; i < consumerCount; ++i) {
      HashSet<String> ownedKeys = receivedKeys.get(i);
      logger.info("Consumer {}: {}", i, ownedKeys);
      // 1. When consumer number <= key number, every consumer owns at least 1 key
      Assertions.assertFalse(ownedKeys.isEmpty());
      unionOfKeys.addAll(ownedKeys);
    }
    logger.info("All allocated keys: {}", unionOfKeys);

    // 2. Every item written to the database is read out
    HashSet<String> writeResAsSet = new HashSet<>();
    HashSet<String> readResAsSet = new HashSet<>();
    for (var thisValue : writeRes.values()) {
      writeResAsSet.addAll(thisValue.records);
    }
    for (var thisValue : readRes) {
      readResAsSet.addAll(thisValue);
    }
    Assertions.assertEquals(readResAsSet, writeResAsSet);

    // 3. Assert the union of keys all consumers own is equal to all keys
    HashSet<String> expectedKeys = new HashSet<>(writeRes.keySet());
    Assertions.assertEquals(unionOfKeys, expectedKeys);
  }
}
