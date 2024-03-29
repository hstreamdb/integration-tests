package io.hstream.testing;

import static io.hstream.testing.Utils.ConsumerService.consume;
import static io.hstream.testing.Utils.ConsumerService.startConsume;
import static io.hstream.testing.Utils.TestUtils.*;
import static io.hstream.testing.Utils.TestUtils.diffAndLogResultSets;
import static io.hstream.testing.Utils.TestUtils.handleForKeys;
import static io.hstream.testing.Utils.TestUtils.handleForKeysSync;
import static io.hstream.testing.Utils.TestUtils.makeBufferedProducer;
import static io.hstream.testing.Utils.TestUtils.produce;
import static io.hstream.testing.Utils.TestUtils.randStream;
import static io.hstream.testing.Utils.TestUtils.randSubscription;
import static io.hstream.testing.Utils.TestUtils.randSubscriptionWithTimeout;
import static io.hstream.testing.Utils.TestUtils.receiveNRawRecords;
import static org.assertj.core.api.Assertions.*;

import io.hstream.*;
import io.hstream.testing.Utils.ConsumerService;
import io.hstream.testing.Utils.TestUtils;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class PartitionTest {
  HStreamClient client;
  private static final Logger logger = LoggerFactory.getLogger(PartitionTest.class);
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(10)
  void testListShards() {
    int ShardCnt = 5;
    String streamName = randStream(client, ShardCnt);
    var shards = client.listShards(streamName);
    assertThat(shards).hasSize(ShardCnt);
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
    assertThat(diffAndLogResultSets(produced, received)).isTrue();
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
  @Timeout(60)
  void testOrderWithRandomKeys() throws Exception {
    int shardCount = 5;
    String streamName = randStream(client, shardCount);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    int count = 1000;
    int keys = 100;
    var pairs = produce(producer, 128, count, new TestUtils.RandomKeyGenerator(keys));
    producer.close();
    String subscription = randSubscription(client, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        client, subscription, streamName, 10, receiveNRawRecords(count, received, receivedCount));
    assertThat(diffAndLogResultSets(pairs, received)).isTrue();
    assertThat(received).isEqualTo(pairs);
  }

  @Test
  @Timeout(60)
  void testConsumerGroup() throws Exception {
    int shardCount = 3;
    final String streamName = randStream(client, shardCount);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 1000;
    final int keysSize = 100;
    // write
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();

    // read
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);
    var consumers = new ArrayList<ConsumerService>();
    for (int i = 0; i < shardCount; i++) {
      consumers.add(startConsume(client, subscription, handleForKeys(received, latch)));
    }

    assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
    consumers.forEach(ConsumerService::stop);
    assertThat(diffAndLogResultSets(pairs, received)).isTrue();
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

    var sub = randSubscriptionWithTimeoutAndMaxUnack(client, streamName, 5, 100);
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
    Thread.sleep(500);

    // consumer 2
    consume(client, sub, "c2", 10, handleForKeysSync(received, maxReceivedCountC2));
    logger.info("received:{}", received);
    // waiting for server to handler ACKs
    Thread.sleep(500);

    // start a new consumer to consume the rest records.
    consume(client, sub, "c3", 10, handleForKeysSync(received, rest));
    assertThat(diffAndLogResultSets(wrote, received)).isTrue();
  }

  @Test
  @Timeout(60)
  void testAddConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client, 5);
    final String subscription = randSubscription(client, streamName);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 2000;
    final int keysSize = 100;
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();
    var res = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);
    var c1 = startConsume(client, subscription, handleForKeys(res, latch));
    Thread.sleep(500);
    var c2 = startConsume(client, subscription, handleForKeys(res, latch));
    assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
    c1.stop();
    c2.stop();
    assertThat(diffAndLogResultSets(pairs, res)).isTrue();
  }

  @Test
  @Timeout(60)
  void testReduceConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client, 10);
    final String subscription = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 5000;
    final int keysSize = 100;
    var wrote = produce(producer, 10, count, keysSize);
    producer.close();
    CountDownLatch signal = new CountDownLatch(count);
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var f1 = startConsume(client, subscription, "c1", handleForKeys(received, signal));
    var f2 = startConsume(client, subscription, "c2", handleForKeys(received, signal));
    var f3 = startConsume(client, subscription, "c3", handleForKeys(received, signal));

    while (signal.getCount() > count * 2 / 3) {
      Thread.sleep(5);
    }
    f1.stop();

    while (signal.getCount() > count / 2) {
      Thread.sleep(5);
    }
    f2.stop();

    assertThat(signal.await(40, TimeUnit.SECONDS)).isTrue();
    f3.stop();

    assertThat(diffAndLogResultSetsWithoutDuplicated(wrote, received)).isTrue();
  }

  @Timeout(60)
  @Test
  void testLargeConsumerGroup() throws Exception {
    final String streamName = randStream(client, 20);
    final String subscription = randSubscription(client, streamName);
    io.hstream.Producer producer = client.newProducer().stream(streamName).build();
    byte[] rRec = new byte[10];
    var rids = new ArrayList<String>();
    var writes = new ArrayList<CompletableFuture<String>>();
    // NOTE: parallel should not be too large, otherwise the store server will
    // reject the request. Especially for github ci environment.
    int count = 2000;
    for (int i = 0; i < count; i++) {
      if (i % 20 == 0) {
        Thread.sleep(500);
      }
      writes.add(
          producer.write(Record.newBuilder().rawRecord(rRec).partitionKey("k_" + i % 10).build()));
    }
    CompletableFuture.allOf(writes.toArray(new CompletableFuture[0])).join();
    for (var f : writes) {
      rids.add(f.get());
    }

    CountDownLatch signal = new CountDownLatch(count);
    var receivedRids = new ArrayList<String>();
    // start 5 consumers
    for (int i = 0; i < 5; i++) {
      var cm = "c" + i;
      client
          .newConsumer()
          .subscription(subscription)
          .name(cm)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                logger.info("consumer {} received:{}", cm, receivedRawRecord.getRecordId());
                synchronized (receivedRids) {
                  receivedRids.add(receivedRawRecord.getRecordId());
                }
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }

    for (int i = 0; i < 15; i++) {
      Thread.sleep(5);
      var cm = "c" + (10 + i);
      client
          .newConsumer()
          .subscription(subscription)
          .name(cm)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                synchronized (receivedRids) {
                  receivedRids.add(receivedRawRecord.getRecordId());
                }
                logger.info("consumer {} received:{}", cm, receivedRawRecord.getRecordId());
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }
    assertThat(signal.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(receivedRids.stream().sorted().distinct().collect(Collectors.toList()))
        .as("duplicated consume should be avoid")
        .containsExactlyInAnyOrderElementsOf(receivedRids);
    assertThat(receivedRids).containsExactlyInAnyOrderElementsOf(rids);
  }

  @Timeout(60)
  @Test
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(client, 5);
    final String subscription = randSubscriptionWithTimeout(client, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 50);
    final int count = 20000;
    final int keysSize = 50;
    var pairs = produce(producer, 10, count, keysSize);
    producer.flush();
    producer.close();
    assertThat(pairs.values().stream().map(r -> r.ids.size()).reduce(0, Integer::sum))
        .isEqualTo(count);

    var res = new HashMap<String, TestUtils.RecordsPair>();
    CountDownLatch signal = new CountDownLatch(count);
    var consumers = new LinkedList<ConsumerService>();
    var alived = new HashSet<String>();
    var died = new HashSet<String>();
    // start 5 consumers
    int counsumerCount = 5;
    for (int i = 0; i < counsumerCount; i++) {
      var consumer = startConsume(client, subscription, "c" + i, handleForKeys(res, signal));
      alived.add(consumer.getConsumerName());
      // FIXME: give some time for server to allocate task to consumer
      Thread.sleep(100);
      consumers.add(consumer);
    }

    // FIXME：give some time for previous consumers to consume some records
    Thread.sleep(800);

    // randomly kill and start some consumers
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      if (globalRandom.nextInt(4) % 2 == 0 && !consumers.isEmpty()) {
        var c = consumers.poll();
        died.add(c.getConsumerName());
        alived.remove(c.getConsumerName());
        c.stop();
        logger.info("stopped consumer {}", c.getConsumerName());
      } else {
        var consumer =
            startConsume(client, subscription, "c" + counsumerCount, handleForKeys(res, signal));
        counsumerCount++;
        consumers.add(consumer);
        alived.add(consumer.getConsumerName());
        logger.info("started a new consumer {}", consumer.getConsumerName());
      }
    }

    logger.info(
        "after random kill and start, alived consumers: [{}], died consumers: [{}]",
        String.join(",", alived),
        String.join(",", died));

    assertThat(signal.await(40, TimeUnit.SECONDS))
        .withFailMessage(
            "wait consumer timeout, received %d records, expected %d, CountDown latch remained count %d",
            res.values().stream().map(r -> r.ids.size()).reduce(0, Integer::sum),
            count,
            signal.getCount())
        .isTrue();
    consumers.forEach(ConsumerService::stop);
    assertThat(diffAndLogResultSetsWithoutDuplicated(pairs, res)).isTrue();
  }
}
