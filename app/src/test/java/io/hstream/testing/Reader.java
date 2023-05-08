package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Reader {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  HStreamClient client;

  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(10)
  void testCreateReaderWithNonExistShardShouldFail() {
    int ShardCnt = 1;
    String streamName = randStream(client, ShardCnt);

    Assertions.assertThrows(
        Throwable.class,
        () -> client.newReader().readerId("reader").streamName(streamName).shardId(10098).build());
  }

  @Test
  @Timeout(10)
  void testCreateShardReaderWithSameReaderIdShouldFail() {
    int ShardCnt = 1;
    String streamName = randStream(client, ShardCnt);
    var readerId = "reader";
    var shardId = client.listShards(streamName).get(0).getShardId();
    client.newReader().readerId(readerId).streamName(streamName).shardId(shardId).build();
    Assertions.assertThrows(
        Throwable.class,
        () ->
            client.newReader().readerId("reader").streamName(streamName).shardId(shardId).build());
  }

  @Test
  @Timeout(15)
  void testReaderReadFromLatest() throws Throwable {
    int ShardCnt = 5;
    String streamName = randStream(client, ShardCnt);
    int threadCount = 10;
    int count = 1000;
    int keys = 16;

    var rids = new ArrayList<Thread>();
    var readRes =
        TestUtils.readStreamShards(
            client,
            ShardCnt,
            streamName,
            threadCount * count,
            rids,
            new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST));
    rids.forEach(Thread::start);

    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(10).build())
            .build();
    HashMap<String, RecordsPair> produced =
        batchAppendConcurrentlyWithRandomKey(
            producer, threadCount, count, 128, new RandomKeyGenerator(keys));
    producer.close();

    for (var rid : rids) {
      rid.join();
    }

    logger.info("length of ReadRes = {}", readRes.size());
    Assertions.assertEquals(threadCount * count, readRes.size());
  }

  @Test
  @Timeout(15)
  void testReaderReadFromEarliest() throws Throwable {
    int ShardCnt = 5;
    String streamName = randStream(client, ShardCnt);
    int threadCount = 10;
    int count = 1000;
    int keys = 16;

    var rids = new ArrayList<Thread>();
    var readRes =
        TestUtils.readStreamShards(
            client,
            ShardCnt,
            streamName,
            threadCount * count,
            rids,
            new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST));
    rids.forEach(Thread::start);

    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(10).build())
            .build();
    HashMap<String, RecordsPair> produced =
        batchAppendConcurrentlyWithRandomKey(
            producer, threadCount, count, 128, new RandomKeyGenerator(keys));
    producer.close();

    for (var rid : rids) {
      rid.join();
    }

    logger.info("length of ReadRes = {}", readRes.size());
    Assertions.assertEquals(threadCount * count, readRes.size());
  }

  @Test
  @Timeout(15)
  void testReaderReadFromRecordId() throws InterruptedException {
    int ShardCnt = 1;
    String streamName = randStream(client, ShardCnt);
    int count = 1000;

    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];

    var rids = new ArrayList<String>();
    for (int i = 0; i < count; i++) {
      rand.nextBytes(record);
      String rId = producer.write(buildRecord(record)).join();
      rids.add(rId);
    }

    var idx = rand.nextInt(rids.size());
    var shard = client.listShards(streamName).get(0).getShardId();
    var reader =
        client
            .newReader()
            .readerId("reader")
            .streamName(streamName)
            .shardId(shard)
            .shardOffset(new StreamShardOffset(rids.get(idx)))
            .timeoutMs(100)
            .build();
    logger.info("read offset: {}, it's the {} record in rids.", rids.get(idx), idx);
    var res = new ArrayList<ReceivedRecord>();
    var readCnts = new AtomicInteger(5);
    while (true) {
      var cnt = readCnts.decrementAndGet();
      if (cnt < 0) {
        break;
      }
      reader
          .read(1000)
          .thenApply(
              records -> {
                logger.info("read {} records", records.size());
                synchronized (res) {
                  res.addAll(records);
                }
                return null;
              });
      Thread.sleep(1000);
    }

    Assertions.assertEquals(rids.size() - idx, res.size());
  }

  @Test
  @Timeout(30)
  void testStreamReaderReadFromEarlist() throws Throwable {
    checkStreamReaderReadWithSpecialOffset(
        new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST));
  }

  @Test
  @Timeout(30)
  void testStreamReaderReadFromLatest() throws Throwable {
    checkStreamReaderReadWithSpecialOffset(
        new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST));
  }

  @Test
  @Timeout(30)
  void testStreamReaderReadFromRecordId() throws Throwable {
    String streamName = randStream(client, 1);
    int count = 500;

    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    rand.setSeed(System.currentTimeMillis());
    byte[] record = new byte[10];

    var rids = new ArrayList<String>();
    for (int i = 0; i < count; i++) {
      rand.nextBytes(record);
      String rId = producer.write(buildRecord(record)).join();
      rids.add(rId);
    }

    var idx = rand.nextInt(rids.size());
    var offset = new StreamShardOffset(rids.get(idx));
    logger.info("read offset: {}, it's the {} record in rids.", rids.get(idx), idx);
    var writes = new ArrayList<String>();
    for (int i = idx; i < rids.size(); i++) {
      writes.add(rids.get(i));
    }

    var totalRead = rids.size() - idx;
    logger.info("records need to read: {}", totalRead);
    var shards = client.listShards(streamName);
    var countDownLatch = new CountDownLatch(1);
    var readRes = new ArrayList<String>();
    var terminate = new AtomicBoolean(false);

    var reader =
        client
            .newStreamShardReader()
            .streamName(streamName)
            .shardId(shards.get(0).getShardId())
            .shardOffset(offset)
            .receiver(
                records -> {
                  if (terminate.get()) {
                    return;
                  }

                  synchronized (readRes) {
                    readRes.add(records.getRecordId());
                    if (readRes.size() >= totalRead) {
                      logger.info("count down");
                      countDownLatch.countDown();
                      terminate.set(true);
                    }
                  }
                })
            .build();
    reader.startAsync().awaitRunning(5, TimeUnit.SECONDS);

    countDownLatch.await();
    reader.stopAsync();
    reader.awaitTerminated(10, TimeUnit.SECONDS);
    logger.info("length of ReadRes = {}", readRes.size());

    writes.sort(String::compareTo);
    readRes.sort(String::compareTo);
    Assertions.assertEquals(writes, readRes);
  }

  @Test
  @Timeout(30)
  void testStreamReaderReadFromTimestamp() throws Throwable {
    String streamName = randStream(client, 1);
    int count = 500;

    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    rand.setSeed(System.currentTimeMillis());
    byte[] record = new byte[10];

    var rids = new ArrayList<String>(count);
    var breakPoints = rand.nextInt(count);
    StreamShardOffset offset = null;
    int startIdx = 0;
    for (int i = 0; i < count; i++) {
      Thread.sleep(5);
      rand.nextBytes(record);
      if (i == breakPoints) {
        offset = new StreamShardOffset(System.currentTimeMillis());
        startIdx = i;
      }
      String rId = producer.write(buildRecord(record)).join();
      rids.add(rId);
    }

    assert (offset != null);
    logger.info(
        "read offset: {}, expected first recordId: {}, it's the {} record in rids.",
        offset.getTimestampOffset(),
        rids.get(startIdx),
        startIdx);

    var writes = new ArrayList<String>();
    for (int i = startIdx; i < rids.size(); i++) {
      writes.add(rids.get(i));
    }

    var totalRead = rids.size() - startIdx;
    logger.info("records need to read: {}", totalRead);
    var shards = client.listShards(streamName);
    var countDownLatch = new CountDownLatch(1);
    var readRes = new ArrayList<String>();
    var terminate = new AtomicBoolean(false);

    var reader =
        client
            .newStreamShardReader()
            .streamName(streamName)
            .shardId(shards.get(0).getShardId())
            .shardOffset(offset)
            .receiver(
                records -> {
                  if (terminate.get()) {
                    return;
                  }

                  synchronized (readRes) {
                    logger.info(
                        "read record {}, create time {}",
                        records.getRecordId(),
                        records.getCreatedTime().toEpochMilli());
                    readRes.add(records.getRecordId());
                    if (readRes.size() >= totalRead) {
                      logger.info("count down");
                      countDownLatch.countDown();
                      terminate.set(true);
                    }
                  }
                })
            .build();
    reader.startAsync().awaitRunning(5, TimeUnit.SECONDS);

    countDownLatch.await();
    reader.stopAsync();
    reader.awaitTerminated(10, TimeUnit.SECONDS);
    logger.info("length of ReadRes = {}", readRes.size());

    writes.sort(String::compareTo);
    readRes.sort(String::compareTo);
    Assertions.assertEquals(writes, readRes);
  }

  void checkStreamReaderReadWithSpecialOffset(StreamShardOffset offset) throws Throwable {
    int shardCnt = 5;
    String streamName = randStream(client, shardCnt);
    int threadCount = 5;
    int count = 1000;
    int keys = 16;

    var shards = client.listShards(streamName);
    var totalRead = threadCount * count;
    var countDownLatch = new CountDownLatch(1);
    var readRes = new ArrayList<ReceivedRecord>();
    var readers = new ArrayList<StreamShardReader>();
    var terminate = new AtomicBoolean(false);

    for (int i = 0; i < shardCnt; i++) {
      var reader =
          client
              .newStreamShardReader()
              .streamName(streamName)
              .shardId(shards.get(i).getShardId())
              .shardOffset(offset)
              .receiver(
                  records -> {
                    if (terminate.get()) {
                      return;
                    }

                    synchronized (readRes) {
                      readRes.add(records);
                      if (readRes.size() >= totalRead) {
                        logger.info("count down");
                        countDownLatch.countDown();
                        terminate.set(true);
                      }
                      if (readRes.size() % 100 == 0) {
                        logger.info("read {} records", readRes.size());
                      }
                    }
                  })
              .build();
      reader.startAsync().awaitRunning(5, TimeUnit.SECONDS);
      readers.add(reader);
    }

    // Wait for reader creation to complete
    Thread.sleep(2000);

    BufferedProducer producer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(10).build())
            .build();
    HashMap<String, RecordsPair> produced =
        batchAppendConcurrentlyWithRandomKey(
            producer, threadCount, count, 10, new RandomKeyGenerator(keys));
    producer.close();

    countDownLatch.await();
    for (var reader : readers) {
      reader.stopAsync();
      reader.awaitTerminated(10, TimeUnit.SECONDS);
    }

    logger.info("length of ReadRes = {}", readRes.size());
    Assertions.assertEquals(threadCount * count, readRes.size());
  }
}
