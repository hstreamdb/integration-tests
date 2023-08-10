package io.hstream.testing;

import static io.hstream.testing.Utils.TestUtils.*;
import static org.assertj.core.api.Assertions.*;

import io.hstream.*;
import io.hstream.testing.Utils.TestUtils;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
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
public class ReaderTest {
  private static final Logger logger = LoggerFactory.getLogger(ReaderTest.class);
  HStreamClient client;

  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
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
    var rand = new Random(System.currentTimeMillis());
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
            .from(offset)
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
    assertThat(readRes).size().isEqualTo(readRes.size());
    assertThat(readRes).containsExactlyElementsOf(writes);
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
            .from(offset)
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
    assertThat(readRes).size().isEqualTo(readRes.size());
    assertThat(readRes).containsExactlyElementsOf(writes);
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
              .from(offset)
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

  @Test
  @Timeout(10)
  @Deprecated
  void testCreateReaderWithNonExistShardShouldFail() {
    int ShardCnt = 1;
    String streamName = randStream(client, ShardCnt);

    Assertions.assertThrows(
        Throwable.class,
        () -> client.newReader().readerId("reader").streamName(streamName).shardId(10098).build());
  }

  @Test
  @Timeout(10)
  @Deprecated
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
  @Deprecated
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
  @Deprecated
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
  @Deprecated
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

  // ======================== Test for readStreamByKey ============================

  @Test
  @Timeout(60)
  void testReadStreamByKeyWithUnExistedKey() throws Exception {
    var rand = new Random(System.currentTimeMillis());
    var recordCount = 100;
    var shardCount = Math.max(1, rand.nextInt(5));
    var streamName = randStream(client, shardCount);
    var producer = client.newProducer().stream(streamName).build();

    var records = new HashMap<String, ArrayList<Record>>();
    var rids = new ArrayList<String>();
    for (int i = 0; i < recordCount; i++) {
      var key = "key_" + (i % 10);
      var record =
          Record.newBuilder().hRecord(HRecord.newBuilder().put("index", i).build()).build();
      record.setPartitionKey(key);
      records.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
      rids.add(producer.write(record).join());
    }

    var res = new ArrayList<ReceivedRecord>();
    StreamKeyReader streamShardReader =
        client
            .newStreamKeyReader()
            .streamName(streamName)
            .key("key-11")
            .from(new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST))
            .until(new StreamShardOffset(rids.get(rids.size() - 1)))
            .bufferSize(100)
            .build();
    while (streamShardReader.hasNext()) {
      ReceivedRecord receivedRecord = streamShardReader.next();
      if (receivedRecord == null) break;
      res.add(receivedRecord);
    }

    streamShardReader.close();
    assertThat(res.isEmpty()).as("read un-existed key should return empty list").isTrue();
  }

  @Test
  @Timeout(60)
  void testReadStreamByKeyFromUnBatchedRecords() throws Exception {
    var rand = new Random(System.currentTimeMillis());
    var keyCount = 50;
    var recordCount = 1000;
    var shardCount = Math.max(1, rand.nextInt(5));
    var streamName = randStream(client, shardCount);
    var producer = client.newProducer().stream(streamName).build();

    var records = new HashMap<String, ArrayList<Record>>();
    var rids = new ArrayList<String>();
    var readKey = "key_" + rand.nextInt(keyCount);
    var lastIdxForReadKey = 0;
    for (int i = 0; i < recordCount; i++) {
      if (i % 10 == 0) {
        Thread.sleep(20);
      }
      var key = "key_" + rand.nextInt(keyCount);
      var ts = System.currentTimeMillis();
      var record =
          Record.newBuilder()
              .hRecord(
                  HRecord.newBuilder().put("index", i).put("timestamp", ts).put("key", key).build())
              .build();
      record.setPartitionKey(key);
      records.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
      rids.add(producer.write(record).join());
      if (key.equals(readKey)) {
        lastIdxForReadKey = i;
      }
    }

    StreamKeyReader streamShardReader =
        client
            .newStreamKeyReader()
            .streamName(streamName)
            .key(readKey)
            .from(new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST))
            .until(new StreamShardOffset(rids.get(lastIdxForReadKey)))
            .bufferSize(100)
            .build();

    var receivedRecords = new ArrayList<ReceivedRecord>();
    var lastIndex = 0;
    while (streamShardReader.hasNext()) {
      ReceivedRecord receivedRecord = streamShardReader.next();
      if (receivedRecord == null) break;

      var index = receivedRecord.getRecord().getHRecord().getInt("index");
      assertThat(index).isGreaterThan(lastIndex);
      assertThat(receivedRecord.getRecord().getPartitionKey()).isEqualTo(readKey);
      receivedRecords.add(receivedRecord);
      lastIndex = index;
    }

    Thread.sleep(2000);
    streamShardReader.close();

    var targetRecords = records.get(readKey);
    logger.info("write {} records for key {}: ", targetRecords.size(), readKey);

    for (var record : targetRecords) {
      var hrecord = record.getHRecord();
      logger.info(
          "{}: index={}, timstamp={}",
          rids.get(hrecord.getInt("index")),
          hrecord.getInt("index"),
          hrecord.getInt("timestamp"));
    }

    logger.info("read {} records for key {}: ", receivedRecords.size(), readKey);
    for (var receivedRecord : receivedRecords) {
      var hrecord = receivedRecord.getRecord().getHRecord();
      logger.info(
          "{}: index={}, timstamp={}",
          receivedRecord.getRecordId(),
          hrecord.getInt("index"),
          hrecord.getInt("timestamp"));
    }

    assertThat(receivedRecords.size()).isEqualTo(records.get(readKey).size());

    for (int i = 0; i < receivedRecords.size(); i++) {
      assertThat(receivedRecords.get(i).getRecordId())
          .isEqualTo(rids.get(targetRecords.get(i).getHRecord().getInt("index")));
      assertThat(receivedRecords.get(i).getRecord().getHRecord().getInt("index"))
          .isEqualTo(targetRecords.get(i).getHRecord().getInt("index"));
      assertThat(receivedRecords.get(i).getRecord().getHRecord().getInt("timestamp"))
          .isEqualTo(targetRecords.get(i).getHRecord().getInt("timestamp"));
    }
  }

  @Test
  @Timeout(60)
  void testReadStreamByKeyFromBatchedRecords() throws Exception {
    var rand = new Random(System.currentTimeMillis());
    var keyCount = 50;
    var recordCount = 1000;
    var shardCount = Math.max(1, rand.nextInt(5));
    var streamName = randStream(client, shardCount);
    var producer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(
                BatchSetting.newBuilder().recordCountLimit(15).ageLimit(10).bytesLimit(-1).build())
            .build();

    var records = new HashMap<String, ArrayList<Record>>();
    var rids = new HashMap<String, ArrayList<String>>();
    var futures = new ArrayList<CompletableFuture<Void>>();
    var readKey = "key_" + rand.nextInt(keyCount);
    for (int i = 0; i < recordCount; i++) {
      if (i % 10 == 0) {
        Thread.sleep(20);
      }
      var key = "key_" + rand.nextInt(keyCount);
      var ts = Instant.now().toEpochMilli();
      var record =
          Record.newBuilder()
              .hRecord(
                  HRecord.newBuilder().put("index", i).put("timestamp", ts).put("key", key).build())
              .build();
      record.setPartitionKey(key);
      records.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
      futures.add(
          producer
              .write(record)
              .thenAccept(rid -> rids.computeIfAbsent(key, k -> new ArrayList<>()).add(rid)));
    }

    futures.forEach(CompletableFuture::join);

    var targetRids = rids.get(readKey);

    StreamKeyReader streamShardReader =
        client
            .newStreamKeyReader()
            .streamName(streamName)
            .key(readKey)
            .from(new StreamShardOffset(StreamShardOffset.SpecialOffset.EARLIEST))
            .until(new StreamShardOffset(targetRids.get(targetRids.size() - 1)))
            .bufferSize(100)
            .build();

    var receivedRecords = new ArrayList<ReceivedRecord>();
    var lastIndex = 0;
    while (streamShardReader.hasNext()) {
      ReceivedRecord receivedRecord = streamShardReader.next();
      if (receivedRecord == null) break;

      var index = receivedRecord.getRecord().getHRecord().getInt("index");
      assertThat(index).isGreaterThan(lastIndex);
      assertThat(receivedRecord.getRecord().getPartitionKey()).isEqualTo(readKey);
      receivedRecords.add(receivedRecord);
      lastIndex = index;
    }

    Thread.sleep(2000);
    streamShardReader.close();

    var targetRecords = records.get(readKey);
    logger.info("write {} records for key {}: ", targetRecords.size(), readKey);

    for (int i = 0; i < targetRecords.size(); i++) {
      var hrecord = targetRecords.get(i).getHRecord();
      logger.info(
          "{}: index={}, timstamp={}",
          targetRids.get(i),
          hrecord.getInt("index"),
          hrecord.getInt("timestamp"));
    }

    logger.info("read {} records for key {}: ", receivedRecords.size(), readKey);
    for (var receivedRecord : receivedRecords) {
      var hrecord = receivedRecord.getRecord().getHRecord();
      logger.info(
          "{}: index={}, timstamp={}",
          receivedRecord.getRecordId(),
          hrecord.getInt("index"),
          hrecord.getInt("timestamp"));
    }

    assertThat(receivedRecords.size()).isEqualTo(records.get(readKey).size());

    for (int i = 0; i < receivedRecords.size(); i++) {
      assertThat(receivedRecords.get(i).getRecordId()).isEqualTo(targetRids.get(i));
      assertThat(receivedRecords.get(i).getRecord().getHRecord().getInt("index"))
          .isEqualTo(targetRecords.get(i).getHRecord().getInt("index"));
      assertThat(receivedRecords.get(i).getRecord().getHRecord().getInt("timestamp"))
          .isEqualTo(targetRecords.get(i).getHRecord().getInt("timestamp"));
    }
  }
}
