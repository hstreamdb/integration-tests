package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
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

  @Disabled("server need a method to check if specific logId exist.")
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
    client.newReader().readerId(readerId).streamName(streamName).build();
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

    System.out.println("length of ReadRes = " + readRes.size());
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

    System.out.println("length of ReadRes = " + readRes.size());
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
    System.out.printf("read offset: %s, it's %d record in rids.\n", rids.get(idx), idx);
    var res = new ArrayList<Record>();
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
                System.out.printf("read %s records", records.size());
                synchronized (res) {
                  res.addAll(records);
                }
                return null;
              });
      Thread.sleep(1000);
    }

    Assertions.assertEquals(rids.size() - idx, res.size());
  }
}
