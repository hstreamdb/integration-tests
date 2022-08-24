package io.hstream.testing;

import static io.hstream.testing.TestUtils.assertExceptions;
import static io.hstream.testing.TestUtils.buildRecord;
import static io.hstream.testing.TestUtils.consume;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.handleForKeysSync;
import static io.hstream.testing.TestUtils.makeBufferedProducer;
import static io.hstream.testing.TestUtils.produce;
import static io.hstream.testing.TestUtils.randRawRec;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.runWithThreads;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.CompressionType;
import io.hstream.FlowControlSetting;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.HStreamDBClientException;
import io.hstream.Stream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Producer {
  private static final Logger logger = LoggerFactory.getLogger(Producer.class);
  HStreamClient client;
  Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(60)
  void testWriteRaw() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    String rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(client, streamName);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        (r) -> {
          res.add(r.getRawRecord());
          return false;
        });
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testWriteRawOutOfPayloadLimitShouldFail() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[max];
    rand.nextBytes(record);
    Assertions.assertThrows(Exception.class, () -> producer.write(buildRecord(record)).join());
  }

  @Test
  @Timeout(60)
  void testWriteJSON() throws Exception {
    final String streamName = randStream(client);
    List<Stream> streams = client.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = client.newProducer().stream(streamName).build();
    HRecord hRec = HRecord.newBuilder().put("x", "y").put("acc", 0).put("init", false).build();
    String rId = producer.write(buildRecord(hRec)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(client, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return false;
        });
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
  }

  @Test
  @Timeout(60)
  void testWriteMixPayload() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
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

    final String subscription = randSubscription(client, streamName);
    List<HRecord> hRes = new ArrayList<>();
    List<String> rawRes = new ArrayList<>();
    int total = rawRecords.size() + hRecords.size();
    AtomicInteger received = new AtomicInteger();
    consume(
        client,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          rawRes.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return received.incrementAndGet() != total;
        },
        receivedHRecord -> {
          hRes.add(receivedHRecord.getHRecord());
          return received.incrementAndGet() != total;
        });
    var hRecordInput =
        hRecords.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var hOutputRecord = hRes.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(hRecordInput, hOutputRecord);
    Assertions.assertEquals(rawRecords, rawRes);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
    var records = doProduce(producer, 128, 100);
    producer.close();

    final String subscription = randSubscription(client, streamName);
    List<String> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testNoBatchWriteInForLoopShouldNotStuck() throws Exception {
    final String streamName = randStream(client);
    io.hstream.Producer producer = client.newProducer().stream(streamName).build();
    var records = doProduce(producer, 128, globalRandom.nextInt(100) + 1);

    final String subscription = randSubscription(client, streamName);
    List<String> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    Assertions.assertEquals(
        records.stream().sorted().collect(Collectors.toList()),
        res.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  @Timeout(60)
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100);
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

    final String subscription = randSubscription(client, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return res.size() < records.size();
        });
    var input = records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatchMultiThread() throws Throwable {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(10).ageLimit(10).build())
            .build();
    var futures = new LinkedList<CompletableFuture<String>>();
    assertExceptions(
        runWithThreads(
            10,
            () -> {
              Random rand = new Random();
              var fs = new LinkedList<CompletableFuture<String>>();
              for (int i = 0; i < 100; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                fs.add(producer.write(buildRecord(rRec)));
              }
              synchronized (futures) {
                futures.addAll(fs);
              }
            }));
    for (var f : futures) {
      Assertions.assertNotNull(f.get());
    }
  }

  @Test
  @Timeout(60)
  void testMixWriteBatchAndNoBatchRecords() throws Exception {
    final String streamName = randStream(client);
    int totalWrites = 10;
    int batchWrites = 0;
    int batchSize = 5;
    BufferedProducer batchProducer = makeBufferedProducer(client, streamName, batchSize);
    io.hstream.Producer producer = client.newProducer().stream(streamName).build();
    Random rand = new Random();
    var records = new ArrayList<String>();
    var recordIds = new ArrayList<String>();

    for (int i = 0; i < totalWrites; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        batchWrites++;
        logger.info("[turn]: {}, batch write!!!!!\n", i);
        var writes = new ArrayList<CompletableFuture<String>>(5);
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

    int totalSize = batchWrites * batchSize + (totalWrites - batchWrites);
    final String subscription = randSubscription(client, streamName);
    List<String> res = new ArrayList<>();
    List<String> receivedRecordIds = new ArrayList<>();
    consume(
        client,
        subscription,
        "test-consumer",
        20,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          receivedRecordIds.add(receivedRawRecord.getRecordId());
          return res.size() < totalSize;
        });
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
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(-1).build())
            .build();
    Random random = new Random();
    final int count = 10;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
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
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(100).build())
            .build()) {
      Random random = new Random();
      final int count = 10;
      CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
      for (int i = 0; i < count; ++i) {
        byte[] rawRecord = new byte[100];
        random.nextBytes(rawRecord);
        CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
        recordIdFutures[i] = future;
      }
      CompletableFuture.allOf(recordIdFutures).join();
    }
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordBasedBytesSize() throws Exception {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(
                BatchSetting.newBuilder()
                    .recordCountLimit(100)
                    .ageLimit(-1)
                    .bytesLimit(4096)
                    .build())
            .build();
    Random random = new Random();
    final int count = 42;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
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

  @Test
  @Timeout(60)
  public void testWriteRandomSizeRecords() throws Exception {
    var stream = randStream(client);
    BufferedProducer producer =
        client.newBufferedProducer().stream(stream)
            .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(40960).build())
            .build();
    int count = 1000;
    var pairs = produce(producer, count, new TestUtils.RandomSizeRecordGenerator(128, 10240));
    producer.close();
    logger.info("wrote :{}", pairs);

    var sub = randSubscription(client, stream);
    var res = new HashMap<String, TestUtils.RecordsPair>();
    consume(client, sub, 20, handleForKeysSync(res, count));
    Assertions.assertEquals(pairs, res);
  }

  @Test
  @Timeout(60)
  public void testBadFlowControlSettingShouldFail() {
    var stream = randStream(client);
    Assertions.assertThrows(
        HStreamDBClientException.class,
        () ->
            client.newBufferedProducer().stream(stream)
                .batchSetting(BatchSetting.newBuilder().bytesLimit(4096).build())
                .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(1024).build())
                .build());
  }

  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() throws Exception {
    String stream = randStream(client);

    io.hstream.Producer producer = client.newProducer().stream(stream).build();

    String id0 = producer.write(randRawRec()).join();
    String id1 = producer.write(randRawRec()).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);

    client.deleteStream(stream);
    Assertions.assertThrows(Exception.class, () -> producer.write(randRawRec()).join());
  }

  @Test
  @Timeout(20)
  void testWriteRawBatchWithCompression() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 100, CompressionType.GZIP);
    var records = doProduce(producer, 128, 100);
    producer.close();

    final String subscription = randSubscription(client, streamName);
    List<String> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testWriteJSONBatchWithCompression() throws Exception {
    final String streamName = randStream(client);
    BufferedProducer producer = makeBufferedProducer(client, streamName, 10, CompressionType.GZIP);
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

    final String subscription = randSubscription(client, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return res.size() < records.size();
        });
    var input = records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }
}
