package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;
import static io.hstream.testing.Utils.ConsumerService.consume;
import static org.assertj.core.api.Assertions.*;

import io.hstream.*;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class ProducerTest {
  private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);
  HStreamClient client;
  Random globalRandom = new Random(System.currentTimeMillis());

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(60)
  void testCreateProducer() {
    String streamName = randStream(client);
    assertThatNoException().isThrownBy(() -> client.newProducer().stream(streamName).build());
    assertThatNoException()
        .isThrownBy(() -> client.newProducer().stream(streamName).requestTimeoutMs(100).build());
    assertThatNoException()
        .isThrownBy(() -> client.newBufferedProducer().stream(streamName).build());
    assertThatNoException()
        .isThrownBy(
            () -> client.newBufferedProducer().stream(streamName).requestTimeoutMs(100).build());

    assertThatThrownBy(() -> client.newProducer().stream("unExistStream").build())
        .as("create producer with unExist stream")
        .isInstanceOf(HStreamDBClientException.class);
    assertThatThrownBy(
            () ->
                client.newBufferedProducer().stream(streamName)
                    .batchSetting(BatchSetting.newBuilder().bytesLimit(4096).build())
                    .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(1024).build())
                    .build())
        .as("create buffer producer with invalid flow control setting")
        .isInstanceOf(HStreamDBClientException.class);
  }

  @Test
  @Timeout(60)
  void testWriteRaw() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    List<byte[]> records = new ArrayList<>();
    int cnt = 10;
    for (int i = 0; i < cnt; i++) {
      byte[] record = randBytes(10);
      records.add(record);
      var rId = producer.write(buildRecord(record)).join();
      assertThat(rId).isNotNull();
    }

    final String subscription = randSubscription(client, streamName);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        (r) -> {
          synchronized (res) {
            res.add(r.getRawRecord());
            return res.size() < cnt;
          }
        });
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
  }

  @Test
  @Timeout(60)
  void testWriteJSON() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    int cnt = 10;
    List<String> records = new ArrayList<>(cnt);
    for (int i = 0; i < cnt; i++) {
      HRecord hRec = HRecord.newBuilder().put("index", i).put("bool", i % 2 == 0).build();
      String rId = producer.write(buildRecord(hRec)).join();
      assertThat(rId).isNotNull();
      records.add(hRec.toString());
    }

    final String subscription = randSubscription(client, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          synchronized (res) {
            res.add(receivedHRecord.getHRecord());
            return res.size() < cnt;
          }
        });
    assertThat(res.stream().map(HRecord::toString).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(records);
  }

  @Test
  @Timeout(30)
  void testWriteRawOutOfPayloadLimitShouldFail() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var record = randBytes(max);
    assertThatThrownBy(() -> producer.write(buildRecord(record)).join())
        .as("write raw record with size %d", max)
        .isInstanceOf(Exception.class);
  }

  @Test
  @Timeout(60)
  void testWriteMixPayload() throws Exception {
    final String streamName = randStream(client);
    var producer = client.newProducer().stream(streamName).build();
    var rand = new Random(System.currentTimeMillis());
    var rawRecords = new ArrayList<String>();
    var hRecords = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      if (rand.nextInt() % 2 == 0) {
        var record = randBytes(128);
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
    assertThat(rawRecords).containsExactlyInAnyOrderElementsOf(rawRes);
    assertThat(hRecordInput).containsExactlyInAnyOrderElementsOf(hOutputRecord);
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
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
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
    assertThat(output).containsExactlyInAnyOrderElementsOf(input);
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
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatchMultiThread() {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(10).ageLimit(10).build())
            .build();
    var futures = new ArrayList<CompletableFuture<String>>();
    int recordPerThread = 100;
    int threadNum = 10;
    assertThatNoException()
        .isThrownBy(
            () ->
                runWithThreads(
                    threadNum,
                    () -> {
                      var fs = new LinkedList<CompletableFuture<String>>();
                      for (int i = 0; i < recordPerThread; i++) {
                        var record = randBytes(128);
                        fs.add(producer.write(buildRecord(record)));
                      }
                      synchronized (futures) {
                        futures.addAll(fs);
                      }
                    }));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    for (var f : futures) {
      assertThat(f).isCompleted();
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
    var writes = new ArrayList<CompletableFuture<String>>();

    for (int i = 0; i < totalWrites; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        batchWrites++;
        logger.info("[turn]: {}, batch write!!!!!\n", i);
        for (int j = 0; j < batchSize; j++) {
          var rRec = new byte[] {(byte) i};
          records.add(Arrays.toString(rRec));
          writes.add(batchProducer.write(buildRecord(rRec)));
        }
      } else {
        logger.info("[turn]: {}, no batch write!!!!!\n", i);
        var rRec = new byte[] {(byte) i};
        records.add(Arrays.toString(rRec));
        writes.add(producer.write(buildRecord(rRec)));
      }
    }

    batchProducer.close();
    writes.forEach(w -> recordIds.add(w.join()));
    assertThat(recordIds).size().isEqualTo(records.size());

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
    for (int i = 0; i < recordIds.size(); i++) {
      logger.info(recordIds.get(i) + ": " + records.get(i));
    }
    logger.info("received rid");
    for (int i = 0; i < receivedRecordIds.size(); i++) {
      logger.info(receivedRecordIds.get(i) + ": " + res.get(i));
    }
    assertThat(res).size().isEqualTo(records.size());
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordAndClose() {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(-1).build())
            .build();
    final int count = 10;
    var writes = new ArrayList<CompletableFuture<String>>(count);
    for (int i = 0; i < count; ++i) {
      var rawRecord = randBytes(100);
      writes.add(producer.write(buildRecord(rawRecord)));
    }
    // flush and close producer
    producer.close();

    var rids = new ArrayList<String>(count);
    writes.forEach(f -> rids.add(f.join()));
    assertThat(rids).size().isEqualTo(count);
  }

  @Test
  @Timeout(30)
  public void testWriteBatchRawRecordBasedTimer() {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(-1).ageLimit(10).build())
            .build();
    final int count = 10;
    var writes = new ArrayList<CompletableFuture<String>>(count);
    for (int i = 0; i < count; ++i) {
      var rawRecord = randBytes(100);
      writes.add(producer.write(buildRecord(rawRecord)));
    }
    writes.forEach(
        f -> assertThatNoException().isThrownBy(() -> f.orTimeout(3, TimeUnit.SECONDS).join()));
    writes.forEach(f -> assertThat(f).isCompleted());
    producer.close();
  }

  @Test
  @Timeout(30)
  public void testWriteBatchRawRecordBasedBytesSize() {
    BufferedProducer producer =
        client.newBufferedProducer().stream(randStream(client))
            .batchSetting(
                BatchSetting.newBuilder()
                    .recordCountLimit(100)
                    .ageLimit(-1)
                    .bytesLimit(4096)
                    .build())
            .build();
    final int count = 42;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      var rawRecord = randBytes(100);
      recordIdFutures[i] = producer.write(buildRecord(rawRecord));
    }
    for (int i = 0; i < count - 1; ++i) {
      recordIdFutures[i].join();
    }

    assertThatThrownBy(() -> recordIdFutures[count - 1].get(3, TimeUnit.SECONDS))
        .isInstanceOf(TimeoutException.class);
    producer.close();
    assertThatNoException().isThrownBy(() -> recordIdFutures[count - 1].join());
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
    assertThat(res).size().isEqualTo(pairs.size());
    assertThat(res).containsExactlyInAnyOrderEntriesOf(pairs);
  }

  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() {
    String stream = randStream(client);
    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    String id0 = producer.write(randRawRec()).join();
    assertThat(id0).isNotNull();

    client.deleteStream(stream);
    assertThatThrownBy(() -> producer.write(randRawRec()).join()).isInstanceOf(Exception.class);
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
    assertThat(res).containsExactlyInAnyOrderElementsOf(records);
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
    assertThat(output).containsExactlyInAnyOrderElementsOf(input);
  }
}
