package io.hstream.testing.Utils;

import static org.assertj.core.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import io.hstream.*;
import io.hstream.CompressionType;
import io.hstream.Producer;
import io.hstream.ReceivedRecord;
import io.hstream.Record;
import io.hstream.Subscription;
import io.hstream.impl.DefaultSettings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.bouncycastle.util.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {

  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
  private static final DockerImageName defaultHStreamImageName =
      DockerImageName.parse("hstreamdb/hstream:latest");
  private static final Network test = Network.newNetwork();

  public static String randText() {
    return "test_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static byte[] randBytes() {
    return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] randBytes(int size) {
    byte[] payload = new byte[size];
    ThreadLocalRandom.current().nextBytes(payload);
    return payload;
  }

  public static Record randRawRec() {
    return buildRecord(randBytes());
  }

  public static Record buildRecord(byte[] xs) {
    return Record.newBuilder().rawRecord(xs).build();
  }

  public static Record buildRecord(HRecord xs) {
    return Record.newBuilder().hRecord(xs).build();
  }

  public static String randStream(HStreamClient c) {
    Random rand = new Random(System.currentTimeMillis());
    int shardCnt = Math.max(1, rand.nextInt(5));
    return randStream(c, shardCnt);
  }

  public static String randStream(HStreamClient c, int shardCnt) {
    String streamName = "test_stream_" + randText();
    c.createStream(streamName, (short) 3, shardCnt);
    return streamName;
  }

  public static String randSubscriptionWithTimeoutAndMaxUnack(
      HStreamClient c, String streamName, int timeout, int maxUnack) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .ackTimeoutSeconds(timeout)
            .maxUnackedRecords(maxUnack)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscriptionWithTimeout(
      HStreamClient c, String streamName, int timeout) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .ackTimeoutSeconds(timeout)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    final String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String randSubscription(
      HStreamClient c, String streamName, Subscription.SubscriptionOffset offset) {
    final String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(offset)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  public static String simpleQuery(HStreamClient c, String streamName, String sql) {
    var queryName = "query_" + UUID.randomUUID().toString();
    var query = c.createQuery(queryName, "CREATE STREAM " + streamName + " AS " + sql);
    logger.info(query.getQueryText() + " has been created");
    return query.getName();
  }

  // -----------------------------------------------------------------------------------------------

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse("zookeeper:3.8"))
        .withNetwork(test)
        .withNetworkAliases("zookeeper");
  }

  public static GenericContainer<?> makeRQLite() {
    return new GenericContainer<>(DockerImageName.parse("rqlite/rqlite"))
        .withNetwork(test)
        .withNetworkAliases("rqlite");
  }

  private static DockerImageName getHStreamImageName() {
    String hstreamImageName = System.getenv("HSTREAM_IMAGE_NAME");
    if (hstreamImageName == null || hstreamImageName.equals("")) {
      logger.info(
          "No env variable HSTREAM_IMAGE_NAME found, use default name {}", defaultHStreamImageName);
      return defaultHStreamImageName;
    } else {
      logger.info("Found env variable HSTREAM_IMAGE_NAME = {}", hstreamImageName);
      return DockerImageName.parse(hstreamImageName);
    }
  }

  private static String getHStreamMetaStorePreference(String metaHost) {
    String hstreamMetaStore = System.getenv("HSTREAM_META_STORE");
    if (hstreamMetaStore == null
        || hstreamMetaStore.equals("")
        || hstreamMetaStore.equalsIgnoreCase("ZOOKEEPER")) {
      logger.info("Use default Zookeeper HSTREAM_META_STORE");
      return "zk://zookeeper:2181";
    } else if (hstreamMetaStore.equalsIgnoreCase("RQLITE")) {
      logger.info("HSTREAM_META_STORE specified RQLITE as meta store");
      return "rq://rqlite:4001";
    } else {
      throw new RuntimeException("Invalid HSTREAM_META_STORE env variable value");
    }
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer<>(getHStreamImageName())
        .withNetwork(test)
        .withNetworkAliases("hstore")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "$(hostname -I | awk '{print $1}') "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static class SecurityOptions {
    public String dir;
    public boolean enableTls;
    public String keyPath;
    public String certPath;
    public String caPath;

    @Override
    public String toString() {
      String msg = "";
      if (enableTls) {
        msg +=
            " --enable-tls " + " --tls-key-path=" + keyPath + " --tls-cert-path=" + certPath + " ";
      }
      if (caPath != null) {
        msg += " --tls-ca-path=" + caPath + " ";
      }
      return msg;
    }
  }

  public static FixedHostPortGenericContainer<?> makeHServer(
      HServerCliOpts hserverConf, String seedNodes, Path dataDir) {
    return new FixedHostPortGenericContainer<>(getHStreamImageName().toString())
        .withNetwork(test)
        .withNetworkAliases("hserver" + hserverConf.serverId)
        .withFixedExposedPort(hserverConf.port, hserverConf.port)
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withFileSystemBind(hserverConf.securityOptions.dir, "/data/security", BindMode.READ_ONLY)
        .withCommand(
            "bash", "-c", " hstream-server" + hserverConf.toString() + " --seed-nodes " + seedNodes)
        .waitingFor(Wait.forLogMessage(".*Server is started on port.*", 1));
  }

  public static class HServerCliOpts {
    public int serverId;
    public String address;
    public int port;
    public int internalPort;
    public String metaHost;
    public String hstoreHost = "127.0.0.1";

    public SecurityOptions securityOptions;

    public String toString() {
      return " --bind-address "
          + "0.0.0.0 "
          + " --port "
          + port
          + " --internal-port "
          + internalPort
          + " --advertised-address "
          + address
          + " --gossip-address $(hostname -I | awk '{print $1}')"
          + " --server-id "
          + serverId
          + " --metastore-uri "
          + getHStreamMetaStorePreference(metaHost)
          + " --store-config "
          + "/data/hstore/logdevice.conf "
          + " --store-admin-host hstore "
          + " --store-admin-port "
          + "6440"
          + " --log-level "
          + "debug"
          + securityOptions
          + " --log-with-color"
          + " --store-log-level "
          + "error";
    }
  }

  public static List<GenericContainer<?>> bootstrapHServerCluster(
      List<HServerCliOpts> hserverConfs, List<String> hserverInnerUrls, Path dataDir)
      throws IOException, InterruptedException {
    List<GenericContainer<?>> hservers = new ArrayList<>();
    for (HServerCliOpts hserverConf : hserverConfs) {
      logger.info(hserverInnerUrls.toString());
      hserverInnerUrls.remove("hserver" + hserverConf.serverId + ":" + hserverConf.internalPort);
      var seedNodes =
          hserverInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get()
              + ",$(hostname -I | awk '{print $1}'):"
              + hserverConf.internalPort;
      hserverInnerUrls.add("hserver" + hserverConf.serverId + ":" + hserverConf.internalPort);

      logger.info(seedNodes);
      var hserver = makeHServer(hserverConf, seedNodes, dataDir);
      hservers.add(hserver);
    }
    hservers.stream().parallel().forEach(GenericContainer::start);
    var res =
        hservers
            .get(0)
            .execInContainer(
                "bash",
                "-c",
                "hstream --host 127.0.0.1 "
                    // + hserverConfs.get(0).address
                    + " --port "
                    + hserverConfs.get(0).port
                    + " init ");
    return hservers;
  }

  // -----------------------------------------------------------------------------------------------

  public static void writeLog(ExtensionContext context, String entryName, String grp, String logs)
      throws Exception {
    String testClassName = context.getRequiredTestClass().getSimpleName();
    String testName = context.getTestMethod().get().getName();
    String filePathFromProject =
        ".logs/" + testClassName + "/" + testName + "/" + grp + "/" + entryName;
    logger.info("log to " + filePathFromProject);
    String fileName = "../" + filePathFromProject;

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(logs);
    writer.close();
  }

  // -----------------------------------------------------------------------------------------------

  public static void createStreamSucceeds(HStreamClient client, int sizeExpected, String stream) {
    List<io.hstream.Stream> streams = client.listStreams();
    assertThat(streams.size()).isEqualTo(sizeExpected);
    assertThat(streams.stream().map(io.hstream.Stream::getStreamName).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(stream);
  }

  public static void deleteStreamSucceeds(HStreamClient client, int sizeExpected, String stream) {
    List<io.hstream.Stream> streams = client.listStreams();
    Assertions.assertEquals(sizeExpected, streams.size());
    Assertions.assertFalse(
        streams.stream()
            .map(io.hstream.Stream::getStreamName)
            .collect(Collectors.toList())
            .contains(stream));
  }

  public static Function<ReceivedRawRecord, Boolean> handleForKeysSync(
      HashMap<String, RecordsPair> pairs, int count) {
    var received = new AtomicInteger(0);
    return receiveNRawRecords(count, pairs, received);
  }

  public static Function<ReceivedRawRecord, Boolean> handleForKeys(
      HashMap<String, RecordsPair> pairs, CountDownLatch latch) {
    return r -> {
      synchronized (pairs) {
        var key = r.getHeader().getPartitionKey();
        var succ =
            pairs
                .computeIfAbsent(key, v -> new RecordsPair())
                .insertWithoutDuplicated(r.getRecordId(), Arrays.toString(r.getRawRecord()));
        if (succ) latch.countDown();
        return latch.getCount() > 0;
      }
    };
  }

  public static List<String> doProduce(Producer producer, int payloadSize, int recordsNums) {
    return produce(producer, payloadSize, recordsNums).records;
  }

  public static List<String> doProduceAndGatherRid(
      Producer producer, int payloadSize, int recordsNums) {
    return produce(producer, payloadSize, recordsNums).ids;
  }

  public static class RecordsPair {
    public List<String> ids;
    public List<String> records;

    public RecordsPair() {
      ids = new ArrayList<>();
      records = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RecordsPair that = (RecordsPair) o;
      return Objects.equals(ids, that.ids) && Objects.equals(records, that.records);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ids, records);
    }

    @Override
    public String toString() {
      return "RecordsPair{" + "ids count=" + ids.size() + ", records count=" + records.size() + '}';
    }

    public void extend(RecordsPair other) {
      ids.addAll(other.ids);
      records.addAll(other.records);
    }

    public void insert(String id, String record) {
      ids.add(id);
      records.add(record);
    }

    public boolean insertWithoutDuplicated(String id, String record) {
      if (!ids.contains(id)) {
        ids.add(id);
        records.add(record);
        return true;
      }
      return false;
    }
  }

  public static RecordsPair produce(Producer producer, int payloadSize, int count) {
    return produce(producer, payloadSize, count, DefaultSettings.DEFAULT_PARTITION_KEY);
  }

  public static HashMap<String, RecordsPair> produce(
      Producer producer, int payloadSize, int totalCount, int keysSize) {
    return produce(producer, payloadSize, totalCount, new RobinRoundKeyGenerator(keysSize));
  }

  public static RecordsPair produce(Producer producer, int payloadSize, int count, String key) {
    return produce(producer, payloadSize, count, () -> key).get(key);
  }

  @FunctionalInterface
  public interface KeyGenerator {
    String get();
  }

  public static class RobinRoundKeyGenerator implements KeyGenerator {
    private final ArrayList<String> keys;
    private int idx;

    RobinRoundKeyGenerator(ArrayList<String> keys) {
      this.keys = keys;
    }

    RobinRoundKeyGenerator(int keysSize) {
      this.keys =
          (ArrayList<String>)
              IntStream.range(0, keysSize)
                  .mapToObj(k -> "test_key_" + k)
                  .collect(Collectors.toList());
    }

    @Override
    public String get() {
      var key = keys.get(idx);
      idx = (idx + 1) % keys.size();
      return key;
    }
  }

  public static class RandomKeyGenerator implements KeyGenerator {
    private final ArrayList<String> keys;
    private final Random rand = new Random();

    RandomKeyGenerator(ArrayList<String> keys) {
      this.keys = keys;
    }

    public RandomKeyGenerator(int keysSize) {
      this.keys =
          (ArrayList<String>)
              IntStream.range(0, keysSize)
                  .mapToObj(k -> "test_key_" + k)
                  .collect(Collectors.toList());
    }

    @Override
    public String get() {
      return keys.get(rand.nextInt(keys.size()));
    }
  }

  public static HashMap<String, RecordsPair> produce(
      Producer producer, int payloadSize, int totalCount, KeyGenerator kg) {
    assert totalCount > 0;
    assert payloadSize > 0;
    Random rand = new Random();
    byte[] rRec = new byte[payloadSize];
    var records = new HashMap<String, ArrayList<String>>();
    var futures = new HashMap<String, List<CompletableFuture<String>>>();
    for (int i = 0; i < totalCount; i++) {
      var key = kg.get();
      rand.nextBytes(rRec);
      Record recordToWrite = Record.newBuilder().partitionKey(key).rawRecord(rRec).build();
      if (!futures.containsKey(key)) {
        futures.put(key, new ArrayList<>());
        records.put(key, new ArrayList<>());
      }
      futures.get(key).add(producer.write(recordToWrite));
      records.get(key).add(Arrays.toString(rRec));
    }

    var res = new HashMap<String, RecordsPair>();
    futures.forEach(
        (key, v) -> {
          RecordsPair p = new RecordsPair();
          p.records = records.get(key);
          var ids = new ArrayList<String>();
          v.forEach(x -> ids.add(x.join()));
          p.ids = ids;
          res.put(key, p);
        });

    StringBuilder builder = new StringBuilder();
    var ids = new HashMap<String, Integer>();
    var total = res.values().stream().map(x -> x.ids.size()).reduce(0, Integer::sum);
    logger.info("total write records: {}", total);
    for (var entry : res.entrySet()) {
      builder
          .append(("key: "))
          .append(entry.getKey())
          .append(", keysize: ")
          .append(entry.getValue().ids.size())
          .append(", ids: {");
      for (var id : entry.getValue().ids) {
        builder.append(id).append(", ");
        var items = Strings.split(id, '-');

        var k = items[0] + '-' + items[1];
        if (!ids.containsKey(k)) {
          ids.put(k, 0);
        }
        ids.put(k, ids.get(k) + 1);
      }
      builder.append("}\n");
    }
    //    logger.info("================ produce result ================");
    //    logger.info("{}", builder);
    //    logger.info("================================================");
    //
    //    logger.info("total writes");
    //    StringBuilder builder1 = new StringBuilder();
    //    for (var entry : ids.entrySet()) {
    //      builder1.append("key: ").append(entry.getKey()).append(", count:
    // ").append(entry.getValue()).append("\n");
    //    }
    //    logger.info("{}", builder1);
    return res;
  }

  public static HashMap<String, RecordsPair> produce(
      Producer producer, AtomicInteger value, int totalCount, KeyGenerator kg) {
    assert totalCount > 0;
    var records = new HashMap<String, LinkedList<String>>();
    var futures = new HashMap<String, List<CompletableFuture<String>>>();
    for (int i = 0; i < totalCount; i++) {
      var key = kg.get();
      var thisValue = value.getAndIncrement();
      ByteBuffer b = ByteBuffer.allocate(4);
      b.putInt(thisValue);
      byte[] rRec = b.array();
      Record recordToWrite = Record.newBuilder().partitionKey(key).rawRecord(rRec).build();
      if (!futures.containsKey(key)) {
        futures.put(key, new LinkedList<>());
        records.put(key, new LinkedList<>());
      }
      futures.get(key).add(producer.write(recordToWrite));
      ByteBuffer wrapped = ByteBuffer.wrap(rRec);
      int thisNum = wrapped.getInt();
      records.get(key).add(String.valueOf(thisNum));
    }

    var res = new HashMap<String, RecordsPair>();
    futures.forEach(
        (key, v) -> {
          RecordsPair p = new RecordsPair();
          p.records = records.get(key);
          var ids = new LinkedList<String>();
          v.forEach(x -> ids.add(x.join()));
          p.ids = ids;
          res.put(key, p);
        });
    return res;
  }

  public static HashMap<String, RecordsPair> produce(
      Producer producer, AtomicInteger value, int totalCount, int keysSize) {
    return produce(producer, value, totalCount, new RobinRoundKeyGenerator(keysSize));
  }

  public static RecordsPair produce(Producer producer, AtomicInteger value, int count, String key) {
    return produce(producer, value, count, () -> key).get(key);
  }

  public static RecordsPair produce(Producer producer, AtomicInteger value, int count) {
    return produce(producer, value, count, DefaultSettings.DEFAULT_PARTITION_KEY);
  }

  @FunctionalInterface
  public interface RecordGenerator {
    Record get();
  }

  public static class RandomSizeRecordGenerator implements RecordGenerator {
    int beg;
    int end;
    Random rand = new Random();

    public RandomSizeRecordGenerator(int begIncluded, int endExcluded) {
      assert begIncluded > 0 && endExcluded > begIncluded;
      this.beg = begIncluded;
      this.end = endExcluded;
    }

    @Override
    public Record get() {
      var size = rand.nextInt(end - beg) + beg;
      byte[] rRec = new byte[size];
      return Record.newBuilder()
          .rawRecord(rRec)
          .partitionKey(DefaultSettings.DEFAULT_PARTITION_KEY)
          .build();
    }
  }

  public static HashMap<String, RecordsPair> produce(
      Producer producer, int count, RecordGenerator rg) {
    var records = new ArrayList<String>();
    var futures = new HashMap<String, List<CompletableFuture<String>>>();
    for (int i = 0; i < count; i++) {
      var record = rg.get();
      var key = record.getPartitionKey();
      if (record.isRawRecord()) {
        records.add(Arrays.toString(record.getRawRecord()));
      } else {
        records.add(record.getHRecord().toString());
      }
      if (!futures.containsKey(key)) {
        futures.put(key, new LinkedList<>());
      }
      futures.get(key).add(producer.write(record));
    }

    var res = new HashMap<String, RecordsPair>();
    futures.forEach(
        (key, v) -> {
          RecordsPair p = new RecordsPair();
          p.records = records;
          var ids = new LinkedList<String>();
          v.forEach(x -> ids.add(x.join()));
          p.ids = ids;
          res.put(key, p);
        });
    return res;
  }

  public static BufferedProducer makeBufferedProducer(HStreamClient client, String streamName) {
    return client.newBufferedProducer().stream(streamName).build();
  }

  public static BufferedProducer makeBufferedProducer(
      HStreamClient client, String streamName, int batchRecordLimit) {
    BatchSetting batchSetting =
        BatchSetting.newBuilder().recordCountLimit(batchRecordLimit).build();
    return client.newBufferedProducer().stream(streamName).batchSetting(batchSetting).build();
  }

  public static BufferedProducer makeBufferedProducer(
      HStreamClient client, String streamName, int batchRecordLimit, CompressionType tp) {
    BatchSetting batchSetting =
        BatchSetting.newBuilder().recordCountLimit(batchRecordLimit).ageLimit(100).build();
    return client.newBufferedProducer().stream(streamName)
        .batchSetting(batchSetting)
        .compressionType(tp)
        .build();
  }

  public static void restartServer(GenericContainer<?> server) throws Exception {
    Thread.sleep(1000);
    server.close();
    Thread.sleep(5000); // need time to let zk clear old data
    logger.info("begin restart!");
    try {
      if (server.isRunning()) Thread.sleep(2000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
    } catch (ContainerLaunchException e) {
      logger.info("start hserver failed, try another restart.");
      server.close();
      Thread.sleep(5000);
      server.withStartupTimeout(Duration.ofSeconds(5)).start();
      Thread.sleep(2000);
    }
  }

  private static void printFlag(String flag, ExtensionContext context) {
    logger.info(
        "=====================================================================================");
    logger.info(
        "{} {} {} {}",
        flag,
        context.getRequiredTestInstance().getClass().getSimpleName(),
        context.getTestMethod().get().getName(),
        context.getDisplayName());
    logger.info(
        "=====================================================================================");
  }

  public static void printBeginFlag(ExtensionContext context) {
    printFlag("begin", context);
  }

  public static void printEndFlag(ExtensionContext context) {
    printFlag("end", context);
  }

  public static boolean diffAndLogResultSetsWithoutDuplicated(
      HashMap<String, TestUtils.RecordsPair> l, HashMap<String, TestUtils.RecordsPair> r) {
    if (!l.keySet().equals(r.keySet())) {
      logger.info("keySet is not same, l:{}, r:{}", l.keySet(), r.keySet());
      return false;
    }
    for (var k : l.keySet()) {
      var lids = new HashSet<>(l.get(k).ids);
      var rids = new HashSet<>(r.get(k).ids);
      if (!lids.equals(rids)) {
        logger.info("key:{}, ids is not same \n l:{} \n r:{}", k, lids, rids);
        return false;
      }
      var lrecords = new HashSet<>(l.get(k).records);
      var rrecords = new HashSet<>(r.get(k).records);
      if (!lrecords.equals(rrecords)) {
        logger.info("key:{}, records is not same \n l:{} \n r:{}", k, lrecords, rrecords);
        return false;
      }
    }
    return true;
  }

  public static boolean diffAndLogResultSets(
      HashMap<String, TestUtils.RecordsPair> l, HashMap<String, TestUtils.RecordsPair> r) {
    if (!l.keySet().equals(r.keySet())) {
      logger.info("keySet is not same, l:{}, r:{}", l.keySet(), r.keySet());
      return false;
    }
    for (var k : l.keySet()) {
      if (l.get(k).ids.size() != r.get(k).ids.size()) {
        logger.info(
            "key:{}, ids is not same \n l:{} \n r:{}", k, l.get(k).ids.size(), r.get(k).ids.size());
        return false;
      }
      if (l.get(k).records.size() != r.get(k).records.size()) {
        logger.info(
            "key:{}, records is not same \n l:{} \n r:{}",
            k,
            r.get(k).records.size(),
            r.get(k).records.size());
        return false;
      }
      // check order
      // if (!l.get(k).ids.equals(r.get(k).ids)) {
      //   logger.info("key:{}, ids is not same \n l:{} \n r:{}", k, l.get(k).ids, r.get(k).ids);
      //   return false;
      // }
      // if (!l.get(k).records.equals(r.get(k).records)) {
      //   logger.info(
      //       "key:{}, records is not same \n l:{} \n r:{}", k, r.get(k).records,
      // r.get(k).records);
      //   return false;
      // }
    }
    return true;
  }

  public static ArrayList<String> generateKeysIncludingDefaultKey(int size) {
    assert size > 0;
    var res = new ArrayList<String>(size);
    res.add(DefaultSettings.DEFAULT_PARTITION_KEY);
    for (int i = 1; i < size; i++) {
      res.add("test_key_" + i);
    }
    return res;
  }

  public static HStreamClient makeClient(String url, Set<String> tags) {
    logger.info("hStreamDBUrl " + url);
    HStreamClientBuilder builder = HStreamClient.builder().serviceUrl(url);
    var securityPath = TestUtils.class.getClassLoader().getResource("security").getPath();
    if (tags.contains("tls")) {
      builder = builder.enableTls().tlsCaPath(securityPath + "/ca.cert.pem");
    }
    if (tags.contains("tls-authentication")) {
      builder =
          builder
              .enableTlsAuthentication()
              .tlsKeyPath(securityPath + "/role.key-pk8.pem")
              .tlsCertPath(securityPath + "/signed.role.cert.pem");
    }
    return builder.build();
  }

  @FunctionalInterface
  public interface ThrowableRunner {
    void run() throws Throwable;
  }

  public static void silence(ThrowableRunner r) {
    try {
      r.run();
    } catch (Throwable e) {
      logger.info("ignored exception:{}", e.getMessage());
    }
  }

  // run with multi threads, and return the exceptions
  public static List<Throwable> runWithThreads(int threadCount, ThrowableRunner runner)
      throws InterruptedException {
    assert threadCount > 0;
    var es = new LinkedList<Throwable>();
    var ts = new ArrayList<Thread>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      ts.add(
          new Thread(
              () -> {
                try {
                  runner.run();
                } catch (Throwable e) {
                  synchronized (es) {
                    es.add(e);
                  }
                }
              }));
    }
    ts.forEach(Thread::start);
    for (var t : ts) {
      t.join();
    }
    return es;
  }

  public static void assertExceptions(List<Throwable> es) throws Throwable {
    if (es != null && !es.isEmpty()) {
      logger.info("caught exceptions:{}", es);
      throw es.get(0);
    }
  }

  public static <T> List<Throwable> waitFutures(List<ListenableFuture<T>> fs) {
    var es = new LinkedList<Throwable>();
    for (var f : fs) {
      try {
        f.get();
      } catch (Throwable e) {
        es.add(e);
      }
    }
    return es;
  }

  public static Throwable getRootCause(Throwable e) {
    Objects.requireNonNull(e);
    Throwable res = e;
    while (res.getCause() != null && res.getCause() != res) {
      res = res.getCause();
    }
    return res;
  }

  public static void assertShardId(List<String> ids) {
    assertThat(ids.stream().map(s -> Strings.split(s, '-')[0]).distinct().count()).isEqualTo(1);
  }

  public static HashMap<String, RecordsPair> batchAppendConcurrentlyWithRandomKey(
      BufferedProducer producer,
      int threadCount,
      int recordCnt,
      int payloadSize,
      RandomKeyGenerator keys)
      throws InterruptedException {
    var produced = new HashMap<String, TestUtils.RecordsPair>();
    runWithThreads(
        threadCount,
        () -> {
          var pairs = produce(producer, payloadSize, recordCnt, keys);
          synchronized (produced) {
            pairs.forEach(
                (k, v) -> produced.computeIfAbsent(k, value -> new RecordsPair()).extend(v));
          }
        });
    return produced;
  }

  public static Function<ReceivedRawRecord, Boolean> receiveNRawRecords(
      int count, HashMap<String, RecordsPair> received, AtomicInteger receivedCount) {
    return receivedRawRecord -> {
      if (receivedCount.get() >= count) {
        return false;
      }
      var key = receivedRawRecord.getHeader().getPartitionKey();
      var succ =
          received
              .computeIfAbsent(key, v -> new RecordsPair())
              .insertWithoutDuplicated(
                  receivedRawRecord.getRecordId(),
                  Arrays.toString(receivedRawRecord.getRawRecord()));
      if (succ) {
        return receivedCount.incrementAndGet() < count;
      }
      return receivedCount.get() < count;
    };
  }

  @Deprecated
  public static List<ReceivedRecord> readStreamShards(
      HStreamClient client,
      int ShardCnt,
      String streamName,
      int totalRead,
      ArrayList<Thread> rids,
      StreamShardOffset offset) {
    var shards = client.listShards(streamName);

    var terminate = new AtomicBoolean(false);
    var readRes = new ArrayList<ReceivedRecord>();
    for (int i = 0; i < ShardCnt; i++) {
      var reader =
          client
              .newReader()
              .readerId("reader_" + i)
              .streamName(streamName)
              .shardId(shards.get(i).getShardId())
              .shardOffset(offset)
              .timeoutMs(100)
              .build();
      var rid =
          new Thread(
              () -> {
                while (!terminate.get()) {
                  reader
                      .read(1000)
                      .thenApply(
                          records -> {
                            synchronized (readRes) {
                              readRes.addAll(records);
                              if (readRes.size() >= totalRead) {
                                terminate.set(true);
                              }
                            }
                            return null;
                          });

                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      rids.add(rid);
    }

    return readRes;
  }
}
