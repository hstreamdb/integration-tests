package io.hstream.testing;

import static io.hstream.testing.Utils.TestUtils.*;

import io.hstream.HStreamClient;
import io.hstream.testing.Utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

public class ClusterExtension implements BeforeEachCallback, AfterEachCallback {

  static final int CLUSTER_SIZE = 3;
  private static final AtomicInteger count = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(ClusterExtension.class);
  private final List<GenericContainer<?>> hservers = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hserverUrls = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hserverInnerUrls = new ArrayList<>(CLUSTER_SIZE);
  private String seedNodes;
  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> rq;
  private GenericContainer<?> hstore;
  private String grp;
  private long beginTime;

  private HStreamClient client;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    beginTime = System.currentTimeMillis();

    grp = UUID.randomUUID().toString();
    printBeginFlag(context);

    dataDir = Files.createTempDirectory("hstream");

    TestUtils.SecurityOptions securityOptions = makeSecurityOptions(context.getTags());

    zk = makeZooKeeper();
    zk.start();
    rq = makeRQLite();
    rq.start();
    String metaHost = "127.0.0.1";
    logger.debug("metaHost: " + metaHost);

    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = hstore.getHost();
    logger.info("hstoreHost: " + hstoreHost);

    List<TestUtils.HServerCliOpts> hserverConfs = new ArrayList<>(CLUSTER_SIZE);
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.incrementAndGet();
      int hserverPort = 6570 + offset;
      int hserverInnerPort = 65000 + offset;
      TestUtils.HServerCliOpts options = new TestUtils.HServerCliOpts();
      options.serverId = offset;
      options.port = hserverPort;
      options.internalPort = hserverInnerPort;
      options.address = "localhost";
      options.metaHost = metaHost;
      options.securityOptions = securityOptions;
      options.hstoreHost = hstoreHost;
      hserverConfs.add(options);
      hserverInnerUrls.add("hserver" + offset + ":" + hserverInnerPort);
      hserverUrls.add("localhost:" + hserverPort);
    }
    seedNodes = hserverInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
    hservers.addAll(bootstrapHServerCluster(hserverConfs, hserverInnerUrls, dataDir));
    Thread.sleep(5000);
    hservers.stream().forEach(h -> logger.info(h.getLogs()));
    // Thread.sleep(50000);
    // hservers.stream().forEach(h -> hserverUrls.add(h.getHost() + ":" + h.getFirstMappedPort()));

    Object testInstance = context.getRequiredTestInstance();
    var initUrl =
        "hstream://" + hserverUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();

    client = makeClient(initUrl, context.getTags());

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHStreamDBUrl", String.class)
                .invoke(testInstance, initUrl));

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setCount", AtomicInteger.class)
                .invoke(testInstance, count));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setSecurityOptions", SecurityOptions.class)
                .invoke(testInstance, securityOptions));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setSeedNodes", String.class)
                .invoke(testInstance, seedNodes));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setDataDir", Path.class)
                .invoke(testInstance, dataDir));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setClient", HStreamClient.class)
                .invoke(testInstance, client));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHServers", List.class)
                .invoke(testInstance, hservers));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHServerUrls", List.class)
                .invoke(testInstance, hserverUrls));

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setLogMsgPathPrefix", String.class)
                .invoke(testInstance, grp));

    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setExtensionContext", ExtensionContext.class)
                .invoke(testInstance, context));
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Thread.sleep(100);

    client.close();
    // waiting for servers to flush logs
    for (int i = 0; i < hservers.size(); i++) {
      var hserver = hservers.get(i);
      writeLog(context, "hserver-" + i, grp, hserver.getLogs());
      hserver.close();
    }

    hservers.clear();
    hserverUrls.clear();
    hserverInnerUrls.clear();
    count.set(0);
    writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();
    writeLog(context, "zk", grp, zk.getLogs());
    zk.close();
    rq.close();

    logger.info("total time is = {}ms", System.currentTimeMillis() - beginTime);
    printEndFlag(context);
  }

  TestUtils.SecurityOptions makeSecurityOptions(Set<String> tags) {
    TestUtils.SecurityOptions options = new TestUtils.SecurityOptions();
    options.dir = getClass().getClassLoader().getResource("security").getPath();
    if (tags.contains("tls")) {
      options.enableTls = true;
      options.keyPath = "/data/security/server.key.pem";
      options.certPath = "/data/security/signed.server.cert.pem";
    }
    if (tags.contains("tls-authentication")) {
      options.caPath = "/data/security/ca.cert.pem";
    }
    return options;
  }
}
