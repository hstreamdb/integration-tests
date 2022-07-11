package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.HStreamClient;
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
  private final List<GenericContainer<?>> hServers = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hServerUrls = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hServerInnerUrls = new ArrayList<>(CLUSTER_SIZE);
  private String seedNodes;
  private Path dataDir;
  private GenericContainer<?> zk;
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
    String zkHost = "127.0.0.1";
    logger.debug("zkHost: " + zkHost);

    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = "127.0.0.1";
    logger.debug("hstoreHost: " + hstoreHost);

    String hServerAddress = "127.0.0.1";
    List<TestUtils.HServerCliOpts> hserverConfs = new ArrayList<>(CLUSTER_SIZE);
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.incrementAndGet();
      int hServerPort = 1234 + offset;
      int hServerInnerPort = 65000 + offset;
      TestUtils.HServerCliOpts options = new TestUtils.HServerCliOpts();
      options.serverId = offset;
      options.port = hServerPort;
      options.internalPort = hServerInnerPort;
      options.address = hServerAddress;
      options.zkHost = zkHost;
      options.securityOptions = securityOptions;
      hserverConfs.add(options);
      hServerUrls.add(hServerAddress + ":" + hServerPort);
      hServerInnerUrls.add(hServerAddress + ":" + hServerInnerPort);
    }
    seedNodes = hServerInnerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
    hServers.addAll(bootstrapHServerCluster(hserverConfs, seedNodes, dataDir));
    hServers.stream().forEach(h -> logger.info(h.getLogs()));
    Thread.sleep(3000);

    Object testInstance = context.getRequiredTestInstance();
    var initUrl = hServerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get();
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
                .invoke(testInstance, hServers));
    silence(
        () ->
            testInstance
                .getClass()
                .getMethod("setHServerUrls", List.class)
                .invoke(testInstance, hServerUrls));

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
    for (int i = 0; i < hServers.size(); i++) {
      var hServer = hServers.get(i);
      writeLog(context, "hserver-" + i, grp, hServer.getLogs());
      hServer.close();
    }

    hServers.clear();
    hServerUrls.clear();
    hServerInnerUrls.clear();
    count.set(0);
    writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();
    writeLog(context, "zk", grp, zk.getLogs());
    zk.close();

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
