package io.hstream.testing;

import static io.hstream.testing.TestUtils.makeHServer;
import static io.hstream.testing.TestUtils.makeHStore;
import static io.hstream.testing.TestUtils.makeZooKeeper;
import static io.hstream.testing.TestUtils.writeLog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

public class ClusterExtension implements BeforeEachCallback, AfterEachCallback {

  static final int CLUSTER_SIZE = 3;
  private final List<GenericContainer<?>> hServers = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hServerUrls = new ArrayList<>(CLUSTER_SIZE);
  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> hstore;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    dataDir = Files.createTempDirectory("hstream");

    zk = makeZooKeeper();
    zk.start();
    String zkHost = "127.0.0.1";
    System.out.println("[DEBUG]: zkHost: " + zkHost);

    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = "127.0.0.1";
    System.out.println("[DEBUG]: hstoreHost: " + hstoreHost);

    String hServerAddress = "127.0.0.1";
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int hServerPort = 6570 + i;
      int hServerInnerPort = 65000 + i;
      var hServer =
          makeHServer(
              hServerAddress, hServerPort, hServerInnerPort, dataDir, zkHost, hstoreHost, i);
      hServer.start();
      hServers.add(hServer);
      hServerUrls.add(hServerAddress + ":" + hServerPort);

      // System.out.println(hServer.getLogs());
    }
    Thread.sleep(100);

    Object testInstance = context.getRequiredTestInstance();
    testInstance
        .getClass()
        .getMethod("setHStreamDBUrl", String.class)
        .invoke(testInstance, hServerUrls.stream().reduce((url1, url2) -> url1 + "," + url2).get());
    testInstance.getClass().getMethod("setHServers", List.class).invoke(testInstance, hServers);
    testInstance
        .getClass()
        .getMethod("setHServerUrls", List.class)
        .invoke(testInstance, hServerUrls);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    for (int i = 0; i < hServers.size(); i++) {
      var hServer = hServers.get(i);
      writeLog(context, "hserver-" + i, hServer.getLogs());
      hServer.close();
    }
    writeLog(context, "hstore", hstore.getLogs());
    hstore.close();
    writeLog(context, "zk", zk.getLogs());
    zk.close();
  }
}
