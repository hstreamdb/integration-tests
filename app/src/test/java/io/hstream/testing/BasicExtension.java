package io.hstream.testing;

import static io.hstream.testing.TestUtils.makeHServer;
import static io.hstream.testing.TestUtils.makeHStore;
import static io.hstream.testing.TestUtils.makeZooKeeper;
import static io.hstream.testing.TestUtils.redirectStdErr;
import static io.hstream.testing.TestUtils.writeLog;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

public class BasicExtension implements BeforeEachCallback, AfterEachCallback {

  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> hstore;
  private GenericContainer<?> hserver;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    redirectStdErr(context);

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
    int hServerPort = 6570;
    int hServerInnerPort = 65000;
    hserver =
        makeHServer(hServerAddress, hServerPort, hServerInnerPort, dataDir, zkHost, hstoreHost, 0);
    hserver.start();
    Thread.sleep(100);
    Object testInstance = context.getRequiredTestInstance();
    testInstance
        .getClass()
        .getMethod("setHStreamDBUrl", String.class)
        .invoke(testInstance, hServerAddress + ":" + hServerPort);
    testInstance
        .getClass()
        .getMethod("setServer", GenericContainer.class)
        .invoke(testInstance, hserver);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {

    writeLog(context, "hserver", hserver.getLogs());
    hserver.close();

    writeLog(context, "hstore", hstore.getLogs());
    hstore.close();

    writeLog(context, "zk", zk.getLogs());
    zk.close();

    hserver = null;
    hstore = null;
    zk = null;
    dataDir = null;
  }
}
