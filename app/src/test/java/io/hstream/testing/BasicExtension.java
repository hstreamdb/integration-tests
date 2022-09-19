package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

public class BasicExtension implements BeforeEachCallback, AfterEachCallback {

  private static final Logger logger = LoggerFactory.getLogger(BasicExtension.class);
  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> rq;
  private GenericContainer<?> hstore;
  private GenericContainer<?> hserver;
  private long beginTime;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    beginTime = System.currentTimeMillis();
    printBeginFlag(context);

    dataDir = Files.createTempDirectory("hstream");

    zk = makeZooKeeper();
    zk.start();
    rq = makeRQLite();
    rq.start();

    String zkHost = "127.0.0.1";
    logger.debug("zkHost: " + zkHost);
    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = "127.0.0.1";
    logger.debug("hstoreHost: " + hstoreHost);
    String hServerAddress = "127.0.0.1";
    int hServerPort = 6570;
    int hServerInternalPort = 65000;
    TestUtils.HServerCliOpts options = new TestUtils.HServerCliOpts();
    options.serverId = 0;
    options.address = hServerAddress;
    options.port = hServerPort;
    options.internalPort = hServerInternalPort;
    options.metaHost = zkHost;
    hserver = makeHServer(options, hServerAddress + ":" + hServerInternalPort, dataDir);
    hserver.start();
    Thread.sleep(1000);
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
    String grp = UUID.randomUUID().toString();

    writeLog(context, "hserver", grp, hserver.getLogs());
    hserver.close();

    writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();

    writeLog(context, "zk", grp, zk.getLogs());
    zk.close();
    writeLog(context, "rq", grp, rq.getLogs());
    rq.close();

    hserver = null;
    hstore = null;
    zk = null;
    dataDir = null;
    rq = null;

    logger.info("total time is = {}ms", System.currentTimeMillis() - beginTime);
    printEndFlag(context);
  }
}
