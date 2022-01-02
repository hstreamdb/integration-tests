package io.hstream.testing;

import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.SubscriptionOffset;
import io.hstream.SubscriptionOffset.SpecialOffset;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {
  public static String randText() {
    return "test_stream_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static String randStream(HStreamClient c) {
    String streamName = randText();
    c.createStream(streamName, (short) 3);
    return streamName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(new SubscriptionOffset(SpecialOffset.LATEST))
            .ackTimeoutSeconds(10)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  // -----------------------------------------------------------------------------------------------

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer(DockerImageName.parse("zookeeper")).withNetworkMode("host");
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer(DockerImageName.parse("hstreamdb/hstream:latest"))
        // .withNetwork(network)
        .withNetworkMode("host")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "127.0.0.1 "
                // + "$(hostname -I | cut -f1 -d' ') "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static GenericContainer<?> makeHServer(
      String address,
      int port,
      int internalPort,
      Path dataDir,
      String zkHost,
      String hstoreHost,
      int serverId) {
    return new GenericContainer(DockerImageName.parse("hstreamdb/hstream:v0.6.0"))
        .withNetworkMode("host")
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withCommand(
            "bash",
            "-c",
            " hstream-server"
                + " --host "
                + "127.0.0.1 "
                + " --port "
                + port
                + " --internal-port "
                + internalPort
                + " --address "
                + address
                + " --server-id "
                + serverId
                + " --zkuri "
                + zkHost
                + ":2181"
                + " --store-config "
                + "/data/hstore/logdevice.conf "
                + " --store-admin-host "
                + hstoreHost
                + " --store-admin-port "
                + "6440")
        .waitingFor(Wait.forLogMessage(".*Server started on port.*", 1));
  }

  // -----------------------------------------------------------------------------------------------

  public static String trimMethodName(String methodName) {
    return methodName.substring(0, methodName.indexOf('('));
  }

  public static void writeLog(ExtensionContext context, String entryName, String logs)
      throws Exception {
    String testClassName = context.getRequiredTestClass().getSimpleName();
    String testName = trimMethodName(context.getDisplayName());
    String fileName = "../.logs/" + testClassName + "/" + testName + "/" + entryName;

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    PrintWriter printWriter = new PrintWriter(file);
    printWriter.println(logs);
    printWriter.close();
  }
}
