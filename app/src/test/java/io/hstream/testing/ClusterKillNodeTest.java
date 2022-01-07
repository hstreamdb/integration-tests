package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.writeLog;

import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterKillNodeTest {

  private final Random random = new Random(System.currentTimeMillis());
  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private String logGrp;
  private ExtensionContext context;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setLogGrp(String logGrp) {
    this.logGrp = logGrp;
  }

  public void setContext(ExtensionContext context) {
    this.context = context;
  }

  private void terminateHServerWithLogs(int turn, int serverId) throws Exception {
    System.out.println("[DEBUG]: terminate HServer" + serverId);
    String logs = hServers.get(serverId).getLogs();
    Assertions.assertNotNull(logs);
    writeLog(context, "hserver-" + serverId + "-turn-" + turn, logGrp, logs);
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("[DEBUG]: hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  @RepeatedTest(5)
  void listStreamAfterKillNodes() {
    String stream = randStream(hStreamClient);
    hServers.get(0).close();
    hServers.get(1).close();
    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());
  }

  @RepeatedTest(5)
  void listSubscriptionAfterKillNodes() {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hServers.get(0).close();
    hServers.get(1).close();
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
  }

  @RepeatedTest(5)
  void write() throws Exception {
    var streamName = TestUtils.randText();
    System.out.println("[DEBUG]: HServer cluster size is " + hServers.size());
    int luckyServer = random.nextInt(hServers.size());
    System.out.println("lucky server is " + luckyServer);
    hStreamClient.createStream(streamName);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    new Thread(
            () -> {
              for (int i = 0; i < hServers.size(); ++i) {
                try {
                  Thread.sleep(1000);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                if (i != luckyServer) {
                  try {
                    terminateHServerWithLogs(0, i);
                  } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                }
              }
            })
        .start();

    for (int i = 0; i < hServers.size() * 20; ++i) {
      System.out.println("ready for writing record " + i);
      var recordId = producer.write(("hello" + i).getBytes(StandardCharsets.UTF_8)).join();
      System.out.println(recordId);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @RepeatedTest(5)
  void testWriteAfterKillStreamHost() throws Exception {
    String stream = randStream(hStreamClient);

    Random rand = new Random();
    byte[] randRecs = new byte[128];

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id0 = producer.write(randRecs).join();

    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);

    producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id1 = producer.write(randRecs).join();

    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }

  @RepeatedTest(5)
  void testRestartNodeJoinCluster() throws Exception {
    terminateHServerWithLogs(0, 2);
    Thread.sleep(10 * 1000);
    String stream = randStream(hStreamClient);
    Thread.sleep(5 * 1000);
    hServers.get(2).start();
    Thread.sleep(15 * 1000);
    terminateHServerWithLogs(0, 0);
    terminateHServerWithLogs(0, 1);
    Thread.sleep(5 * 1000);

    Assertions.assertEquals(stream, hStreamClient.listStreams().get(0).getStreamName());

    Random rand = new Random();
    byte[] randRecs = new byte[128];
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    rand.nextBytes(randRecs);
    RecordId id0 = producer.write(randRecs).join();
    rand.nextBytes(randRecs);
    RecordId id1 = producer.write(randRecs).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);
  }
}
