package io.hstream.testing;

import io.hstream.HStreamClient;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterKillNodeTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private Random random = new Random(System.currentTimeMillis());

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  private void terminateHServer(int serverId) {
    hServers.get(serverId).close();
    System.out.println("[DEBUG]: terminate HServer " + String.valueOf(serverId));
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

  @Test
  void listStreamAfterKillNode() {
    hServers.get(0).close();
    hStreamClient.listStreams();
  }

  @Test
  // @RepeatedTest(5)
  void write() {
    var streamName = TestUtils.randText();
    System.out.println("hserver cluster size is " + hServers.size());
    int luckyServer = random.nextInt(hServers.size());
    // int luckyServer = 2;
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
                  terminateHServer(i);
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
}
