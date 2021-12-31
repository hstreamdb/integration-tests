package io.hstream.testing;

import io.hstream.HStreamClient;
import io.hstream.Stream;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicClusterTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
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

  // -----------------------------------------------------------------------------------------------

  @Test
  void testConnections() throws Exception {

    for (var hServerUrl : hServerUrls) {
      System.out.println(hServerUrl);
      try (HStreamClient client = HStreamClient.builder().serviceUrl(hServerUrl).build()) {
        List<Stream> res = client.listStreams();
        Assertions.assertTrue(res.isEmpty());
      }
    }
  }
}
