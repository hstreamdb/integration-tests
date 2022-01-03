package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionFromEarliest;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.HStreamClient;
import io.hstream.Stream;
import io.hstream.Subscription;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

  // -----------------------------------------------------------------------------------------------

  @Test
  void testCreateStream() {
    final String streamName = randText();
    hStreamClient.createStream(streamName);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  void testDeleteStream() {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    hStreamClient.deleteStream(streamName);
    streams = hStreamClient.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  void testDeleteNonExistingStreamShouldFail() {
    Assertions.assertThrows(Exception.class, () -> hStreamClient.deleteStream("aaa"));
  }

  @Test
  void testListStreams() {
    Assertions.assertTrue(hStreamClient.listStreams().isEmpty());
    var streamNames = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      streamNames.add(randStream(hStreamClient));
    }
    var res =
        hStreamClient.listStreams().parallelStream()
            .map(Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(streamNames.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  void testListSubscriptions() {
    String streamName = randStream(hStreamClient);
    var subscriptions = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      subscriptions.add(randSubscription(hStreamClient, streamName));
    }
    var res =
        hStreamClient.listSubscriptions().parallelStream()
            .map(Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(subscriptions.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  void testCreateConsumerWithoutSubscriptionShouldFail() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> hStreamClient.newConsumer().name("test-consumer").build());
  }

  @Disabled
  @Test
  void testDeleteNonExistingSubscriptionShouldFail() {
    Assertions.assertThrows(Exception.class, () -> hStreamClient.deleteSubscription("aaa"));
  }

  @Test
  void testGetResourceAfterRestartServer() throws InterruptedException {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscriptionFromEarliest(hStreamClient, streamName);
    for (var server : hServers) {
      server.close();
      Thread.sleep(5000); // need time to let zk clear old data
      server.start();
    }
    Thread.sleep(100);
    var streams = hStreamClient.listStreams();
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    var subscriptions = hStreamClient.listSubscriptions();
    Assertions.assertEquals(subscription, subscriptions.get(0).getSubscriptionId());
  }

  // -----------------------------------------------------------------------------------------------

  void randomTerminate() throws Exception {
    ArrayList<Integer> xs = new ArrayList<Integer>();
    for (int i = 0; i < 5; i++) {
      xs.add(i);
    }
    Collections.shuffle(xs);
    hServers.get(xs.get(0)).close();
    hServers.get(xs.get(1)).close();
    Thread.sleep(100);
  }

  void testCreateStreamAfterTermination() throws Exception {
    randomTerminate();
    testCreateStream();
  }

  void testDeleteStreamAfterTermination() throws Exception {
    randomTerminate();
    testDeleteStream();
  }

  void testListStreamsAfterTermination() throws Exception {
    randomTerminate();
    testListStreams();
  }

  void testListSubscriptionsAfterTermination() throws Exception {
    randomTerminate();
    testListSubscriptions();
  }
}
