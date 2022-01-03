package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionFromEarliest;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.HStreamClient;
import io.hstream.Stream;
import io.hstream.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(BasicExtension.class)
class BasicTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private GenericContainer<?> server;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setServer(GenericContainer<?> s) {
    this.server = s;
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("db url: " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
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
    server.close();
    Thread.sleep(5000); // need time to let zk clear old data
    server.start();
    var streams = hStreamClient.listStreams();
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    var subscriptions = hStreamClient.listSubscriptions();
    Assertions.assertEquals(subscription, subscriptions.get(0).getSubscriptionId());
  }
}
