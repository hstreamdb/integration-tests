package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.of;

import io.hstream.HServerException;
import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class StreamTest {
  private static final Logger logger = LoggerFactory.getLogger(StreamTest.class);
  private HStreamClient client;
  private List<String> hserverUrls;

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHServerUrls(List<String> hserverUrls) {
    this.hserverUrls = hserverUrls;
  }

  @Test
  @Timeout(60)
  void testStreamBasicOperation() {
    var rand = new Random(System.currentTimeMillis());
    var s1 = "test_stream_" + randText();
    assertThatNoException().isThrownBy(() -> client.createStream(s1));
    var s2 = "test_stream_" + randText();
    assertThatNoException().isThrownBy(() -> client.createStream(s2, (short) 1));
    var s3 = "test_stream_" + randText();
    assertThatNoException()
        .isThrownBy(() -> client.createStream(s3, (short) 1, Math.max(1, rand.nextInt(5))));
    var s4 = "test_stream_" + randText();
    assertThatNoException()
        .isThrownBy(() -> client.createStream(s4, (short) 1, Math.max(1, rand.nextInt(5)), 100));
    var streamNames = Arrays.asList(s1, s2, s3, s4);

    var res =
        client.listStreams().stream()
            .map(io.hstream.Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    assertThat(res).size().isEqualTo(streamNames.size());
    assertThat(res)
        .containsExactlyElementsOf(streamNames.stream().sorted().collect(Collectors.toList()));

    for (String streamName : streamNames) {
      client.deleteStream(streamName);
    }
    assertThat(client.listStreams()).isEmpty();
  }

  @Deprecated
  @ParameterizedTest
  @MethodSource("testCases")
  void testInvalidCreateStreamOptionsShouldFail(
      String streamName, short replicationFactor, int shardCount, int backlogDuration) {
    assertThatThrownBy(
            () -> client.createStream(streamName, replicationFactor, shardCount, backlogDuration))
        .isInstanceOf(Exception.class);
  }

  static Stream<Arguments> testCases() {
    return Stream.of(
        of("", (short) 1, 1, 100), of("abc", (short) 1, -1, 100), of("abc", (short) 1, 1, -1));
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistStreamShouldFail() throws Exception {
    assertThatThrownBy(() -> client.deleteStream(randText())).isInstanceOf(HServerException.class);
    assertThatThrownBy(() -> client.deleteStream(randText(), true))
        .isInstanceOf(HServerException.class);
  }

  @Test
  @Timeout(60)
  void testMultiThreadCreateSameStream() throws Exception {
    String stream = randText();
    var es = runWithThreads(3, () -> client.createStream(stream));
    createStreamSucceeds(client, 1, stream);
    assertThat(es).hasSize(2);
  }

  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    String stream = randStream(client);
    var es = runWithThreads(3, () -> client.deleteStream(stream));
    assertThat(client.listStreams()).isEmpty();
    assertThat(es).hasSize(2);

    String stream1 = randStream(client);
    es = runWithThreads(3, () -> client.deleteStream(stream1, true));
    assertThat(client.listStreams()).isEmpty();
    assertThat(es).hasSize(2);
  }

  @Test
  @Timeout(60)
  void testDeleteStreamWithSubscription() throws Exception {
    String stream = randStream(client);
    assertThatNoException().isThrownBy(() -> randSubscription(client, stream));
    assertThatThrownBy(() -> client.deleteStream(stream)).isInstanceOf(HServerException.class);
    client.deleteStream(stream, true);
    assertThat(client.listStreams()).isEmpty();
  }

  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() throws Exception {
    String stream = randStream(client);
    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    assertThatNoException().isThrownBy(() -> doProduce(producer, 100, 1));

    client.deleteStream(stream);
    assertThatThrownBy(() -> doProduce(producer, 100, 1)).isInstanceOf(Exception.class);
    //    Thread.sleep(1000);
  }

  @Disabled("HS-3496，HS-3497")
  @Test
  @Timeout(60)
  void testCreateANewStreamWithSameNameAfterDeletion() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscription(client, stream);
    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    doProduce(producer, 100, 10);
    activateSubscription(client, subscription);
    client.deleteStream(stream, true);
    deleteStreamSucceeds(client, 0, stream);

    client.createStream(stream);
    createStreamSucceeds(client, 1, stream);
    String subscription2 = randSubscription(client, stream);
    doProduce(producer, 100, 10);
    activateSubscription(client, subscription2);
    Thread.sleep(100);
    client.deleteStream(stream, true);
    deleteStreamSucceeds(client, 0, stream);
  }

  @Test
  @Timeout(60)
  void testResumeSubscriptionOnForceDeletedStream() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscriptionWithTimeout(client, stream, 5);
    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    List<String> records = doProduce(producer, 100, 100);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          synchronized (res) {
            res.add(r.getRawRecord());
          }
          ;
          return res.size() < records.size() / 2;
        });
    assertThatNoException().isThrownBy(() -> client.deleteStream(stream, true));

    //    Thread.sleep(1000);
    consume(
        client,
        subscription,
        "c2",
        10,
        (r) -> {
          synchronized (res) {
            res.add(r.getRawRecord());
          }
          ;
          return res.size() < records.size();
        });
    assertThat(res.size()).isEqualTo(records.size());
  }
}
