package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;

import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Stream {
  private static final Logger logger = LoggerFactory.getLogger(Stream.class);
  private HStreamClient client;
  private List<String> hserverUrls;
  private final Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHServerUrls(List<String> hserverUrls) {
    this.hserverUrls = hserverUrls;
  }

  @Test
  @Timeout(60)
  void testCreateStream() {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);
  }

  @Test
  @Timeout(60)
  void testListStreams() {
    Assertions.assertTrue(client.listStreams().isEmpty());
    var streamNames = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      streamNames.add(randStream(client));
    }
    var res =
        client.listStreams().parallelStream()
            .map(io.hstream.Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(streamNames.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testDeleteStreamWithListStreams() {
    final String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);
    final String stream2 = randStream(client);
    createStreamSucceeds(client, 2, stream);
    client.deleteStream(stream);
    deleteStreamSucceeds(client, 1, stream);
    client.deleteStream(stream2, true);
    deleteStreamSucceeds(client, 0, stream);
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistStreamShouldFail() throws Exception {
    Assertions.assertThrows(Throwable.class, () -> client.deleteStream(randText()));
    Assertions.assertThrows(Throwable.class, () -> client.deleteStream(randText(), true));
  }

  @Test
  @Timeout(60)
  void testMultiThreadListStream() throws Throwable {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);
    assertExceptions(runWithThreads(3, client::listStreams));
  }

  @Test
  @Timeout(60)
  void testMultiThreadCreateSameStream() throws Exception {
    String stream = randText();
    var es = runWithThreads(3, () -> client.createStream(stream));
    createStreamSucceeds(client, 1, stream);
    Assertions.assertEquals(2, es.size());
  }

  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);

    var es = runWithThreads(3, () -> client.deleteStream(stream));

    deleteStreamSucceeds(client, 0, stream);
    Assertions.assertEquals(2, es.size());
  }

  @Test
  @Timeout(60)
  void testMultiThreadForceDeleteSameStream() throws Exception {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);

    var es = runWithThreads(3, () -> client.deleteStream(stream, true));

    deleteStreamSucceeds(client, 0, stream);
    Assertions.assertEquals(2, es.size());
  }

  @Test
  @Timeout(60)
  void testDeleteStreamWithSubscription() throws Exception {
    String stream = randStream(client);
    String subscription = randSubscription(client, stream);
    createStreamSucceeds(client, 1, stream);
    Assertions.assertThrows(Exception.class, () -> client.deleteStream(stream));

    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    List<String> records = doProduce(producer, 100, 100);
    List<byte[]> res = new ArrayList<>();
    consume(
        client,
        subscription,
        "c1",
        10,
        (r) -> {
          res.add(r.getRawRecord());
          return res.size() < records.size();
        });
    client.deleteStream(stream, true);
    Assertions.assertEquals(records.size(), res.size());
    deleteStreamSucceeds(client, 0, stream);
  }

  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() throws Exception {
    String stream = randStream(client);
    String stream2 = randStream(client);
    io.hstream.Producer producer = client.newProducer().stream(stream).build();
    io.hstream.Producer producer2 = client.newProducer().stream(stream2).build();
    doProduce(producer, 100, 1);
    doProduce(producer2, 100, 1);
    client.deleteStream(stream);
    deleteStreamSucceeds(client, 1, stream);
    Assertions.assertThrows(Exception.class, () -> doProduce(producer, 100, 1));
    client.deleteStream(stream2, true);
    deleteStreamSucceeds(client, 0, stream2);
    Assertions.assertThrows(Exception.class, () -> doProduce(producer2, 100, 1));
    Thread.sleep(1000);
  }

  @Disabled("HS-1314")
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
    client.deleteStream(stream, true);
    deleteStreamSucceeds(client, 0, stream);

    Thread.sleep(1000);
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
    Assertions.assertEquals(records.size(), res.size());
  }
}
