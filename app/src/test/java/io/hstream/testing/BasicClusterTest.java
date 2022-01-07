package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import io.hstream.Stream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicClusterTest {

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

  @Test
  void testMultiThreadListStream() throws Exception {
    randStream(hStreamClient);

    ExecutorService executor = Executors.newCachedThreadPool();
    for (String hServerUrl : hServerUrls) {
      executor.execute(
          () -> {
            HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();
            Assertions.assertNotNull(c.listStreams());
          });
    }
  }

  @Test
  void testMultiThreadCreateSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randText();

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.createStream(stream);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  @Test
  void createDeleteStreamFromNodes() throws Exception {
    ArrayList<HStreamClient> clients = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      clients.add(HStreamClient.builder().serviceUrl(hServerUrl).build());
    }
    String stream = randStream(clients.get(0));
    clients.get(1).deleteStream(stream);
    for (int i = 2; i < clients.size(); i++) {
      int finalI = i;
      Assertions.assertThrows(Exception.class, () -> clients.get(finalI).deleteStream(stream));
    }
  }

  @Test
  void testMultiThreadDeleteSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randStream(hStreamClient);

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.deleteStream(stream);
                } catch (Exception e) {
                  exceptions.add(e);
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  @Test
  void testWriteRawReadFromNodes() throws Exception {
    Random rand = new Random();
    byte[] randRecs = new byte[128];
    rand.nextBytes(randRecs);

    HStreamClient hStreamClient1 = HStreamClient.builder().serviceUrl(hServerUrls.get(1)).build();
    String stream = randStream(hStreamClient1);
    hStreamClient1.close();

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    producer.write(randRecs);

    String subscription = randSubscription(hStreamClient, stream);
    HStreamClient hStreamClient2 = HStreamClient.builder().serviceUrl(hServerUrls.get(2)).build();
    Consumer consumer =
        hStreamClient2
            .newConsumer()
            .name("test-newConsumer-" + UUID.randomUUID())
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, receiver) -> {
                  Assertions.assertEquals(randRecs, recs.getRawRecord());
                  receiver.ack();
                })
            .build();

    consumer.startAsync().awaitRunning(5, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();
  }

  @Test
  void testWriteFromNodesSeq() throws Exception {
    Random rand = new Random();
    byte[] randBytes = new byte[128];
    final String stream = randStream(hStreamClient);

    for (String hServerUrl : hServerUrls) {
      HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();
      rand.nextBytes(randBytes);
      Producer producer = c.newProducer().stream(stream).build();
      producer.write(randBytes);
    }

    ArrayList<RecordId> recIds = new ArrayList<>();

    String subscription = randSubscription(hStreamClient, stream);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .name("test-newConsumer-" + UUID.randomUUID())
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recIds.add(recs.getRecordId());
                  recv.ack();
                })
            .build();

    consumer.startAsync().awaitRunning(10, TimeUnit.SECONDS);
    consumer.stopAsync().awaitTerminated();

    ArrayList<RecordId> recIdsSeq = new ArrayList<>(recIds);
    recIdsSeq = (ArrayList<RecordId>) recIdsSeq.stream().sorted().collect(Collectors.toList());
    Assertions.assertEquals(recIdsSeq, recIds);
  }
}
