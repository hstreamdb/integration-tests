package io.hstream.testing;

import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.randBytes;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ScaleTest {

  private static final Logger logger = LoggerFactory.getLogger(ScaleTest.class);
  private final Random random = new Random(System.currentTimeMillis());
  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private String logMsgPathPrefix;
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

  public void setLogMsgPathPrefix(String logMsgPathPrefix) {
    this.logMsgPathPrefix = logMsgPathPrefix;
  }

  public void setExtensionContext(ExtensionContext context) {
    this.context = context;
  }

  @BeforeEach
  public void setup() throws Exception {
    logger.debug(" hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @RepeatedTest(10)
  @Timeout(120)
  void testLargeNumProducer() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int total = 64;

    List<RecordId> recordIds0 = new ArrayList<>();
    ReentrantLock lock = new ReentrantLock();

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < total; ++i) {
      Thread thread =
          new Thread(
              () -> {
                Producer producer = hStreamClient.newProducer().stream(stream).build();
                for (int j = 0; j < total; ++j) {
                  lock.lock();
                  try {
                    recordIds0.add(producer.write(randBytes()).join());
                  } catch (Throwable e) {
                    logger.info("========e {}", e.getMessage());
                    e.printStackTrace();
                    recordIds0.add(producer.write(randBytes()).join());
                  }
                  lock.unlock();
                }
              });
      thread.start();
      threads.add(thread);
    }
    for (var x : threads) {
      x.join();
    }

    Set<RecordId> set0 = new HashSet<>(recordIds0);
    Set<RecordId> set1 = new HashSet<>();
    CountDownLatch countDown1 = new CountDownLatch(total * total);
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recv.ack();
                  if (set1.add(recs.getRecordId())) {
                    countDown1.countDown();
                  }
                })
            .build();

    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDown1.await(90, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();

    Assertions.assertEquals(set0, set1);
    Assertions.assertEquals(total * total, recordIds0.size());
  }

  @Test
  @Timeout(120)
  void testLargeNumConsumer() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int total = 64;

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    List<RecordId> recordIds0 = new ArrayList<>();
    for (int i = 0; i < total; ++i) {
      recordIds0.add(
          producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    }

    AtomicReference<Throwable> err = new AtomicReference<>();
    err.set(null);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < total; ++i) {
      Thread thread =
          new Thread(
              () -> {
                Set<RecordId> recordIds1 = new HashSet<>();
                CountDownLatch countDown = new CountDownLatch(total);
                Consumer consumer =
                    hStreamClient
                        .newConsumer()
                        .subscription(subscription)
                        .rawRecordReceiver(
                            (recs, recv) -> {
                              if (recordIds1.add(recs.getRecordId())) {
                                countDown.countDown();
                              }
                            })
                        .build();
                consumer.startAsync().awaitRunning();
                try {
                  Assertions.assertTrue(countDown.await(60, TimeUnit.SECONDS));
                } catch (Throwable e) {
                  if (err.get() != null) {
                    err.set(e);
                  }
                }
                consumer.stopAsync().awaitTerminated();
                try {
                  Assertions.assertEquals(new HashSet<>(recordIds0), recordIds1);
                } catch (Throwable e) {
                  if (err.get() != null) {
                    err.set(e);
                  }
                }
              });
      thread.start();
      threads.add(thread);
    }
    for (var x : threads) {
      x.join();
    }
    Assertions.assertNull(err.get());
  }

  @Test
  @Timeout(120)
  void testLargeNumSubscription() throws Exception {
    final String stream = randStream(hStreamClient);
    final int total = 64;
    final int msgCntForEachCase = total * 2;

    String[] subscriptions = new String[total];
    for (int i = 0; i < total; ++i) {
      subscriptions[i] = randSubscription(hStreamClient, stream);
    }

    Producer producer = hStreamClient.newProducer().stream(stream).build();
    List<RecordId> recordIds0 = new ArrayList<>();
    for (int i = 0; i < msgCntForEachCase; ++i) {
      recordIds0.add(
          producer.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)).join());
    }

    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      int finalI = i;
      Thread thread =
          new Thread(
              () -> {
                List<RecordId> recordIds1 = new ArrayList<>();
                CountDownLatch countDown = new CountDownLatch(msgCntForEachCase);
                Consumer consumer =
                    hStreamClient
                        .newConsumer()
                        .subscription(subscriptions[finalI])
                        .rawRecordReceiver(
                            (recs, recv) -> {
                              recordIds1.add(recs.getRecordId());
                              recv.ack();
                              countDown.countDown();
                            })
                        .build();
                consumer.startAsync().awaitRunning();
                try {
                  Assertions.assertTrue(countDown.await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                consumer.stopAsync().awaitTerminated();
                Assertions.assertEquals(recordIds0, recordIds1);
              });
      thread.start();
      threads.add(thread);
    }
    for (var x : threads) {
      x.join();
    }
  }

  @Test
  @Timeout(60)
  void testLargeNumBatch() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    final int batchSize = 512;

    Producer producer =
        hStreamClient.newProducer().stream(stream)
            .enableBatch()
            .recordCountLimit(batchSize)
            .build();
    List<String> recs0 = doProduce(producer, 4, 2048);

    CountDownLatch countDown = new CountDownLatch(2048);
    List<String> recs1 = new ArrayList<>();
    Consumer consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (recs, recv) -> {
                  recs1.add(Arrays.toString(recs.getRawRecord()));
                  recv.ack();
                  countDown.countDown();
                })
            .build();

    consumer.startAsync().awaitRunning();
    Assertions.assertTrue(countDown.await(60, TimeUnit.SECONDS));
    consumer.stopAsync().awaitTerminated();

    Assertions.assertEquals(recs0, recs1);
  }
}
