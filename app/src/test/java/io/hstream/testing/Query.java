package io.hstream.testing;

import static io.hstream.testing.TestUtils.*;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.hstream.*;
import io.hstream.Consumer;
import io.hstream.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Query {
  private static final Logger logger = LoggerFactory.getLogger(Query.class);
  private HStreamClient client;
  private List<String> hServerUrls;
  private final Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  @Test
  void testCreateQuery() throws Exception {
    String source = randStream(client);
    String sink = "test_sink_stream_" + randText();
    String qid = simpleQuery(client, sink, "select * from " + source + " ;");

    var qids =
        client.listQueries().stream().map(io.hstream.Query::getId).collect(Collectors.toList());
    Assertions.assertTrue(qids.contains(qid));
    String subSs = randSubscription(client, source);
    String subSk = randSubscription(client, sink);
    var producer = client.newProducer().stream(source).build();
    List<String> records = new ArrayList<>();

    HRecord hRecord =
        HRecord.newBuilder()
            // Number
            .put("id", 10)
            // Boolean
            .put("isReady", true)
            // List
            .put("targets", HArray.newBuilder().add(1).add(2).add(3).build())
            // String
            .put("name", "hRecord-example")
            .build();

    Thread.sleep(5000);
    for (int i = 0; i < 10; i++) {
      Record record = Record.newBuilder().hRecord(hRecord).build();
      producer.write(record);
      records.add(record.toString());
      logger.info(record.toString());
    }
    List<HRecord> res = new ArrayList<>();
    HRecordReceiver receiver1 =
        ((h, responder) -> {
          responder.ack();
          res.add(h.getHRecord());
        });

    Consumer consumer1 =
        client
            .newConsumer()
            .subscription(subSk)
            .name("consumer_1")
            .hRecordReceiver(receiver1)
            .build();

    consumer1.startAsync().awaitRunning();
    try {
      consumer1.awaitTerminated(5, SECONDS);
    } catch (TimeoutException e) {
      consumer1.stopAsync().awaitTerminated();
    }

    logger.info("records size = " + records.size());
    logger.info("res size = " + res.size());
    Assertions.assertEquals(records.size(), res.size());
  }
}
