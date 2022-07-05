package io.hstream.testing;

import static io.hstream.testing.TestUtils.assertGrpcException;
import static io.hstream.testing.TestUtils.createStreamSucceeds;
import static io.hstream.testing.TestUtils.deleteStreamSucceeds;
import static io.hstream.testing.TestUtils.randAppendRequest;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randText;
import static io.hstream.testing.TestUtils.waitFutures;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.hstream.HStreamClient;
import io.hstream.internal.AppendRequest;
import io.hstream.internal.DeleteStreamRequest;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.HStreamRecord;
import io.hstream.internal.ListStreamsRequest;
import io.hstream.internal.LookupStreamRequest;
import io.hstream.internal.ServerNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("server")
@ExtendWith(ClusterExtension.class)
public class ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(ClusterTest.class);
  private HStreamClient client;
  private List<String> hServerUrls;
  private final Random globalRandom = new Random();
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  private HStreamApiGrpc.HStreamApiFutureStub getStub(String url) {
    for (int i = 0; i < hServerUrls.size(); i++) {
      if (hServerUrls.get(i).equals(url)) {
        return stubs.get(i);
      }
    }
    return null;
  }

  private HStreamApiGrpc.HStreamApiFutureStub getStub(ServerNode node) {
    return getStub(node.getHost() + ":" + node.getPort());
  }

  @BeforeEach
  public void setup() {
    for (var url : hServerUrls) {
      var ss = url.split(":");
      var channel =
          ManagedChannelBuilder.forAddress(ss[0], Integer.parseInt(ss[1])).usePlaintext().build();
      channels.add(channel);
      stubs.add(HStreamApiGrpc.newFutureStub(channel));
    }
  }

  @AfterEach
  public void teardown() {
    channels.forEach(ManagedChannel::shutdown);
  }

  // ----------------------------------------------------------------------------
  // Stream
  @Test
  @Timeout(60)
  void testMultiThreadListStream() throws Exception {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);
    var fs =
        stubs.stream()
            .map(s -> s.listStreams(ListStreamsRequest.newBuilder().build()))
            .collect(Collectors.toList());
    for (var f : fs) {
      Assertions.assertEquals(f.get().getStreamsList().size(), 1);
    }
  }

  @Test
  @Timeout(60)
  void testMultiThreadCreateSameStream() throws Exception {
    String stream = randText();

    var req =
        io.hstream.internal.Stream.newBuilder()
            .setStreamName(stream)
            .setReplicationFactor(1)
            .build();
    var fs = stubs.stream().map(s -> s.createStream(req)).collect(Collectors.toList());
    var es = waitFutures(fs);
    Assertions.assertEquals(hServerUrls.size() - 1, es.size());
    createStreamSucceeds(client, 1, stream);
  }

  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);

    var req = DeleteStreamRequest.newBuilder().setStreamName(stream).build();
    var fs = stubs.stream().map(s -> s.deleteStream(req)).collect(Collectors.toList());
    var es = waitFutures(fs);

    deleteStreamSucceeds(client, 0, stream);
    Assertions.assertEquals(hServerUrls.size() - 1, es.size());
  }

  @Test
  @Timeout(60)
  void testMultiThreadForceDeleteSameStream() throws Exception {
    String stream = randStream(client);
    createStreamSucceeds(client, 1, stream);

    var req = DeleteStreamRequest.newBuilder().setStreamName(stream).build();
    var fs = stubs.stream().map(s -> s.deleteStream(req)).collect(Collectors.toList());
    var es = waitFutures(fs);

    deleteStreamSucceeds(client, 0, stream);
    Assertions.assertEquals(hServerUrls.size() - 1, es.size());
  }

  @Test
  @Timeout(60)
  void testCreateThenDeleteStreamFromDifferentServerUrl() throws Exception {
    var stream = randStream(stubs.get(0)).get().getStreamName();
    stubs.get(1).deleteStream(DeleteStreamRequest.newBuilder().setStreamName(stream).build()).get();
    Assertions.assertThrows(
        Exception.class,
        () ->
            stubs
                .get(2)
                .deleteStream(DeleteStreamRequest.newBuilder().setStreamName(stream).build())
                .get());
  }

  // ---------------------------------------------------------------------
  // LookupStream
  @Test
  @Timeout(60)
  void testLookupFromDiffServer() throws Throwable {
    var stream = randStream(stubs.get(0)).get().getStreamName();
    Assertions.assertNotNull(
        stubs
            .get(1)
            .lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build())
            .get()
            .getServerNode());
  }

  @Test
  @Timeout(60)
  void testLookupDeletedStream() throws Throwable {
    var stream = randStream(stubs.get(0)).get().getStreamName();
    stubs.get(1).deleteStream(DeleteStreamRequest.newBuilder().setStreamName(stream).build()).get();
    Assertions.assertThrows(
        Exception.class,
        () ->
            stubs
                .get(2)
                .lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build())
                .get());
  }

  // --------------------------------------------------------------------------------------------
  // Append
  @Disabled("INVALID_ARGUMENT")
  @Test
  @Timeout(60)
  void testAppendEmptyRequest() throws Throwable {
    assertGrpcException(
        Status.INVALID_ARGUMENT,
        () -> stubs.get(0).append(AppendRequest.getDefaultInstance()).get());
  }

  @Disabled("INVALID_ARGUMENT")
  @Test
  @Timeout(60)
  void testAppendInvalidRecord() throws Throwable {
    var stub = stubs.get(0);
    var stream = randStream(stub).get().getStreamName();
    var node =
        stub.lookupStream(LookupStreamRequest.newBuilder().setStreamName(stream).build())
            .get()
            .getServerNode();
    var appendStub = getStub(node);
    assertGrpcException(
        Status.INVALID_ARGUMENT,
        () ->
            appendStub
                .append(
                    AppendRequest.newBuilder()
                        .setStreamName(stream)
                        .addRecords(HStreamRecord.newBuilder().build())
                        .build())
                .get());
  }

  @Test
  @Timeout(60)
  void testAppendWithoutLookup() throws Throwable {
    var stream = randStream(stubs.get(0)).get().getStreamName();
    var fs =
        stubs.stream()
            .map(stub -> stub.append(randAppendRequest(stream)))
            .collect(Collectors.toList());
    Assertions.assertEquals(stubs.size() - 1, waitFutures(fs).size());
  }
}
