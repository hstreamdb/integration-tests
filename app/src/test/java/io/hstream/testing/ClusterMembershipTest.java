package io.hstream.testing;

import static io.hstream.testing.ClusterExtension.CLUSTER_SIZE;
import static io.hstream.testing.TestUtils.*;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.hstream.internal.DescribeClusterResponse;
import io.hstream.internal.HStreamApiGrpc;
import io.hstream.internal.ServerNode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
public class ClusterMembershipTest {

  private static final Logger logger = LoggerFactory.getLogger(ClusterMembershipTest.class);
  private List<String> hServerUrls;
  private List<GenericContainer<?>> hServers;
  private static AtomicInteger count;
  private Path dataDir;
  private static SecurityOptions securityOptions;
  private String seedNodes;
  private final List<HStreamApiGrpc.HStreamApiFutureStub> stubs = new ArrayList<>();
  private final List<ManagedChannel> channels = new ArrayList<>();

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setSeedNodes(String seedNodes) {
    this.seedNodes = seedNodes;
  }

  public void setDataDir(Path dataDir) {
    this.dataDir = dataDir;
  }

  public void setSecurityOptions(SecurityOptions securityOptions) {
    this.securityOptions = securityOptions;
  }

  public void setCount(AtomicInteger count) {
    this.count = count;
  }

  private HStreamApiGrpc.HStreamApiFutureStub getStub(ServerNode node) {
    return getStub(node.getHost() + ":" + node.getPort());
  }

  private String doGetToString(ListenableFuture<DescribeClusterResponse> resp) {
    try {
      return resp.get().toString();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private HStreamApiGrpc.HStreamApiFutureStub getStub(String url) {
    for (int i = 0; i < hServerUrls.size(); i++) {
      if (hServerUrls.get(i).equals(url)) {
        return stubs.get(i);
      }
    }
    return null;
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

  @Test
  @Timeout(60)
  void testClusterBootStrap() throws Exception {
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(this::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
    }
  }

  @Test
  @Timeout(60)
  void testSingleNodeJoin() throws Exception {
    var options = makeHServerCliOpts(count, securityOptions);
    var newServer = makeHServer(options, seedNodes, dataDir);
    newServer.start();

    var req = Empty.newBuilder().build();
    var newNode =
        ServerNode.newBuilder()
            .setId(options.serverId)
            .setHost(options.address)
            .setPort(options.port)
            .build();
    Thread.sleep(1000);
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(this::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE + 1, f.get().getServerNodesCount());
      Assertions.assertTrue(f.get().getServerNodesList().contains(newNode));
    }

    newServer.close();
  }

  @Test
  @Timeout(60)
  void testMultipleNodesJoin() throws Exception {
    var newNodesNum = 3;
    var newNodesInternalUrls = new ArrayList<String>();
    var newServers = new ArrayList<GenericContainer>();
    var newNodes = new ArrayList<ServerNode>();
    for (int i = 0; i < newNodesNum; ++i) {
      var options = makeHServerCliOpts(count, securityOptions);
      var newServer = makeHServer(options, seedNodes, dataDir);
      newServers.add(newServer);
      newNodes.add(options.toNode());
      newNodesInternalUrls.add(options.address + ":" + options.port);
    }
    newServers.stream().parallel().forEach(GenericContainer::start);

    for (var url : newNodesInternalUrls) {
      var ss = url.split(":");
      var channel =
          ManagedChannelBuilder.forAddress(ss[0], Integer.parseInt(ss[1])).usePlaintext().build();
      channels.add(channel);
      stubs.add(HStreamApiGrpc.newFutureStub(channel));
    }

    Thread.sleep(3000);
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    logger.info(fs.stream().map(this::doGetToString).collect(Collectors.toList()).toString());
    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE + newNodesNum, f.get().getServerNodesCount());
      Assertions.assertTrue(f.get().getServerNodesList().containsAll(newNodes));
    }

    newServers.stream().parallel().forEach(GenericContainer::close);
  }

  @Test
  @Timeout(60)
  void testSingleNodeLeave() throws Exception {
    var rand = new Random();
    var req = Empty.newBuilder().build();
    var fs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());

    var index = rand.nextInt(CLUSTER_SIZE - 1);
    var leavingNode = hServers.get(index);

    for (var f : fs) {
      Assertions.assertEquals(CLUSTER_SIZE, f.get().getServerNodesCount());
      //      Assertions.assertTrue(f.get().getServerNodesList().contains(leavingNode));
    }

    leavingNode.close();
    hServers.remove(index);
    stubs.remove(index);

    Thread.sleep(10000);
    var gs = stubs.stream().map(s -> s.describeCluster(req)).collect(Collectors.toList());
    for (var g : gs) {
      Assertions.assertEquals(CLUSTER_SIZE - 1, g.get().getServerNodesCount());
      //      Assertions.assertFalse(f.get().getServerNodesList().contains(leavingNode));
    }
  }
}
