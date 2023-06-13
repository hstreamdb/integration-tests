package io.hstream.testing.Utils;

import static io.hstream.testing.Utils.TestUtils.randText;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Service;
import io.hstream.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerService {
  private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);
  static Executor executor = Executors.newSingleThreadExecutor();

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private Consumer consumer;
  private String consumerName;

  private final CompletableFuture<Void> future = new CompletableFuture<>();

  public static void consume(
      HStreamClient client,
      String subscription,
      long timeoutSeconds,
      Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, randText(), handle, null, null);
    consumer.consumeAsync(timeoutSeconds, TimeUnit.SECONDS);
  }

  public static void consume(
      HStreamClient client,
      String subscription,
      String name,
      Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, name, handle, null, null);
    consumer.consumeAsync();
  }

  public static void consume(
      HStreamClient client,
      String subscription,
      String name,
      long timeoutSeconds,
      Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, name, handle, null, null);
    consumer.consumeAsync(timeoutSeconds, TimeUnit.SECONDS);
  }

  public static void consume(
      HStreamClient client,
      String subscription,
      String name,
      long timeoutSeconds,
      Function<ReceivedRawRecord, Boolean> handle,
      Function<ReceivedHRecord, Boolean> handleHRecord) {
    var consumer = new ConsumerService(client, subscription, name, handle, handleHRecord, null);
    consumer.consumeAsync(timeoutSeconds, TimeUnit.SECONDS);
  }

  public static ConsumerService startConsume(
      HStreamClient client, String subscription, Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, randText(), handle, null, null);
    consumer.consumeAsync();
    return consumer;
  }

  public static ConsumerService startConsume(
      HStreamClient client,
      String subscription,
      String name,
      Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, name, handle, null, null);
    consumer.consumeAsync();
    return consumer;
  }

  public static ConsumerService startConsume(
      HStreamClient client,
      String subscription,
      String name,
      Function<ReceivedRawRecord, Boolean> handle,
      Function<ReceivedHRecord, Boolean> handleHRecord,
      java.util.function.Consumer<Responder> handleRespond) {
    var consumer =
        new ConsumerService(client, subscription, name, handle, handleHRecord, handleRespond);
    consumer.consumeAsync();
    return consumer;
  }

  public static ConsumerService startConsume(
      HStreamClient client,
      String subscription,
      String name,
      long timeout,
      Function<ReceivedRawRecord, Boolean> handle,
      Function<ReceivedHRecord, Boolean> handleHRecord) {
    var consumer = new ConsumerService(client, subscription, name, handle, handleHRecord, null);
    consumer.consumeAsync(timeout, TimeUnit.SECONDS);
    return consumer;
  }

  public ConsumerService(
      HStreamClient client,
      String subscription,
      String name,
      Function<ReceivedRawRecord, Boolean> handle,
      Function<ReceivedHRecord, Boolean> handleHRecord,
      java.util.function.Consumer<Responder> handleResponder) {
    consumer = mkConsumer(client, subscription, name, handle, handleHRecord, handleResponder);
  }

  public void consumeAsync() {
    consumer.startAsync().awaitRunning();
    future.whenCompleteAsync(
        (x, e) -> {
          if (e != null) {
            logger.error("consumer {} failed", consumerName, e);
            throw new RuntimeException(e);
          }
          consumer.stopAsync().awaitTerminated();
        },
        executor);
  }

  public static CompletableFuture<Void> waitConsume(
      HStreamClient client,
      String subscription,
      String consumerName,
      Function<ReceivedRawRecord, Boolean> handle) {
    var consumer = new ConsumerService(client, subscription, consumerName, handle, null, null);
    consumer.consumeAsync();
    return consumer.getFuture();
  }

  private CompletableFuture<Void> getFuture() {
    return future;
  }

  public void consumeAsync(long timeout, TimeUnit unit) {
    consumer.startAsync().awaitRunning();
    try {
      future.get(timeout, unit);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } finally {
      stopped.set(true);
      consumer.stopAsync().awaitTerminated();
    }
  }

  public void start() {
    consumer.startAsync().awaitRunning();
  }

  public void stop() {
    stopped.set(true);
    consumer.stopAsync().awaitTerminated();
    logger.info("===== consumer {} stopped ======", consumerName);
  }

  private Consumer mkConsumer(
      HStreamClient client,
      String subscription,
      String name,
      Function<ReceivedRawRecord, Boolean> handle,
      Function<ReceivedHRecord, Boolean> handleHRecord,
      java.util.function.Consumer<Responder> handleResponder) {
    BiConsumer<Object, Responder> process =
        (receivedRecord, responder) -> {
          if (stopped.get()) {
            logger.info("===== consumer {} already stopped ======", name);
            return;
          }
          if (handleResponder != null) {
            handleResponder.accept(responder);
          } else {
            responder.ack();
          }
          try {
            boolean consumeNext =
                receivedRecord instanceof ReceivedRawRecord
                    ? handle.apply((ReceivedRawRecord) receivedRecord)
                    : handleHRecord.apply((ReceivedHRecord) receivedRecord);
            if (!consumeNext) {
              stopped.set(true);
              future.complete(null);
            }
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        };
    consumerName = name;

    consumer =
        client
            .newConsumer()
            .subscription(subscription)
            .name(name)
            .rawRecordReceiver(process::accept)
            .hRecordReceiver(process::accept)
            .build();

    consumer.addListener(
        new Service.Listener() {
          @Override
          public void terminated(@NotNull Service.State from) {
            future.complete(null);
          }

          @Override
          public void failed(@NotNull Service.State from, @NotNull Throwable failure) {
            future.completeExceptionally(failure);
          }
        },
        Executors.newSingleThreadExecutor());
    return consumer;
  }

  // -----------------------------------------------------------------------------------------------
  // start an async consumer and wait until received first record
  public static Consumer activateSubscription(HStreamClient client, String subscription)
      throws Exception {
    var latch = new CountDownLatch(1);
    var c =
        client
            .newConsumer()
            .subscription(subscription)
            .rawRecordReceiver(
                (x, y) -> {
                  logger.info(x.toString());
                  latch.countDown();
                })
            .build();
    c.startAsync().awaitRunning();
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    logger.info("consumer activated");
    return c;
  }

  public static Consumer createConsumerCollectStringPayload(
      HStreamClient client,
      String subscription,
      String name,
      List<String> records,
      CountDownLatch latch,
      ReentrantLock lock) {
    return client
        .newConsumer()
        .subscription(subscription)
        .name(name)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              lock.lock();
              records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
              lock.unlock();
              responder.ack();
              latch.countDown();
            })
        .build();
  }
}
