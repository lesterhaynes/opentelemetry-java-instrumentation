package io.opentelemetry.javaagent.instrumentation.kafkaclients.v0_11;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.kafka.internal.KafkaConsumerContext;
import io.opentelemetry.instrumentation.kafka.internal.KafkaConsumerContextUtil;
import io.opentelemetry.instrumentation.kafka.internal.KafkaProcessRequest;
import io.opentelemetry.javaagent.bootstrap.kafka.KafkaClientsConsumerProcessTracing;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static io.opentelemetry.javaagent.instrumentation.kafkaclients.v0_11.KafkaSingletons.consumerProcessInstrumenter;


@SuppressWarnings("serial")
public final class TracingQueue<K, V> extends LinkedBlockingQueue<ConsumerRecord<K, V>> {
  private final LinkedBlockingQueue<ConsumerRecord<K, V>> delegate;

  @Nullable
  private KafkaProcessRequest currentRequest;
  @Nullable
  private Context currentContext;
  @Nullable
  private Scope currentScope;
  private TracingQueue(LinkedBlockingQueue<ConsumerRecord<K, V>> delegate) {
    this.delegate = delegate;
  }

  public static <K, V> LinkedBlockingQueue<ConsumerRecord<K, V>> wrap(
      LinkedBlockingQueue<ConsumerRecord<K, V>> delegate) {
    System.out.println("Are you ready to wrap?");
    return new TracingQueue<>(delegate);
  }

  @Override
  public boolean add(ConsumerRecord<K, V> e) {
    return delegate.add(e);
  }

  @Override
  public boolean offer(ConsumerRecord<K, V> e) {
    return delegate.offer(e);
  }

  @Override
  public boolean offer(ConsumerRecord<K, V> e, long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.offer(e, timeout, unit);
  }

  @Override
  public ConsumerRecord<K, V> remove() {
    return delegate.remove();
  }

  @Override
  public boolean remove(Object o) {
    return delegate.remove(o);
  }

  @Override
  public void put(ConsumerRecord<K, V> e) throws InterruptedException {
    delegate.put(e);
  }

  @Override
  public ConsumerRecord<K, V> poll() {
    closeScopeAndEndSpan();
    ConsumerRecord<K, V> next = delegate.poll();
    this.handlePoll(next);
    return next;
  }

  @Override
  public ConsumerRecord<K, V> poll(long timeout, TimeUnit unit) throws InterruptedException {
    closeScopeAndEndSpan();
    ConsumerRecord<K, V> next = delegate.poll(timeout, unit);
    this.handlePoll(next);
    return next;
  }

  @Override
  public ConsumerRecord<K, V> element() {
    return delegate.element();
  }

  @Override
  public ConsumerRecord<K, V> peek() {
    return delegate.peek();
  }

  @Override
  public ConsumerRecord<K, V> take() throws InterruptedException {
    closeScopeAndEndSpan();
    ConsumerRecord<K, V> next = delegate.take();
    this.handlePoll(next);
    return next;
  }

  private void handlePoll(ConsumerRecord<K, V> record) {
    if (record != null && KafkaClientsConsumerProcessTracing.wrappingEnabled()) {
      //String msg = String.format("Starting span for record %s", record);
      //LOGGER.info(msg);
      //System.out.println(msg);
      KafkaConsumerContext consumerContext = KafkaConsumerContextUtil.get(record);
      Context receiveContext = consumerContext.getContext();
      Context parentContext = receiveContext != null ? receiveContext : Context.current();
      currentRequest = KafkaProcessRequest.create(consumerContext, record);
      currentContext = consumerProcessInstrumenter().start(parentContext, currentRequest);
      currentScope = currentContext.makeCurrent();
    }
  }

  @Override
  public int remainingCapacity() {
    return delegate.remainingCapacity();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return delegate.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends ConsumerRecord<K, V>> c) {
    return delegate.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return delegate.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return delegate.retainAll(c);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return delegate.contains(o);
  }

  @Override
  public Iterator<ConsumerRecord<K, V>> iterator() {
    return delegate.iterator();
  }

  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return delegate.toArray(a);
  }

  @Override
  public int drainTo(Collection<? super ConsumerRecord<K, V>> c) {
    return delegate.drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super ConsumerRecord<K, V>> c, int maxElements) {
    return delegate.drainTo(c, maxElements);
  }

  private void closeScopeAndEndSpan() {
    if (currentScope != null) {
      //String msg = String.format("Ending span for previous record %s", _currentRequest.getRecord());
      //LOGGER.info(msg);
      //System.out.println(msg);
      currentScope.close();
      consumerProcessInstrumenter().end(currentContext, currentRequest, null, null);
      currentScope = null;
      currentRequest = null;
      currentContext = null;
    }
  }

  private void writeObject(ObjectOutputStream stream)
      throws IOException {
    stream.defaultWriteObject();
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
  }
}
