package io.opentelemetry.javaagent.instrumentation.kafkaclients.v0_11;

import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class TrackingConsumerWrapperInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("com.linkedin.tracker.consumer.TrackingConsumerWrapper");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isMethod().and(named("subscribe")),
        TrackingConsumerWrapperInstrumentation.class.getName() + "$SubscribeAdvice");
  }

  @SuppressWarnings("unused")
  public static class SubscribeAdvice {
    @Advice.OnMethodExit
    public static <K, V> void wrap(
        @Advice.FieldValue(readOnly = false, value = "_topicRecordQueues")
        ConcurrentHashMap<String, List<LinkedBlockingQueue<ConsumerRecord<K, V>>>> recordQueues) {

      // it's important not to suppress consumer span creation here because this instrumentation can
      // leak the context and so there may be a leaked consumer span in the context, in which
      // case it's important to overwrite the leaked span instead of suppressing the correct span
      // (https://github.com/open-telemetry/opentelemetry-java-instrumentation/issues/1947)

      System.out.println("SubscribeAdvice was called!");
      for (Map.Entry<String, List<LinkedBlockingQueue<ConsumerRecord<K, V>>>> entry : recordQueues.entrySet()) {
        String topic = entry.getKey();
        List<LinkedBlockingQueue<ConsumerRecord<K, V>>> queues = entry.getValue();
        for (int i = 0; i < queues.size(); i++) {
          LinkedBlockingQueue<ConsumerRecord<K, V>> toWrap = queues.get(i);
          System.out.println("Wrapping this queue " + toWrap + " for topic " + topic);
          queues.set(i, TracingQueue.wrap(toWrap));
          System.out.println("Wrapped queue " + toWrap + " for topic " + topic);
        }
      }
    }
  }
}
