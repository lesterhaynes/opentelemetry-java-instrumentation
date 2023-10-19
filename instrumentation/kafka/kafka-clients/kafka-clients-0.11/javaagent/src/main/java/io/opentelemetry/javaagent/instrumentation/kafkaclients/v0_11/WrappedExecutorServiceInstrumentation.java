package io.opentelemetry.javaagent.instrumentation.kafkaclients.v0_11;

import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.nameContains;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;

import java.util.concurrent.ExecutorService;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;


public class WrappedExecutorServiceInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return nameContains("WrappedExecutorService");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isMethod().and(named("getServiceCallExecutorService")),
        WrappedExecutorServiceInstrumentation.class.getName() + "$GetAdvice");
  }

  @SuppressWarnings("unused")
  public static class GetAdvice {
    @Advice.OnMethodExit
    public static void wrap(
        @Advice.Return(readOnly = false) ExecutorService executorService) {
      executorService = Context.taskWrapping(executorService);
    }
  }
}
