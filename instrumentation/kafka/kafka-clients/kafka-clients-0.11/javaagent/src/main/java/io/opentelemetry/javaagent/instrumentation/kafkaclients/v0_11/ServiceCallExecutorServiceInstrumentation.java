package io.opentelemetry.javaagent.instrumentation.kafkaclients.v0_11;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;

import java.util.concurrent.ExecutorService;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.matcher.ElementMatcher;


public class ServiceCallExecutorServiceInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("com.linkedin.container.servicecall.ServiceCallExecutorService");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isMethod().and(isConstructor()),
        ServiceCallExecutorServiceInstrumentation.class.getName() + "$ConstructorAdvice");
  }

  @SuppressWarnings("unused")
  public static class ConstructorAdvice {
    @Advice.OnMethodExit
    public static void wrap(
        @Advice.FieldValue(value = "_threadType") Object threadType,
        @Advice.This(
            typing = Assigner.Typing.DYNAMIC, readOnly = false) ExecutorService executorService) {
      if (((Enum) threadType).name().equals("QUEUE")) {
        executorService = Context.taskWrapping(executorService);
      }
    }
  }
}
