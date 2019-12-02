/*
 * Copyright 2013-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.jms;

import brave.Span;
import brave.Tracing;
import brave.internal.Nullable;
import brave.messaging.MessagingRequest;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.Message;

import static brave.internal.Throwables.propagateIfFatal;
import static brave.jms.JmsTracing.log;

abstract class TracingConsumer<C> {
  final C delegate;
  final JmsTracing jmsTracing;
  final Tracing tracing;
  final Extractor<MessageConsumerRequest> extractor;
  final Injector<MessageConsumerRequest> injector;
  final SamplerFunction<MessagingRequest> sampler;
  @Nullable final String remoteServiceName;

  TracingConsumer(C delegate, JmsTracing jmsTracing) {
    this.delegate = delegate;
    this.jmsTracing = jmsTracing;
    this.tracing = jmsTracing.tracing;
    this.extractor = jmsTracing.messageConsumerExtractor;
    this.sampler = jmsTracing.consumerSampler;
    this.injector = jmsTracing.messageConsumerInjector;
    this.remoteServiceName = jmsTracing.remoteServiceName;
  }

  void handleReceive(Message message) {
    if (message == null || tracing.isNoop()) return;
    MessageConsumerRequest request = new MessageConsumerRequest(message, destination(message));

    // Workaround for #967 bug related to ActiveMQBytesMessage JMS 1.1 implementation where properties cannot
    // be overwritten. This workaround recreates the message to set it on write-more, so properties
    // update is allowed.
    // This conditional code should be removed when new ActiveMQ client release fixes https://issues.apache.org/jira/browse/AMQ-7291
    if (message instanceof BytesMessage &&
        message.getClass().getName().equals("org.apache.activemq.command.ActiveMQBytesMessage")) {
      BytesMessage bytesMessage = (BytesMessage) message;
      try {
        byte[] body = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(body);
        bytesMessage.clearBody();
        bytesMessage.writeBytes(body);
      } catch (Throwable t) {
        propagateIfFatal(t);
        log(t, "error recreating bytes message {0}", message, null);
        return;
      }
    } // end of initialization of workaround

    TraceContextOrSamplingFlags extracted =
      jmsTracing.extractAndClearProperties(extractor, request, message);
    Span span = jmsTracing.nextMessagingSpan(sampler, request, extracted);

    if (!span.isNoop()) {
      span.name("receive").kind(Span.Kind.CONSUMER);
      Destination destination = destination(message);
      if (destination != null) jmsTracing.tagQueueOrTopic(request, span);
      if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);

      // incur timestamp overhead only once
      long timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
      span.start(timestamp).finish(timestamp);
    }
    injector.inject(span.context(), request);

    // Continuation of Workaround for #967 bug.
    // This conditional code should be removed when new ActiveMQ client release fixes https://issues.apache.org/jira/browse/AMQ-7291
    if (message instanceof BytesMessage &&
        message.getClass().getName().equals("org.apache.activemq.command.ActiveMQBytesMessage")) {
      try {
        ((BytesMessage) message).reset();
      } catch (Throwable t) {
        propagateIfFatal(t);
        log(t, "error setting recreated bytes message {0} to read-only mode", message, null);
      }
    } // end of workaround
  }

  abstract @Nullable Destination destination(Message message);
}
