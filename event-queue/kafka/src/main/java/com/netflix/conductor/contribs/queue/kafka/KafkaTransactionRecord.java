/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.core.tracing.Tracing;
import com.netflix.conductor.core.tracing.TracingProvider;

// import io.sentry.ITransaction;
// import io.sentry.Sentry;
// import io.sentry.SentryTraceHeader;
// import io.sentry.TransactionContext;

// import io.opentelemetry.api.GlobalOpenTelemetry;
// import io.opentelemetry.api.trace.Span;
// import io.opentelemetry.api.trace.SpanContext;
// import io.opentelemetry.api.trace.TraceFlags;
// import io.opentelemetry.api.trace.TraceState;
// import io.opentelemetry.api.trace.Tracer;
// import io.opentelemetry.context.Context;

public class KafkaTransactionRecord {
  private ConsumerRecord<String, String> record;
  // private Optional<ITransaction> transaction = Optional.empty();
  // private Optional<Span> span = Optional.empty();
  private Optional<String> traceHeader = Optional.empty();
  private TracingProvider tracingProvider;
  private Tracing tracing;

  private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionRecord.class);

  public KafkaTransactionRecord(ConsumerRecord<String, String> record, TracingProvider tracingProvider) {
    this.record = record;
    this.tracingProvider = tracingProvider;

    Headers headers = record.headers();
    Header header = headers.lastHeader("traceparent");
    if (header != null) {
      this.traceHeader = Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }
  }

  public void start() {
    try {
      if (this.traceHeader.isPresent()) {
        this.tracing = this.tracingProvider.startTracing("Consumed " + record.topic(), traceHeader);
      }
    } catch (Exception e) {
      logger.error("Error creating sentry transaction: {}", e);
    }
  }

  public ConsumerRecord<String, String> get() {
    return this.record;
  }

  public void finish() {
    // if (this.transaction.isPresent()) {
    //   this.transaction.get().finish();
    // }

    // if (this.span.isPresent()) {
    //   this.span.get().end();
    // }
      if (this.tracing != null) {
        this.tracing.finish();
      }
   }
}
