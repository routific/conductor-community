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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueueHandler;

import com.spotify.futures.CompletableFutures;

public class KafkaPartitionTask implements Runnable {

    private final List<KafkaTransactionRecord> transactionRecords;

    private volatile boolean stopped = false;

    private volatile boolean started = false;

    private volatile boolean finished = false;

    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    private final ReentrantLock startStopLock = new ReentrantLock();

    private final AtomicLong currentOffset = new AtomicLong();

    private Logger logger = LoggerFactory.getLogger(KafkaPartitionTask.class);

    private ObservableQueueHandler handler;

    public KafkaPartitionTask(
            List<KafkaTransactionRecord> transactionRecords, ObservableQueueHandler handler) {
        this.transactionRecords = transactionRecords;
        this.handler = handler;
    }

    public void run() {

        long taskStartMillis = Instant.now().toEpochMilli();
        startStopLock.lock();
        if (stopped) {
            return;
        }
        started = true;
        startStopLock.unlock();

        long lastOffset = transactionRecords.get(transactionRecords.size() - 1).get().offset();

        List<CompletableFuture<Void>> processAll = new ArrayList<>();
        transactionRecords.forEach(
                (transactionRecord) -> {
                    ConsumerRecord<String, String> record = transactionRecord.get();
                    String id =
                            record.key()
                                    + ":"
                                    + record.topic()
                                    + ":"
                                    + record.partition()
                                    + ":"
                                    + record.offset();
                    Message message = new Message(id, String.valueOf(record.value()), "");
                    message.setUserIdentifier(transactionRecord.getUserIdentifier());

                    processAll.add(CompletableFuture.runAsync(() -> {
                        handler.call(message);
                        transactionRecord.finish();
                    }));
                });
        try {
            CompletableFutures.allAsList(processAll).get();
        } catch (Exception e) {
            logger.error("Error processing batch task: {}", e);
        }

        currentOffset.set(lastOffset + 1);

        // for (ConsumerRecord<String, String> record : records) {
        //     if (stopped) break;
        //     try {

        //         logger.info(
        //                 "Consumer Record: "
        //                         + "key: {}, "
        //                         + "value: {}, "
        //                         + "partition: {}, "
        //                         + "offset: {}",
        //                 record.key(),
        //                 record.value(),
        //                 record.partition(),
        //                 record.offset());
        //         String id =
        //                 record.key()
        //                         + ":"
        //                         + record.topic()
        //                         + ":"
        //                         + record.partition()
        //                         + ":"
        //                         + record.offset();
        //         Message message = new Message(id, String.valueOf(record.value()), "");

        //         this.handler.call(message);
        //     } catch (Exception e) {
        //         logger.error("Error processing kafka partition task: {}", e);
        //     }
        //     currentOffset.set(record.offset() + 1);
        // }

        finished = true;
        completion.complete(currentOffset.get());

        long timeTakenToCompleteTask = Instant.now().toEpochMilli() - taskStartMillis;
        logger.info(
                "Task processed {} records, Time taken {}",
                transactionRecords.size(),
                timeTakenToCompleteTask);
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }
}
