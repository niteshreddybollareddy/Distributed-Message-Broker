package com.broker.core;

import com.broker.storage.CommitLog;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class BrokerDisruptor {

    private final Disruptor<MessageEvent> disruptor;
    private final RingBuffer<MessageEvent> ringBuffer;
    private final CommitLog commitLog;

    public BrokerDisruptor(CommitLog commitLog) {
        this.commitLog = commitLog;
        
        // Define ring buffer size (must be power of 2)
        int bufferSize = 1024 * 64; 
        
        ThreadFactory threadFactory = Executors.defaultThreadFactory();

        // We use ProducerType.MULTI as multiple network threads might be publishing
        // BlockingWaitStrategy is chosen for lower CPU usage when idle, 
        // though BusySpinWaitStrategy could be used for absolute lowest latency at the cost of 100% CPU.
        this.disruptor = new Disruptor<>(
                MessageEvent.FACTORY, 
                bufferSize, 
                threadFactory, 
                ProducerType.MULTI, 
                new BlockingWaitStrategy()
        );

        // Define the event handler for appending to the commit log
        EventHandler<MessageEvent> logAppender = (event, sequence, endOfBatch) -> {
            try {
                // Write payload to mmap commit log
                long offset = this.commitLog.append(event.getPayload());
                event.setGlobalOffset(offset);
            } catch (IOException e) {
                e.printStackTrace(); // In real app, proper error handling
            }
        };

        // Define event handler for notifying network consumers (replicas/subscribers)
        // This runs AFTER logAppender
        EventHandler<MessageEvent> networkNotifier = (event, sequence, endOfBatch) -> {
            // Here we would push the committed event to consumer channels
            // (Skipped full implementation for brevity, typically Netty channel writes happen here)
        };

        // Chain the handlers: first append to log, then notify
        disruptor.handleEventsWith(logAppender).then(networkNotifier);
        
        this.ringBuffer = disruptor.start();
    }

    /**
     * Publishes a message to the RingBuffer.
     * This method is zero-allocation: it claims a pre-allocated event from the buffer.
     *
     * @param payload The binary message data.
     */
    public void publish(byte[] payload) {
        long sequence = ringBuffer.next();
        try {
            MessageEvent event = ringBuffer.get(sequence);
            event.setPayload(payload);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    public void shutdown() {
        disruptor.shutdown();
    }
}
