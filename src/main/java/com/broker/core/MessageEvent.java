package com.broker.core;

import com.lmax.disruptor.EventFactory;

/**
 * Pre-allocated event for the LMAX Disruptor ring buffer.
 * It uses memory padding to avoid false sharing of CPU cache lines.
 */
public class MessageEvent {
    
    // Padding to prevent false sharing (assuming 64-byte cache line)
    public long p1, p2, p3, p4, p5, p6, p7;
    
    // The actual payload
    private byte[] payload;
    private long globalOffset; // The offset assigned after committing to log
    
    // More padding after fields
    public long p8, p9, p10, p11, p12, p13, p14;

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setGlobalOffset(long globalOffset) {
        this.globalOffset = globalOffset;
    }

    public long getGlobalOffset() {
        return globalOffset;
    }

    public final static EventFactory<MessageEvent> FACTORY = MessageEvent::new;
}
