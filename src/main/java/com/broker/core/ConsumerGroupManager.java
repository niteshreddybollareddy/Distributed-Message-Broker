package com.broker.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages offsets for different consumer groups and calculates the high-watermark.
 */
public class ConsumerGroupManager {
    // Map of GroupID -> Last fetched offset
    private final ConcurrentHashMap<String, AtomicLong> groupOffsets = new ConcurrentHashMap<>();
    
    // The high-watermark is the minimum offset among all strictly replicated groups
    // For simplicity, we assume all registered groups must consume before watermark advances
    private final AtomicLong highWatermark = new AtomicLong(0);

    /**
     * Updates the offset for a specific consumer group.
     */
    public void updateOffset(String groupId, long offset) {
        groupOffsets.computeIfAbsent(groupId, k -> new AtomicLong(0)).set(offset);
        recalculateHighWatermark();
    }

    public long getOffset(String groupId) {
        AtomicLong offset = groupOffsets.get(groupId);
        return offset != null ? offset.get() : 0L;
    }

    private void recalculateHighWatermark() {
        if (groupOffsets.isEmpty()) {
            return;
        }

        long minOffset = Long.MAX_VALUE;
        for (AtomicLong offset : groupOffsets.values()) {
            if (offset.get() < minOffset) {
                minOffset = offset.get();
            }
        }
        
        // Update high watermark if it increased
        long currentWatermark;
        do {
            currentWatermark = highWatermark.get();
            if (minOffset <= currentWatermark) {
                break;
            }
        } while (!highWatermark.compareAndSet(currentWatermark, minOffset));
    }

    public long getHighWatermark() {
        return highWatermark.get();
    }
}
