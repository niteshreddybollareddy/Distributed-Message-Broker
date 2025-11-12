package com.broker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CommitLog implements AutoCloseable {
    private static final int SEGMENT_SIZE = 1024 * 1024 * 128; // 128 MB per segment
    private final Path logDirectory;
    private StorageSegment currentSegment;
    private final AtomicInteger currentSegmentIndex;
    
    // Tracks the absolute global offset of the next message
    private final AtomicLong globalOffset;

    public enum StorageType { MMAP, IO_URING }
    
    private final StorageType storageType;

    public CommitLog(Path logDirectory, StorageType storageType) throws IOException {
        this.logDirectory = logDirectory;
        this.storageType = storageType;
        this.currentSegmentIndex = new AtomicInteger(0);
        this.globalOffset = new AtomicLong(0);
        
        // Initialize the first segment
        rollSegment();
    }

    public CommitLog(Path logDirectory) throws IOException {
        this(logDirectory, StorageType.MMAP); // Default to MMAP for compatibility
    }

    private synchronized void rollSegment() throws IOException {
        if (currentSegment != null) {
            currentSegment.flush();
        }
        
        String segmentFileName = String.format("segment-%010d.log", currentSegmentIndex.getAndIncrement());
        Path segmentPath = logDirectory.resolve(segmentFileName);
        
        if (storageType == StorageType.IO_URING) {
            try {
                // Dynamically instantiate to avoid ClassNotFoundException on macOS if class isn't loaded properly
                currentSegment = (StorageSegment) Class.forName("com.broker.storage.IoUringSegment")
                        .getConstructor(Path.class, int.class)
                        .newInstance(segmentPath, SEGMENT_SIZE);
            } catch (Exception e) {
                throw new IOException("Failed to initialize io_uring segment on this OS", e);
            }
        } else {
            currentSegment = new MappedSegment(segmentPath, SEGMENT_SIZE);
        }
    }

    private final java.util.List<Integer> offsetIndex = new java.util.ArrayList<>();

    /**
     * Appends data to the commit log.
     * @param data The binary payload.
     * @return The global offset of the appended message.
     * @throws IOException
     */
    public synchronized long append(byte[] data) throws IOException {
        int segmentOffset = currentSegment.append(data);
        
        if (segmentOffset == -1) {
            // Segment is full, roll over to a new one
            rollSegment();
            segmentOffset = currentSegment.append(data);
        }
        
        long currentOffset = globalOffset.getAndIncrement();
        offsetIndex.add(segmentOffset);
        return currentOffset;
    }

    /**
     * Reads a batch of messages starting from a global offset.
     * For simplicity, this implementation reads at most 1 message from the current segment.
     */
    public synchronized byte[] readBatch(long startGlobalOffset, int maxBytes) {
        if (startGlobalOffset < 0 || startGlobalOffset >= globalOffset.get()) {
            return new byte[0]; // Empty batch
        }
        
        // Find local offset
        int localOffset = offsetIndex.get((int) startGlobalOffset);
        
        ByteBuffer dest = ByteBuffer.allocate(maxBytes);
        int bytesRead = currentSegment.read(localOffset, dest);
        
        if (bytesRead > 0) {
            dest.flip();
            byte[] result = new byte[bytesRead];
            dest.get(result);
            return result;
        }
        
        return new byte[0];
    }
    
    public void flush() {
        if (currentSegment != null) {
            currentSegment.flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (currentSegment != null) {
            currentSegment.close();
        }
    }
}
