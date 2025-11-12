package com.broker.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class MappedSegment implements StorageSegment {
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    private final int capacity;
    private final Path path;
    
    // Tracks the current write position
    private int position = 0;

    public MappedSegment(Path path, int capacity) throws IOException {
        this.capacity = capacity;
        this.path = path;
        
        // Ensure parent directories exist
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
        raf.setLength(capacity); // Pre-allocate file size
        this.fileChannel = raf.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
    }

    /**
     * Appends a message to the segment.
     * Returns the offset where the message was written, or -1 if the segment is full.
     *
     * @param data Byte array containing the message.
     * @return Offset of the appended message, or -1 if not enough space.
     */
    public synchronized int append(byte[] data) {
        // Message format: [4 bytes length] [data]
        int requiredSpace = Integer.BYTES + data.length;
        if (position + requiredSpace > capacity) {
            return -1;
        }

        int startOffset = position;
        mappedByteBuffer.putInt(position, data.length);
        
        // Zero-allocation write for primitive arrays
        for (int i = 0; i < data.length; i++) {
            mappedByteBuffer.put(position + Integer.BYTES + i, data[i]);
        }
        
        position += requiredSpace;
        return startOffset;
    }

    /**
     * Reads a message directly into a provided ByteBuffer (zero allocation).
     *
     * @param offset Offset to read from.
     * @param dest The destination ByteBuffer. It should have enough remaining space.
     * @return The number of bytes read, or -1 if the offset is invalid.
     */
    public int read(int offset, ByteBuffer dest) {
        if (offset < 0 || offset >= capacity - Integer.BYTES) {
            return -1;
        }

        int length = mappedByteBuffer.getInt(offset);
        if (length <= 0 || offset + Integer.BYTES + length > capacity) {
            return -1;
        }

        // Duplicate the buffer so we don't modify the original buffer's position
        ByteBuffer readBuffer = mappedByteBuffer.duplicate();
        readBuffer.position(offset + Integer.BYTES);
        readBuffer.limit(offset + Integer.BYTES + length);
        
        dest.put(readBuffer);
        return length;
    }
    
    /**
     * Force changes to be flushed to disk.
     */
    public void flush() {
        mappedByteBuffer.force();
    }
    
    public int getPosition() {
        return position;
    }

    @Override
    public void close() throws IOException {
        flush();
        fileChannel.close();
        // Unmapping in Java is generally handled by GC, though there are sun.misc.Unsafe hacks
        // we'll keep it simple for now as per mechanical sympathy but safe code.
    }
}
