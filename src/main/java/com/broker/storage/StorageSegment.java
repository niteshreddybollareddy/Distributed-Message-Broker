package com.broker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageSegment extends AutoCloseable {
    
    /**
     * Appends a message to the segment.
     * @param data Byte array containing the message.
     * @return Offset of the appended message, or -1 if not enough space.
     */
    int append(byte[] data) throws IOException;

    /**
     * Reads a message into a provided ByteBuffer.
     * @param offset Offset to read from.
     * @param dest The destination ByteBuffer.
     * @return The number of bytes read, or -1 if the offset is invalid.
     */
    int read(int offset, ByteBuffer dest);

    /**
     * Force changes to be flushed to disk.
     */
    void flush();
    
    @Override
    void close() throws IOException;
}
