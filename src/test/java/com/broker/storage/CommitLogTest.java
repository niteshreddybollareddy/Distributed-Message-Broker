package com.broker.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommitLogTest {
    private Path tempDir;
    private CommitLog commitLog;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("broker-log-test");
        commitLog = new CommitLog(tempDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        commitLog.close();
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }

    @Test
    void testAppendAndRead() throws IOException {
        String msgStr = "Hello World";
        byte[] payload = msgStr.getBytes();
        
        long offset = commitLog.append(payload);
        assertEquals(0, offset);
        
        ByteBuffer readBuffer = ByteBuffer.allocate(32);
        // localOffset is 0 for the first message in our simple logic right now
        int length = commitLog.read(0, readBuffer);
        
        assertEquals(payload.length, length);
        
        readBuffer.flip();
        byte[] readBytes = new byte[length];
        readBuffer.get(readBytes);
        
        assertEquals(msgStr, new String(readBytes));
    }
}
