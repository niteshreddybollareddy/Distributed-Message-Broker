package com.broker.benchmark;

import com.broker.core.BrokerDisruptor;
import com.broker.storage.CommitLog;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class RingBufferBenchmark {

    private BrokerDisruptor disruptor;
    private CommitLog commitLog;
    private byte[] dummyPayload;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tempDir = Files.createTempDirectory("benchmark-log");
        commitLog = new CommitLog(tempDir);
        disruptor = new BrokerDisruptor(commitLog);
        dummyPayload = "benchmark-data".getBytes();
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        disruptor.shutdown();
        commitLog.close();
    }

    @Benchmark
    public void testPublishLatency() {
        disruptor.publish(dummyPayload);
    }
}
