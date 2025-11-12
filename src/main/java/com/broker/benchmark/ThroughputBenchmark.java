package com.broker.benchmark;

import com.broker.core.BrokerDisruptor;
import com.broker.storage.CommitLog;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ThroughputBenchmark {

    private BrokerDisruptor disruptor;
    private CommitLog commitLog;
    private byte[] dummyPayload;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Path tempDir = Files.createTempDirectory("benchmark-throughput");
        commitLog = new CommitLog(tempDir);
        disruptor = new BrokerDisruptor(commitLog);
        dummyPayload = new byte[1024]; // 1KB payload
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        disruptor.shutdown();
        commitLog.close();
    }

    @Benchmark
    public void testThroughput() {
        disruptor.publish(dummyPayload);
    }
}
