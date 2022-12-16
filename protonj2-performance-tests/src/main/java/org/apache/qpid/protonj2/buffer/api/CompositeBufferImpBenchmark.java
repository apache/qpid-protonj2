/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.buffer.api;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Base for benchmarks involving {@link Map} types.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 1)
public class CompositeBufferImpBenchmark {

    public static final int DEFAULT_MAP_VALUE_RANGE = 8192;

    protected final String DUMMY_STRING = "ASDFGHJ";
    protected final Random random = new Random();

    private ProtonBuffer buffer1;
    private ProtonBuffer buffer2;
    private ProtonBuffer buffer3;

    @Setup(Level.Invocation)
    public void init() {
        buffer1 = ProtonBufferAllocator.defaultAllocator().allocate(64);
        buffer2 = ProtonBufferAllocator.defaultAllocator().allocate(64);
        buffer3 = ProtonBufferAllocator.defaultAllocator().allocate(64);

        this.random.setSeed(System.currentTimeMillis());
    }

    @Benchmark
    public void ensureWritable() {
        final ProtonCompositeBuffer buffer = ProtonBufferAllocator.defaultAllocator().composite();

        buffer.ensureWritable(64);
        buffer.ensureWritable(64);
        buffer.ensureWritable(64);
    }

    @Benchmark
    public void appendAllReadable() {
        final ProtonCompositeBuffer buffer = ProtonBufferAllocator.defaultAllocator().composite();

        buffer1.setWriteOffset(buffer1.capacity());
        buffer2.setWriteOffset(buffer2.capacity());
        buffer3.setWriteOffset(buffer3.capacity());

        buffer.append(buffer1).append(buffer2).append(buffer3);
    }

    @Benchmark
    public void appendAllWritable() {
        final ProtonCompositeBuffer buffer = ProtonBufferAllocator.defaultAllocator().composite();

        buffer.append(buffer1).append(buffer2).append(buffer3);
    }

    @Benchmark
    public void appendReadableWithUnreadChunks() {
        final ProtonCompositeBuffer buffer = ProtonBufferAllocator.defaultAllocator().composite();

        buffer1.setWriteOffset(buffer1.capacity());
        buffer2.setWriteOffset(buffer2.capacity());
        buffer3.setWriteOffset(buffer3.capacity());

        buffer1.advanceReadOffset(buffer1.capacity() / 2);
        buffer2.advanceReadOffset(buffer2.capacity() / 2);
        buffer3.advanceReadOffset(buffer3.capacity() / 2);

        buffer.append(buffer1).append(buffer2).append(buffer3);
    }

    @Benchmark
    public void appendBufferWithWrittenChunkAndAnotherAllReadable() {
        final ProtonCompositeBuffer buffer = ProtonBufferAllocator.defaultAllocator().composite();

        buffer1.setWriteOffset(buffer1.capacity() / 2);
        buffer2.setWriteOffset(buffer2.capacity());

        buffer.append(buffer1).append(buffer2);
    }

    @Benchmark
    public void composeSingleReadable() {
        buffer1.advanceWriteOffset(buffer1.capacity());

        final ProtonCompositeBuffer composite = ProtonBufferAllocator.defaultAllocator().composite(buffer1);

        composite.capacity();
    }

    @Benchmark
    public void composeAllReadable() {
        buffer1.setWriteOffset(buffer1.capacity());
        buffer2.setWriteOffset(buffer2.capacity());
        buffer3.setWriteOffset(buffer3.capacity());

        final ProtonCompositeBuffer buffer =
            ProtonBufferAllocator.defaultAllocator().composite(new ProtonBuffer[] { buffer1, buffer2, buffer3 });

        buffer.capacity();
    }

    @Benchmark
    public void composeAllWritable() {
        final ProtonCompositeBuffer buffer =
            ProtonBufferAllocator.defaultAllocator().composite(new ProtonBuffer[] { buffer1, buffer2, buffer3 });

        buffer.capacity();
    }

    @Benchmark
    public void composeReadableWithUnreadChunks() {
        buffer1.setWriteOffset(buffer1.capacity());
        buffer2.setWriteOffset(buffer2.capacity());
        buffer3.setWriteOffset(buffer3.capacity());

        buffer1.advanceReadOffset(buffer1.capacity() / 2);
        buffer2.advanceReadOffset(buffer2.capacity() / 2);
        buffer3.advanceReadOffset(buffer3.capacity() / 2);

        final ProtonCompositeBuffer buffer =
            ProtonBufferAllocator.defaultAllocator().composite(new ProtonBuffer[] { buffer1, buffer2, buffer3 });

        buffer.capacity();
    }

    @Benchmark
    public void composeBufferWithWrittenChunkAndAnotherAllReadable() {
        buffer1.setWriteOffset(buffer1.capacity() / 2);
        buffer2.setWriteOffset(buffer2.capacity());

        final ProtonCompositeBuffer buffer =
            ProtonBufferAllocator.defaultAllocator().composite(new ProtonBuffer[] { buffer1, buffer2 });

        buffer.capacity();
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(CompositeBufferImpBenchmark.class);
    }

    public static void runBenchmark(Class<?> benchmarkClass) throws RunnerException {
        final Options opt = new OptionsBuilder()
            .include(benchmarkClass.getSimpleName())
            .addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }
}
