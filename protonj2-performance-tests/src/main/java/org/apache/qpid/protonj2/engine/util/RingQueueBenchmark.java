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
package org.apache.qpid.protonj2.engine.util;

import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 1)
public class RingQueueBenchmark {

    public static final int DEFAULT_MAP_VALUE_RANGE = 8192;

    protected final String DUMMY_STRING = "ASDFGHJ";
    protected final Random random = new Random();

    @Setup
    public void init() {
        this.random.setSeed(System.currentTimeMillis());
    }

    @Benchmark
    public void offer() {
        final Queue<String> queue = new RingQueue<>(32);

        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            if (!queue.offer(DUMMY_STRING)) {
                queue.clear();
                queue.offer(DUMMY_STRING);
            }
        }
    }

    @Benchmark
    public void produceAndConsume(Blackhole blackHole) {
        final Queue<String> queue = new RingQueue<>(32);

        for (int i = 0; i < 32; ++i) {
            queue.offer(DUMMY_STRING);
        }

        for (int p = 0; p < DEFAULT_MAP_VALUE_RANGE; ++p) {
            blackHole.consume(queue.poll());
            queue.offer(DUMMY_STRING);
        }
    }

    @Benchmark
    public void contains(Blackhole blackHole) {
        final Queue<Integer> queue = new RingQueue<>(256);

        for (int i = 0; i < 256; ++i) {
            queue.offer(Integer.valueOf(i));
        }

        for (int i = 0; i < 256; ++i) {
            blackHole.consume(queue.contains(Integer.valueOf(i)));
        }
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(RingQueueBenchmark.class);
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
