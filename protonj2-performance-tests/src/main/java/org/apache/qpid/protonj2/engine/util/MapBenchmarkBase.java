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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.types.UnsignedInteger;
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
public abstract class MapBenchmarkBase {

    public static final int DEFAULT_MAP_VALUE_RANGE = 8192;

    protected final String DUMMY_STRING = "ASDFGHJ";
    protected final Random random = new Random();

    protected Map<UnsignedInteger, String> map;
    protected Map<UnsignedInteger, String> filledMap;

    @Setup(Level.Invocation)
    public void init() {
        this.random.setSeed(System.currentTimeMillis());
        this.map = createMap();
        this.filledMap = fillMap(createMap());
    }

    @Benchmark
    public void put() {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            map.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
        }
    }

    @Benchmark
    public void get(Blackhole blackHole) {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            blackHole.consume(filledMap.get(UnsignedInteger.valueOf(i)));
        }
    }

    @Benchmark
    public void remove(Blackhole blackHole) {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            blackHole.consume(filledMap.remove(UnsignedInteger.valueOf(i)));
        }
    }

    @Benchmark
    public void produceAndConsume(Blackhole blackHole) {
        for (int i = 0; i < 32; ++i) {
            map.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
        }

        for (int p = 0, c = map.size(); p < DEFAULT_MAP_VALUE_RANGE; ++p, ++c) {
            blackHole.consume(filledMap.put(UnsignedInteger.valueOf(p), DUMMY_STRING));
            blackHole.consume(filledMap.remove(UnsignedInteger.valueOf(c)));
        }
    }

    @Benchmark
    public void randomProduceAndConsume(Blackhole blackHole) {
        for (int i = 0; i < 32; ++i) {
            map.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
        }

        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            int p = random.nextInt(DEFAULT_MAP_VALUE_RANGE);
            int c = random.nextInt(DEFAULT_MAP_VALUE_RANGE);

            blackHole.consume(filledMap.put(UnsignedInteger.valueOf(p), DUMMY_STRING));
            blackHole.consume(filledMap.remove(UnsignedInteger.valueOf(c)));
        }
    }

    protected abstract Map<UnsignedInteger, String> createMap();

    protected Map<UnsignedInteger, String> fillMap(Map<UnsignedInteger, String> target) {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            target.put(UnsignedInteger.valueOf(i), DUMMY_STRING);
        }

        return target;
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(MapBenchmarkBase.class);
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
