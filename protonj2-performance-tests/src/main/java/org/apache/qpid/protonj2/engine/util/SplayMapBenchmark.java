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

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Tests for performance characteristics of the {@link SplayMap} implementation
 */
public class SplayMapBenchmark extends MapBenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        runBenchmark(SplayMapBenchmark.class);
    }

    private SplayMap<String> sqMap;
    private SplayMap<String> sqFilledMap;

    @Override
    @Setup(Level.Invocation)
    public void init() {
        super.init();

        this.sqMap = (SplayMap<String>) map;
        this.sqFilledMap = (SplayMap<String>) filledMap;
    }

    @Benchmark
    public void putWithPrimitive() {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            sqMap.put(i, DUMMY_STRING);
        }
    }

    @Benchmark
    public void getWithPrimitive(Blackhole blackHole) {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            blackHole.consume(sqFilledMap.get(i));
        }
    }

    @Benchmark
    public void removeWithPrimitive(Blackhole blackHole) {
        for (int i = 0; i < DEFAULT_MAP_VALUE_RANGE; ++i) {
            blackHole.consume(sqFilledMap.remove(i));
        }
    }

    @Override
    protected Map<UnsignedInteger, String> createMap() {
        return new SplayMap<>();
    }
}
