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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1, warmups = 1,
        jvmArgsPrepend = {"-Xmx1g", "-Xms1g"},
        jvmArgsAppend = {"-dsa",
        "-da",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints",
})
@Warmup(iterations = 1, time = 3)
@Measurement(iterations = 1, time = 3)
@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class CompositeBufferAppendBenchmark {

    final static ProtonBufferAllocator ALLOC = ProtonBufferAllocator.defaultAllocator();
    final static String dataString = Stream.generate(() -> "x").limit(100).collect(Collectors.joining());
    final static byte[] data = dataString.getBytes(StandardCharsets.UTF_8);
    final static int BUFFERS = 64;
    List<ProtonBuffer> buffers;

    @Setup
    public void setUp() {
        buffers = new ArrayList<>(BUFFERS);
        for (int i = 0; i < BUFFERS; i++) {
            buffers.add(ALLOC.copy(data).convertToReadOnly());
        }
    }

    @Benchmark
    public void compositeBufferBenchmark() {
        ProtonCompositeBuffer cb = ALLOC.composite();
        for (ProtonBuffer buffer : buffers) {
            cb.append(buffer.copy(true));
        }
        cb.close();
    }
}
