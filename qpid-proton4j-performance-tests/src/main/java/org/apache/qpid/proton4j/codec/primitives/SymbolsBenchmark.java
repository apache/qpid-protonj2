/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton4j.codec.primitives;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.codec.CodecBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

public class SymbolsBenchmark extends CodecBenchmarkBase {

    private Symbol symbol1;
    private Symbol symbol2;
    private Symbol symbol3;
    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;
        super.init();
        initSymbols();
        encode();
    }

    private void initSymbols() {
        symbol1 = Symbol.valueOf("Symbol-1");
        symbol2 = Symbol.valueOf("Symbol-2");
        symbol3 = Symbol.valueOf("Symbol-3");
    }

    @Benchmark
    public void encode() {
        buffer.clear();
        encoder.writeSymbol(buffer, encoderState, symbol1);
        encoder.writeSymbol(buffer, encoderState, symbol2);
        encoder.writeSymbol(buffer, encoderState, symbol3);
    }

    @Benchmark
    public void decode() throws IOException {
        buffer.setReadIndex(0);
        blackhole.consume(decoder.readSymbol(buffer, decoderState));
        blackhole.consume(decoder.readSymbol(buffer, decoderState));
        blackhole.consume(decoder.readSymbol(buffer, decoderState));
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(SymbolsBenchmark.class);
    }
}