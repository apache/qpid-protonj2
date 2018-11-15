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
import java.util.ArrayList;

import org.apache.qpid.proton4j.codec.CodecBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

public class ListOfIntsBenchmark extends CodecBenchmarkBase {

    private static final int LIST_SIZE = 10;
    private ArrayList<Object> listOfInts;
    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;
        super.init();
        initListOfInts();
        encode();
    }

    private void initListOfInts() {
        this.listOfInts = new ArrayList<>(LIST_SIZE);
        for (int i = 0; i < LIST_SIZE; i++) {
            listOfInts.add(i);
        }
    }

    @Benchmark
    public void encode() {
        buffer.clear();
        encoder.writeList(buffer, encoderState, listOfInts);
    }

    @Benchmark
    public void decode() throws IOException {
        buffer.setReadIndex(0);
        blackhole.consume(decoder.readList(buffer, decoderState));
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(ListOfIntsBenchmark.class);
    }

}