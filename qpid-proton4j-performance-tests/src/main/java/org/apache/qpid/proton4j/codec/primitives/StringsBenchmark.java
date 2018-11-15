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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

public class StringsBenchmark extends CodecBenchmarkBase {

    private static final String PAYLOAD =
          "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
        + "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    private Blackhole blackhole;
    private String string1;
    private String string2;
    private String string3;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;
        super.init();
        initStrings();
        encode();
    }

    private void initStrings() {
        string1 = new String("String-1");
        string2 = new String("String-2");
        string3 = new String("String-3");
    }

    @Benchmark
    public ProtonBuffer encode() {
        buffer.clear();
        encoder.writeString(buffer, encoderState, string1);
        encoder.writeString(buffer, encoderState, string2);
        encoder.writeString(buffer, encoderState, string3);
        return buffer;
    }

    @Benchmark
    public ProtonBuffer encodeLargeString() {
        buffer.clear();
        encoder.writeString(buffer, encoderState, PAYLOAD);
        return buffer;
    }

    @Benchmark
    public ProtonBuffer decode() throws IOException {
        buffer.setReadIndex(0);
        blackhole.consume(decoder.readString(buffer, decoderState));
        blackhole.consume(decoder.readString(buffer, decoderState));
        blackhole.consume(decoder.readString(buffer, decoderState));
        return buffer;
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(StringsBenchmark.class);
    }
}
