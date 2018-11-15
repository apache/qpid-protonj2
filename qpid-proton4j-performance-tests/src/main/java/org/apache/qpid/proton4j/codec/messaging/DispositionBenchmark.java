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
package org.apache.qpid.proton4j.codec.messaging;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.codec.CodecBenchmarkBase;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

public class DispositionBenchmark extends CodecBenchmarkBase {
    private Disposition disposition;
    private Blackhole blackhole;

    @Setup
    public void init(Blackhole blackhole) {
        this.blackhole = blackhole;
        super.init();
        initDisposition();
        encode();
    }

    private void initDisposition() {
        disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setSettled(true);
        disposition.setState(Accepted.getInstance());
        disposition.setFirst(UnsignedInteger.valueOf(2));
        disposition.setLast(UnsignedInteger.valueOf(2));
    }

    @Benchmark
    public void encode() {
        buffer.clear();
        encoder.writeObject(buffer, encoderState, disposition);
    }

    @Benchmark
    public void decode() throws IOException {
        buffer.setReadIndex(0);
        blackhole.consume(decoder.readObject(buffer, decoderState));
    }

    public static void main(String[] args) throws RunnerException {
        runBenchmark(DispositionBenchmark.class);
    }
}
