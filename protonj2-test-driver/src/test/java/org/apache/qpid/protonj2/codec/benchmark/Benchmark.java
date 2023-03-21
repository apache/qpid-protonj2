/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.protonj2.codec.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.codec.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Header;
import org.apache.qpid.protonj2.test.driver.codec.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Properties;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.Disposition;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

public class Benchmark implements Runnable {

    private static final int ITERATIONS = 10 * 1024 * 1024;

    private ByteArrayOutputStream output = new ByteArrayOutputStream(8192);
    private BenchmarkResult resultSet = new BenchmarkResult();
    private boolean warming = true;

    private Codec codec = Codec.Factory.create();

    public static final void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Current PID: " + ManagementFactory.getRuntimeMXBean().getName());
        Benchmark benchmark = new Benchmark();
        benchmark.run();
    }

    @Override
    public void run() {
        try {
            doBenchmarks();
            warming = false;
            doBenchmarks();
        } catch (IOException e) {
            System.out.println("Unexpected error: " + e.getMessage());
        }
    }

    private void time(String message, BenchmarkResult resultSet) {
        if (!warming) {
            System.out.println("Benchmark of type: " + message + ": ");
            System.out.println("    Encode time = " + resultSet.getEncodeTimeMills());
            System.out.println("    Decode time = " + resultSet.getDecodeTimeMills());
        }
    }

    private final void doBenchmarks() throws IOException {
        benchmarkListOfInts();
        benchmarkUUIDs();
        benchmarkHeader();
        benchmarkProperties();
        benchmarkMessageAnnotations();
        benchmarkApplicationProperties();
        benchmarkSymbols();
        benchmarkTransfer();
        benchmarkFlow();
        benchmarkDisposition();
        benchmarkString();
        benchmarkData();
    }

    private void benchmarkListOfInts() throws IOException {
        ArrayList<Object> list = new ArrayList<>(10);
        for (int j = 0; j < 10; j++) {
            list.add(0);
        }

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putJavaList(list);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("List<Integer>", resultSet);
    }

    private void benchmarkUUIDs() throws IOException {
        UUID uuid = UUID.randomUUID();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putUUID(uuid);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("UUID", resultSet);
    }

    private void benchmarkTransfer() throws IOException {
        Transfer transfer = new Transfer();
        transfer.setDeliveryTag(new Binary(new byte[] {1, 2, 3}));
        transfer.setHandle(UnsignedInteger.valueOf(1024));
        transfer.setMessageFormat(UnsignedInteger.valueOf(0));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(transfer);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Transfer", resultSet);
    }

    private void benchmarkFlow() throws IOException {
        Flow flow = new Flow();
        flow.setNextIncomingId(UnsignedInteger.valueOf(1));
        flow.setIncomingWindow(UnsignedInteger.valueOf(2047));
        flow.setNextOutgoingId(UnsignedInteger.valueOf(1));
        flow.setOutgoingWindow(UnsignedInteger.MAX_VALUE);
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.valueOf(10));
        flow.setLinkCredit(UnsignedInteger.valueOf(1000));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(flow);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Flow", resultSet);
    }

    private void benchmarkHeader() throws IOException {
        Header header = new Header();
        header.setDurable(true);
        header.setFirstAcquirer(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(header);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Header", resultSet);
    }

    private void benchmarkProperties() throws IOException {
        Properties properties = new Properties();
        properties.setTo("queue:1-1024");
        properties.setReplyTo("queue:1-11024-reply");
        properties.setMessageId("ID:255f1297-5a71-4df1-8147-b2cdf850a56f:1");
        properties.setCreationTime(new Date(System.currentTimeMillis()));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(properties);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Properties", resultSet);
    }

    private void benchmarkMessageAnnotations() throws IOException {
        MessageAnnotations annotations = new MessageAnnotations();
        annotations.getDescribed().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));
        annotations.getDescribed().put(Symbol.valueOf("test2"), UnsignedShort.valueOf((short) 128));
        annotations.getDescribed().put(Symbol.valueOf("test3"), UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(annotations);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("MessageAnnotations", resultSet);
    }

    private void benchmarkApplicationProperties() throws IOException {
        ApplicationProperties properties = new ApplicationProperties();
        properties.getDescribed().put("test1", UnsignedByte.valueOf((byte) 128));
        properties.getDescribed().put("test2", UnsignedShort.valueOf((short) 128));
        properties.getDescribed().put("test3", UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(properties);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("ApplicationProperties", resultSet);
    }

    private void benchmarkSymbols() throws IOException {
        Symbol symbol1 = Symbol.valueOf("Symbol-1");
        Symbol symbol2 = Symbol.valueOf("Symbol-2");
        Symbol symbol3 = Symbol.valueOf("Symbol-3");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putSymbol(symbol1);
            codec.putSymbol(symbol2);
            codec.putSymbol(symbol3);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Symbol", resultSet);
    }

    private void benchmarkString() throws IOException {
        String string1 = new String("String-1-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string2 = new String("String-2-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string3 = new String("String-3-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putString(string1);
            codec.putString(string2);
            codec.putString(string3);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.decode(buffer);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("String", resultSet);
    }

    private void benchmarkDisposition() throws IOException {
        Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER.getValue());
        disposition.setSettled(true);
        disposition.setState(new Accepted());
        disposition.setFirst(UnsignedInteger.valueOf(2));
        disposition.setLast(UnsignedInteger.valueOf(2));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(disposition);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Disposition", resultSet);
    }

    private void benchmarkData() throws IOException {
        Data data1 = new Data(new Binary(new byte[] {1, 2, 3}));
        Data data2 = new Data(new Binary(new byte[] {4, 5, 6}));
        Data data3 = new Data(new Binary(new byte[] {7, 8, 9}));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            output.reset();
            codec.putDescribedType(data1);
            codec.putDescribedType(data2);
            codec.putDescribedType(data3);
            codec.encode(output);
            codec.clear();
        }
        resultSet.encodesComplete();

        final ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray()).asReadOnlyBuffer();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.position(0);
            codec.decode(buffer);
            codec.clear();
        }
        resultSet.decodesComplete();

        time("Data", resultSet);
    }

    private static class BenchmarkResult {

        private long startTime;

        private long encodeTime;
        private long decodeTime;

        public void start() {
            startTime = System.nanoTime();
        }

        public void encodesComplete() {
            encodeTime = System.nanoTime() - startTime;
        }

        public void decodesComplete() {
            decodeTime = System.nanoTime() - startTime;
        }

        public long getEncodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(encodeTime);
        }

        public long getDecodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(decodeTime);
        }
    }
}
