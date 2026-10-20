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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedShort;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.types.security.SaslChallenge;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.apache.qpid.protonj2.types.security.SaslInit;
import org.apache.qpid.protonj2.types.security.SaslMechanisms;
import org.apache.qpid.protonj2.types.security.SaslOutcome;
import org.apache.qpid.protonj2.types.security.SaslResponse;
import org.apache.qpid.protonj2.types.transactions.Coordinator;
import org.apache.qpid.protonj2.types.transactions.Declare;
import org.apache.qpid.protonj2.types.transactions.Declared;
import org.apache.qpid.protonj2.types.transactions.Discharge;
import org.apache.qpid.protonj2.types.transactions.TransactionalState;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.End;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Role;
import org.apache.qpid.protonj2.types.transport.Transfer;

public class Benchmark implements Runnable {

    private static final int ITERATIONS = 10 * 1024 * 1024;

    ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().allocate(8192);
    private BenchmarkResult resultSet = new BenchmarkResult();
    private boolean warming = true;

    private Encoder encoder = CodecFactory.getDefaultEncoder();
    private Encoder saslEncoder = CodecFactory.getDefaultSaslEncoder();
    private EncoderState encoderState = encoder.newEncoderState();
    private Decoder decoder = CodecFactory.getDefaultDecoder();
    private Decoder saslDecoder = CodecFactory.getSaslDecoder();
    private DecoderState decoderState = decoder.newDecoderState();

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
            resultSet.reset();
            doBenchmarks();

            totalTime(resultSet);
        } catch (IOException e) {
            System.out.println("Unexpected error: " + e.getMessage());
        }
    }

    private void time(String message, BenchmarkResult resultSet) {
        if (!warming) {
            System.out.println("Benchmark of type: " + message + ": ");
            System.out.println("    Encode time = " + resultSet.getEncodeTimeMills() + " ms");
            System.out.println("    Decode time = " + resultSet.getDecodeTimeMills() + " ms");
        }
    }

    private void totalTime(BenchmarkResult resultSet) {
        if (!warming) {
            System.out.println("------------------------------------------");
            System.out.println("Total Encode time = " + resultSet.getTotalEncodeTimeMills() + " ms");
            System.out.println("Total Decode time = " + resultSet.getTotalDecodeTimeMills() + " ms");
        }
    }

    private final void doBenchmarks() throws IOException {
        // Primitives
        benchmarkListOfInts();
        benchmarkMap();
        benchmarkString();
        benchmarkSymbols();
        benchmarkUUIDs();

        // Messaging
        benchmarkAccepted();
        benchmarkApplicationProperties();
        benchmarkData();
        benchmarkDeliveryAnnotations();
        benchmarkFooter();
        benchmarkHeader();
        benchmarkMessageAnnotations();
        benchmarkModified();
        benchmarkProperties();
        benchmarkSource();
        benchmarkTarget();

        // SASL
        benchmarkSaslChallenge();
        benchmarkSaslInit();
        benchmarkSaslMechanisms();
        benchmarkSaslOutcome();
        benchmarkSaslResponse();

        // Transactions
        benchmarkCoordinator();
        benchmarkDeclared();
        benchmarkDeclare();
        benchmarkDischarge();
        benchmarkTransactionalState();

        // Transport
        benchmarkAttach();
        benchmarkBegin();
        benchmarkClose();
        benchmarkDetach();
        benchmarkDisposition();
        benchmarkEnd();
        benchmarkErrorCondition();
        benchmarkFlow();
        benchmarkOpen();
        benchmarkTransfer();

        warming = false;
    }

    // AMQP Primitives benchmarks

    private void benchmarkListOfInts() throws IOException {
        ArrayList<Object> list = new ArrayList<>(10);
        for (int j = 0; j < 10; j++) {
            list.add(0);
        }

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeList(buffer, encoderState, list);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readList(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("List<Integer>", resultSet);
    }

    private void benchmarkMap() throws IOException {
        final Map<UUID, UUID> map = new HashMap<>();

        for (int j = 0; j < 10; j++) {
            final UUID key = UUID.randomUUID();
            final UUID value = UUID.randomUUID();

            map.put(key, value);
        }

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeMap(buffer, encoderState, map);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readMap(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Map<UUID, UUID>", resultSet);
    }

    private void benchmarkString() throws IOException {
        String string1 = new String("String-1-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string2 = new String("String-2-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");
        String string3 = new String("String-3-somewhat-long-test-to-validate-performance-improvements-to-the-proton-j-codec-@!%$");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeString(buffer, encoderState, string1);
            encoder.writeString(buffer, encoderState, string2);
            encoder.writeString(buffer, encoderState, string3);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readString(encoded, decoderState);
            decoder.readString(encoded, decoderState);
            decoder.readString(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("String", resultSet);
    }

    private void benchmarkSymbols() throws IOException {
        Symbol symbol1 = Symbol.valueOf("Symbol-1");
        Symbol symbol2 = Symbol.valueOf("Symbol-2");
        Symbol symbol3 = Symbol.valueOf("Symbol-3");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeSymbol(buffer, encoderState, symbol1);
            encoder.writeSymbol(buffer, encoderState, symbol2);
            encoder.writeSymbol(buffer, encoderState, symbol3);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readSymbol(encoded, decoderState);
            decoder.readSymbol(encoded, decoderState);
            decoder.readSymbol(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Symbol", resultSet);
    }

    private void benchmarkUUIDs() throws IOException {
        UUID uuid = UUID.randomUUID();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeUUID(buffer, encoderState, uuid);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readUUID(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("UUID", resultSet);
    }

    // AMQP Messaging types benchmarks

    private void benchmarkAccepted() throws IOException {
        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, Accepted.getInstance());
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Accepted", resultSet);
    }

    private void benchmarkApplicationProperties() throws IOException {
        ApplicationProperties properties = new ApplicationProperties(new HashMap<>());
        properties.getValue().put("test1", UnsignedByte.valueOf((byte) 128));
        properties.getValue().put("test2", UnsignedShort.valueOf((short) 128));
        properties.getValue().put("test3", UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, properties);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("ApplicationProperties", resultSet);
    }

    private void benchmarkData() throws IOException {
        Data data1 = new Data(new byte[] {1, 2, 3});
        Data data2 = new Data(new byte[] {4, 5, 6});
        Data data3 = new Data(new byte[] {7, 8, 9});

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, data1);
            encoder.writeObject(buffer, encoderState, data2);
            encoder.writeObject(buffer, encoderState, data3);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
            decoder.readObject(encoded, decoderState);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Data", resultSet);
    }

    private void benchmarkDeliveryAnnotations() throws IOException {
        DeliveryAnnotations annotations = new DeliveryAnnotations(new HashMap<>());
        annotations.getValue().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, annotations);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("DeliveryAnnotations", resultSet);
    }

    private void benchmarkHeader() throws IOException {
        Header header = new Header();
        header.setDurable(true);
        header.setFirstAcquirer(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, header);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Header", resultSet);
    }

    private void benchmarkMessageAnnotations() throws IOException {
        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(Symbol.valueOf("test2"), UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(Symbol.valueOf("test3"), UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, annotations);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("MessageAnnotations", resultSet);
    }

    private void benchmarkModified() throws IOException {
        Modified modified = new Modified();
        modified.setDeliveryFailed(true);
        modified.setUndeliverableHere(false);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, modified);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Modified", resultSet);
    }

    private void benchmarkFooter() throws IOException {
        Footer footers = new Footer(new HashMap<>());
        footers.getValue().put(Symbol.valueOf("test1"), UnsignedByte.valueOf((byte) 128));
        footers.getValue().put(Symbol.valueOf("test2"), UnsignedShort.valueOf((short) 128));
        footers.getValue().put(Symbol.valueOf("test3"), UnsignedInteger.valueOf((byte) 128));
        footers.getValue().put(Symbol.valueOf("test4"), UnsignedInteger.valueOf((byte) 128));
        footers.getValue().put(Symbol.valueOf("test5"), UnsignedInteger.valueOf((byte) 128));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, footers);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Footer", resultSet);
    }

    private void benchmarkProperties() throws IOException {
        Properties properties = new Properties();
        properties.setUserId(new byte[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 });
        properties.setTo("queue:1-1024");
        properties.setReplyTo("queue:1-11024-reply");
        properties.setMessageId("ID:255f1297-5a71-4df1-8147-b2cdf850a56f:1");
        properties.setCreationTime(System.currentTimeMillis());

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, properties);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Properties", resultSet);
    }

    private void benchmarkSource() throws IOException {
        Source source = new Source();
        source.setAddress("test-address");
        source.setDurable(TerminusDurability.CONFIGURATION);
        source.setCapabilities(Symbol.valueOf("queue"));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, source);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Source", resultSet);
    }

    private void benchmarkTarget() throws IOException {
        Target target = new Target();
        target.setAddress("test-address");
        target.setDurable(TerminusDurability.CONFIGURATION);
        target.setCapabilities(Symbol.valueOf("queue"));
        target.setExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, target);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Target", resultSet);
    }

    // AMQP SASL type benchmarks

    private void benchmarkSaslChallenge() throws IOException {
        SaslChallenge command = new SaslChallenge();
        command.setChallenge(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 }));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            saslEncoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            saslDecoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("SaslChallenge", resultSet);
    }

    private void benchmarkSaslInit() throws IOException {
        SaslInit command = new SaslInit();
        command.setHostname("localhost");
        command.setMechanism(Symbol.valueOf("ANONYMOUS"));
        command.setInitialResponse(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 }));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            saslEncoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            saslDecoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("SaslInit", resultSet);
    }

    private void benchmarkSaslMechanisms() throws IOException {
        SaslMechanisms command = new SaslMechanisms();
        command.setSaslServerMechanisms(Symbol.valueOf("PLAIN") , Symbol.valueOf("ANONYMOUS"));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            saslEncoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            saslDecoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("SaslMechanisms", resultSet);
    }

    private void benchmarkSaslOutcome() throws IOException {
        SaslOutcome command = new SaslOutcome();
        command.setAdditionalData(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 }));
        command.setCode(SaslCode.OK);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            saslEncoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            saslDecoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("SaslOutcome", resultSet);
    }

    private void benchmarkSaslResponse() throws IOException {
        SaslResponse command = new SaslResponse();
        command.setResponse(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 }));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            saslEncoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            saslDecoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("SaslResponse", resultSet);
    }

    // AMQP Transaction type benchmarks

    private void benchmarkCoordinator() throws IOException {
        Coordinator command = new Coordinator();
        command.setCapabilities(Symbol.valueOf("test"));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Coordinator", resultSet);
    }

    private void benchmarkDeclared() throws IOException {
        Declared command = new Declared();
        command.setTxnId(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7}));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Declared", resultSet);
    }

    private void benchmarkDeclare() throws IOException {
        Declare command = new Declare();

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Declare", resultSet);
    }

    private void benchmarkDischarge() throws IOException {
        Discharge command = new Discharge();
        command.setTxnId(new Binary(new byte[] { 1, 2, 3, 4, 5, 6, 7}));
        command.setFail(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, command);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Discharge", resultSet);
    }

    private void benchmarkTransactionalState() throws IOException {
        TransactionalState state = new TransactionalState();
        state.setOutcome(Accepted.getInstance());
        state.setTxnId(new Binary(new byte[] { 0, 1, 2, 3 }));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, state);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("TransactionalState", resultSet);
    }

    // Benchmarks for Transport performatives

    private void benchmarkAttach() throws IOException {
        Attach attach = new Attach();
        attach.setName("test");
        attach.setHandle(10);
        attach.setRole(Role.RECEIVER);
        attach.setSource(new Source());
        attach.setTarget(new Target());
        attach.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("ANONYMOUS") });

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, attach);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Attach", resultSet);
    }

    private void benchmarkBegin() throws IOException {
        Begin begin = new Begin();
        begin.setHandleMax(64);
        begin.setIncomingWindow(1024);
        begin.setNextOutgoingId(0);
        begin.setDesiredCapabilities(Symbol.valueOf("something"));
        begin.setOutgoingWindow(32768);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, begin);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Begin", resultSet);
    }

    private void benchmarkClose() throws IOException {
        Close close = new Close();
        close.setError(new ErrorCondition("test", "test"));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, close);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Close", resultSet);
    }

    private void benchmarkDetach() throws IOException {
        Detach detach = new Detach();
        detach.setClosed(true);
        detach.setHandle(0);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, detach);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Detach", resultSet);
    }

    private void benchmarkDisposition() throws IOException {
        Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setSettled(true);
        disposition.setState(Accepted.getInstance());
        disposition.setFirst(2);
        disposition.setLast(2);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, disposition);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Disposition", resultSet);
    }

    private void benchmarkEnd() throws IOException {
        End end = new End();
        end.setError(new ErrorCondition("test", null));

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, end);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("End", resultSet);
    }

    private void benchmarkErrorCondition() throws IOException {
        ErrorCondition error = new ErrorCondition("TestCondition", "error message");

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, error);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("ErrorCondition", resultSet);
    }

    private void benchmarkFlow() throws IOException {
        Flow flow = new Flow();
        flow.setNextIncomingId(1);
        flow.setIncomingWindow(2047);
        flow.setNextOutgoingId(1);
        flow.setOutgoingWindow(Integer.MAX_VALUE);
        flow.setHandle(UnsignedInteger.ZERO.longValue());
        flow.setDeliveryCount(10);
        flow.setLinkCredit(1000);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, flow);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Flow", resultSet);
    }

    private void benchmarkOpen() throws IOException {
        Open open = new Open();
        open.setChannelMax(64);
        open.setContainerId("clilent");
        open.setHostname("hostname");
        open.setMaxFrameSize(65535);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, open);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Open", resultSet);
    }

    private void benchmarkTransfer() throws IOException {
        Transfer transfer = new Transfer();
        transfer.setDeliveryTag(new byte[] {1, 2, 3});
        transfer.setHandle(1024);
        transfer.setMessageFormat(0);
        transfer.setDeliveryId(1);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            buffer.clear();
            encoder.writeObject(buffer, encoderState, transfer);
        }
        resultSet.encodesComplete();

        final ProtonBuffer encoded = buffer.copy(true);

        resultSet.start();
        for (int i = 0; i < ITERATIONS; i++) {
            encoded.setReadOffset(0);
            decoder.readObject(encoded, decoderState);
        }
        resultSet.decodesComplete();

        time("Transfer", resultSet);
    }

    private static class BenchmarkResult {

        private long startTime;

        private long totalEncodeTime;
        private long totalDecodeTime;

        private long encodeTime;
        private long decodeTime;

        public void start() {
            startTime = System.nanoTime();
        }

        public void encodesComplete() {
            totalEncodeTime += encodeTime = System.nanoTime() - startTime;
        }

        public void decodesComplete() {
            totalDecodeTime += decodeTime = System.nanoTime() - startTime;
        }

        public void reset() {
            totalDecodeTime = totalEncodeTime = 0;
        }

        public long getEncodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(encodeTime);
        }

        public long getDecodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(decodeTime);
        }

        public long getTotalEncodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(totalEncodeTime);
        }

        public long getTotalDecodeTimeMills() {
            return TimeUnit.NANOSECONDS.toMillis(totalDecodeTime);
        }
    }
}
