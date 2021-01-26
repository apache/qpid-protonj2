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
package org.apache.qpid.protonj2.client.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.SenderOptions;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.SourceOptions;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.TargetOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.Sender;
import org.apache.qpid.protonj2.engine.Session;
import org.apache.qpid.protonj2.engine.impl.ProtonDeliveryTagGenerator;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * Session owned builder of {@link Sender} objects.
 */
final class ClientSenderBuilder {

    private final ClientSession session;
    private final SessionOptions sessionOptions;
    private final AtomicInteger senderCounter = new AtomicInteger();

    private SenderOptions defaultSenderOptions;
    private StreamSenderOptions defaultStreamSenderOptions;

    public ClientSenderBuilder(ClientSession session) {
        this.session = session;
        this.sessionOptions = session.options();
    }

    public ClientSender sender(String address, SenderOptions senderOptions) throws ClientException {
        final SenderOptions options = senderOptions != null ? senderOptions : getDefaultSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(session.getProtonSession(), address, options, senderId);

        return new ClientSender(session, options, senderId, protonSender);
    }

    public ClientSender anonymousSender(SenderOptions senderOptions) throws ClientException {
        final SenderOptions options = senderOptions != null ? senderOptions : getDefaultSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(session.getProtonSession(), null, options, senderId);

        return new ClientSender(session, options, senderId, protonSender);
    }

    public ClientStreamSender streamSender(String address, StreamSenderOptions senderOptions) throws ClientException {
        final StreamSenderOptions options = senderOptions != null ? senderOptions : getDefaultStreamSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(session.getProtonSession(), address, options, senderId);

        return new ClientStreamSender(session, options, senderId, protonSender);
    }

    private static Sender createSender(Session protonSession, String address, SenderOptions options, String senderId) {
        final String linkName;

        if (options.linkName() != null) {
            linkName = options.linkName();
        } else {
            linkName = "sender-" + senderId;
        }

        final Sender protonSender = protonSession.sender(linkName);

        switch (options.deliveryMode()) {
            case AT_MOST_ONCE:
                protonSender.setSenderSettleMode(SenderSettleMode.SETTLED);
                protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
                break;
            case AT_LEAST_ONCE:
                protonSender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
                protonSender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
                break;
        }

        protonSender.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.offeredCapabilities()));
        protonSender.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.desiredCapabilities()));
        protonSender.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.properties()));
        protonSender.setTarget(createTarget(address, options));
        protonSender.setSource(createSource(senderId, options));

        // Use a tag generator that will reuse old tags.  Later we might make this configurable.
        if (protonSender.getSenderSettleMode() == SenderSettleMode.SETTLED) {
            protonSender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.EMPTY.createGenerator());
        } else {
            protonSender.setDeliveryTagGenerator(ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator());
        }

        return protonSender;
    }

    private static Source createSource(String address, SenderOptions options) {
        final SourceOptions sourceOptions = options.sourceOptions();

        // TODO: fully configure source from the options
        final Source source = new Source();

        source.setAddress(address);
        source.setOutcomes(ClientConversionSupport.outcomesToSymbols(sourceOptions.outcomes()));
        source.setCapabilities(ClientConversionSupport.toSymbolArray(sourceOptions.capabilities()));

        if (sourceOptions.timeout() >= 0) {
            source.setTimeout(UnsignedInteger.valueOf(sourceOptions.timeout()));
        }

        return source;
    }

    private static Target createTarget(String address, SenderOptions options) {
        final TargetOptions targetOptions = options.targetOptions();

        // TODO: fully configure target from the options
        final Target target = new Target();

        target.setAddress(address);
        target.setCapabilities(ClientConversionSupport.toSymbolArray(targetOptions.capabilities()));
        if (targetOptions.durabilityMode() != null) {
            target.setDurable(TerminusDurability.valueOf(targetOptions.durabilityMode().name()));
        }
        if (targetOptions.expiryPolicy() != null) {
            target.setExpiryPolicy(TerminusExpiryPolicy.valueOf(targetOptions.expiryPolicy().name()));
        }
        if (targetOptions.timeout() >= 0) {
            target.setTimeout(UnsignedInteger.valueOf(targetOptions.timeout()));
        }

        return target;
    }

    private String nextSenderId() {
        return session.id() + ":" + senderCounter.incrementAndGet();
    }

    /*
     * Sender options used when none specified by the caller creating a new sender.
     */
    private SenderOptions getDefaultSenderOptions() {
        SenderOptions senderOptions = defaultSenderOptions;
        if (senderOptions == null) {
            synchronized (this) {
                senderOptions = defaultSenderOptions;
                if (senderOptions == null) {
                    senderOptions = new SenderOptions();
                    senderOptions.openTimeout(sessionOptions.openTimeout());
                    senderOptions.closeTimeout(sessionOptions.closeTimeout());
                    senderOptions.requestTimeout(sessionOptions.requestTimeout());
                    senderOptions.sendTimeout(sessionOptions.sendTimeout());
                }

                defaultSenderOptions = senderOptions;
            }
        }

        return senderOptions;
    }

    /*
     * Stream Sender options used when none specified by the caller creating a new sender.
     */
    private StreamSenderOptions getDefaultStreamSenderOptions() {
        StreamSenderOptions senderOptions = defaultStreamSenderOptions;
        if (senderOptions == null) {
            synchronized (this) {
                senderOptions = defaultStreamSenderOptions;
                if (senderOptions == null) {
                    senderOptions = new StreamSenderOptions();
                    senderOptions.openTimeout(sessionOptions.openTimeout());
                    senderOptions.closeTimeout(sessionOptions.closeTimeout());
                    senderOptions.requestTimeout(sessionOptions.requestTimeout());
                    senderOptions.sendTimeout(sessionOptions.sendTimeout());
                }

                defaultStreamSenderOptions = senderOptions;
            }
        }

        return senderOptions;
    }
}
