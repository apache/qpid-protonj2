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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.ReceiverOptions;
import org.apache.qpid.protonj2.client.SessionOptions;
import org.apache.qpid.protonj2.client.SourceOptions;
import org.apache.qpid.protonj2.client.StreamReceiverOptions;
import org.apache.qpid.protonj2.client.TargetOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.engine.Receiver;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Outcome;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.protonj2.types.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.types.transport.SenderSettleMode;

/**
 * Session owned builder of {@link Receiver} objects.
 */
final class ClientReceiverBuilder {

    private final ClientSession session;
    private final SessionOptions sessionOptions;
    private final AtomicInteger receiverCounter = new AtomicInteger();

    private ReceiverOptions defaultReceivernOptions;
    private StreamReceiverOptions defaultStreamReceiverOptions;

    public ClientReceiverBuilder(ClientSession session) {
        this.session = session;
        this.sessionOptions = session.options();
    }

    public ClientReceiver receiver(String address, ReceiverOptions receiverOptions) throws ClientException {
        final ReceiverOptions rcvOptions = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();
        final Receiver protonReceiver = createReceiver(address, rcvOptions, receiverId);

        protonReceiver.setSource(createSource(address, rcvOptions));
        protonReceiver.setTarget(createTarget(address, rcvOptions));

        return new ClientReceiver(session, rcvOptions, receiverId, protonReceiver);
    }

    public ClientReceiver durableReceiver(String address, String subscriptionName, ReceiverOptions receiverOptions) {
        final ReceiverOptions options = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();

        options.linkName(subscriptionName);

        final Receiver protonReceiver = createReceiver(address, options, receiverId);

        protonReceiver.setSource(createDurableSource(address, options));
        protonReceiver.setTarget(createTarget(address, options));

        return new ClientReceiver(session, options, receiverId, protonReceiver);
    }

    public ClientReceiver dynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        final ReceiverOptions options = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();
        final Receiver protonReceiver = createReceiver(null, options, receiverId);

        protonReceiver.setSource(createSource(null, options));
        protonReceiver.setTarget(createTarget(null, options));

        // Configure the dynamic nature of the source now.
        protonReceiver.getSource().setDynamic(true);
        protonReceiver.getSource().setDynamicNodeProperties(ClientConversionSupport.toSymbolKeyedMap(dynamicNodeProperties));

        return new ClientReceiver(session, options, receiverId, protonReceiver);
    }

    public ClientStreamReceiver streamReceiver(String address, StreamReceiverOptions receiverOptions) throws ClientException {
        final StreamReceiverOptions options = receiverOptions != null ? receiverOptions : getDefaultStreamReceiverOptions();
        final String receiverId = nextReceiverId();
        final Receiver protonReceiver = createReceiver(address, options, receiverId);

        protonReceiver.setSource(createDurableSource(address, options));
        protonReceiver.setTarget(createTarget(address, options));

        return new ClientStreamReceiver(session, options, receiverId, protonReceiver);
    }

    private String nextReceiverId() {
        return session.id() + ":" + receiverCounter.incrementAndGet();
    }

    private Receiver createReceiver(String address, ReceiverOptions options, String receiverId) {
        final String linkName;

        if (options.linkName() != null) {
            linkName = options.linkName();
        } else {
            linkName = "receiver-" + receiverId;
        }

        final Receiver protonReceiver = session.getProtonSession().receiver(linkName);

        switch (options.deliveryMode()) {
            case AT_MOST_ONCE:
                protonReceiver.setSenderSettleMode(SenderSettleMode.SETTLED);
                protonReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
                break;
            case AT_LEAST_ONCE:
                protonReceiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);
                protonReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
                break;
        }

        protonReceiver.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.offeredCapabilities()));
        protonReceiver.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.desiredCapabilities()));
        protonReceiver.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.properties()));
        protonReceiver.setDefaultDeliveryState(Released.getInstance());

        return protonReceiver;
    }

    private Source createSource(String address, ReceiverOptions options) {
        final SourceOptions sourceOptions = options.sourceOptions();

        Source source = new Source();
        source.setAddress(address);
        if (sourceOptions.durabilityMode() != null) {
            source.setDurable(TerminusDurability.valueOf(sourceOptions.durabilityMode().name()));
        } else {
            source.setDurable(TerminusDurability.NONE);
        }
        if (sourceOptions.expiryPolicy() != null) {
            source.setExpiryPolicy(TerminusExpiryPolicy.valueOf(sourceOptions.expiryPolicy().name()));
        } else {
            source.setExpiryPolicy(TerminusExpiryPolicy.LINK_DETACH);
        }
        if (sourceOptions.distributionMode() != null) {
            source.setDistributionMode(Symbol.valueOf(sourceOptions.distributionMode().name()));
        }
        if (sourceOptions.timeout() >= 0) {
            source.setTimeout(UnsignedInteger.valueOf(sourceOptions.timeout()));
        }

        source.setOutcomes(ClientConversionSupport.outcomesToSymbols(sourceOptions.outcomes()));
        source.setDefaultOutcome((Outcome) ClientDeliveryState.asProtonType(sourceOptions.defaultOutcome()));
        source.setCapabilities(ClientConversionSupport.toSymbolArray(sourceOptions.capabilities()));

        return source;
    }

    private Source createDurableSource(String address, ReceiverOptions options) {
        final SourceOptions sourceOptions = options.sourceOptions();

        Source source = new Source();
        source.setAddress(address);
        source.setDurable(TerminusDurability.UNSETTLED_STATE);
        source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
        source.setDistributionMode(ClientConstants.COPY);
        source.setOutcomes(ClientConversionSupport.outcomesToSymbols(sourceOptions.outcomes()));
        source.setDefaultOutcome((Outcome) ClientDeliveryState.asProtonType(sourceOptions.defaultOutcome()));
        source.setCapabilities(ClientConversionSupport.toSymbolArray(sourceOptions.capabilities()));
        if (sourceOptions.timeout() >= 0) {
            source.setTimeout(UnsignedInteger.valueOf(sourceOptions.timeout()));
        }

        return source;
    }

    private Target createTarget(String address, ReceiverOptions options) {
        final TargetOptions targetOptions = options.targetOptions();

        // TODO: fully configure target from the options
        Target target = new Target();

        target.setCapabilities(ClientConversionSupport.toSymbolArray(targetOptions.capabilities()));

        if (targetOptions.timeout() >= 0) {
            target.setTimeout(UnsignedInteger.valueOf(targetOptions.timeout()));
        }

        return target;
    }

    /*
     * Receiver options used when none specified by the caller creating a new receiver.
     */
    private ReceiverOptions getDefaultReceiverOptions() {
        ReceiverOptions receiverOptions = defaultReceivernOptions;
        if (receiverOptions == null) {
            synchronized (this) {
                receiverOptions = defaultReceivernOptions;
                if (receiverOptions == null) {
                    receiverOptions = new ReceiverOptions();
                    receiverOptions.openTimeout(sessionOptions.openTimeout());
                    receiverOptions.closeTimeout(sessionOptions.closeTimeout());
                    receiverOptions.requestTimeout(sessionOptions.requestTimeout());
                    receiverOptions.drainTimeout(sessionOptions.drainTimeout());
                }

                defaultReceivernOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }

    /*
     * Stream Receiver options used when none specified by the caller creating a new receiver.
     */
    private StreamReceiverOptions getDefaultStreamReceiverOptions() {
        StreamReceiverOptions receiverOptions = defaultStreamReceiverOptions;
        if (receiverOptions == null) {
            synchronized (this) {
                receiverOptions = defaultStreamReceiverOptions;
                if (receiverOptions == null) {
                    receiverOptions = new StreamReceiverOptions();
                    receiverOptions.openTimeout(sessionOptions.openTimeout());
                    receiverOptions.closeTimeout(sessionOptions.closeTimeout());
                    receiverOptions.requestTimeout(sessionOptions.requestTimeout());
                    receiverOptions.drainTimeout(sessionOptions.drainTimeout());
                }

                defaultStreamReceiverOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }
}
