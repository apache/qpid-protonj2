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
package org.messaginghub.amqperative.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.engine.Receiver;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.messaging.Outcome;
import org.apache.qpid.proton4j.types.messaging.Released;
import org.apache.qpid.proton4j.types.messaging.Source;
import org.apache.qpid.proton4j.types.messaging.Target;
import org.apache.qpid.proton4j.types.messaging.TerminusDurability;
import org.apache.qpid.proton4j.types.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.types.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.types.transport.SenderSettleMode;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.SourceOptions;
import org.messaginghub.amqperative.TargetOptions;
import org.messaginghub.amqperative.exceptions.ClientException;

/**
 * Session owned builder of {@link Receiver} objects.
 */
final class ClientReceiverBuilder {

    private final ClientSession session;
    private final SessionOptions sessionOptions;
    private final AtomicInteger receiverCounter = new AtomicInteger();

    private ReceiverOptions defaultReceivernOptions;

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
        final ReceiverOptions rcvOptions = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();

        rcvOptions.linkName(subscriptionName);

        final Receiver protonReceiver = createReceiver(address, rcvOptions, receiverId);

        protonReceiver.setSource(createDurableSource(address, rcvOptions));
        protonReceiver.setTarget(createTarget(address, rcvOptions));

        return new ClientReceiver(session, rcvOptions, receiverId, protonReceiver);
    }

    public ClientReceiver dynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        final ReceiverOptions rcvOptions = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();

        final Receiver protonReceiver = createReceiver(null, rcvOptions, receiverId);

        protonReceiver.setSource(createSource(null, rcvOptions));
        protonReceiver.setTarget(createTarget(null, rcvOptions));

        // Configure the dynamic nature of the source now.
        protonReceiver.getSource().setDynamic(true);
        protonReceiver.getSource().setDynamicNodeProperties(ClientConversionSupport.toSymbolKeyedMap(dynamicNodeProperties));

        return new ClientReceiver(session, rcvOptions, receiverId, protonReceiver);
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
        }
        if (sourceOptions.expiryPolicy() != null) {
            source.setExpiryPolicy(TerminusExpiryPolicy.valueOf(sourceOptions.expiryPolicy().name()));
        }
        if (sourceOptions.distributionMode() != null) {
            source.setDistributionMode(Symbol.valueOf(sourceOptions.distributionMode().name()));
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

        return source;
    }

    private Target createTarget(String address, ReceiverOptions options) {
        final TargetOptions targetOptions = options.targetOptions();

        // TODO: fully configure target from the options
        Target target = new Target();

        target.setCapabilities(ClientConversionSupport.toSymbolArray(targetOptions.capabilities()));

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
                    receiverOptions.sendTimeout(sessionOptions.sendTimeout());
                }

                defaultReceivernOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }
}
