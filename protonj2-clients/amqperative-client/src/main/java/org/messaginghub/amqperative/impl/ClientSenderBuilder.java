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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.engine.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.SourceOptions;
import org.messaginghub.amqperative.TargetOptions;

/**
 * Session owned builder of {@link Sender} objects.
 */
final class ClientSenderBuilder {

    private final ClientSession session;
    private final SessionOptions sessionOptions;
    private final AtomicInteger senderCounter = new AtomicInteger();

    private SenderOptions defaultSenderOptions;

    public ClientSenderBuilder(ClientSession session) {
        this.session = session;
        this.sessionOptions = session.options();
    }

    public ClientSender sender(String address, SenderOptions senderOptions) throws ClientException {
        final SenderOptions options = senderOptions != null ? senderOptions : getDefaultSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(address, options, senderId);

        return new ClientSender(session, options, senderId, protonSender);
    }

    public ClientSender anonymousSender(SenderOptions senderOptions) throws ClientException {
        final SenderOptions options = senderOptions != null ? senderOptions : getDefaultSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(null, options, senderId);

        return new ClientSender(session, options, senderId, protonSender);
    }

    public ClientConnectionSender connectionSender() {
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(null, getDefaultSenderOptions(), senderId);

        return new ClientConnectionSender(session, getDefaultSenderOptions(), senderId, protonSender);
    }

    private Sender createSender(String address, SenderOptions options, String senderId) {
        final String linkName;

        if (options.linkName() != null) {
            linkName = options.linkName();
        } else {
            linkName = "receiver-" + senderId;
        }

        final Sender protonSender = session.getProtonSession().sender(linkName);

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
        protonSender.setSource(createSource(address, options));

        return protonSender;
    }

    private Source createSource(String address, SenderOptions options) {
        final SourceOptions sourceOptions = options.sourceOptions();

        // TODO: fully configure source from the options
        final Source source = new Source();
        source.setOutcomes(ClientConversionSupport.outcomesToSymbols(sourceOptions.outcomes()));

        return source;
    }

    private Target createTarget(String address, SenderOptions options) {
        final TargetOptions targetOptions = options.targetOptions();

        // TODO: fully configure target from the options
        final Target target = new Target();

        target.setAddress(address);
        target.setCapabilities(ClientConversionSupport.toSymbolArray(targetOptions.capabilities()));

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
}
