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

import static org.messaginghub.amqperative.impl.ClientConstants.DEFAULT_SUPPORTED_OUTCOMES;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.engine.Sender;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.SessionOptions;

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

    String nextSenderId() {
        return session.id() + ":" + senderCounter.incrementAndGet();
    }

    public ClientSender sender(String address, SenderOptions senderOptions) throws ClientException {
        final SenderOptions options = senderOptions != null ? senderOptions : getDefaultSenderOptions();
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(address, options, senderId);

        return new ClientSender(session, options, senderId, protonSender);
    }

    public ClientSender anonymousSender(SenderOptions senderOptions) throws ClientException {
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(null, senderOptions, senderId);

        return new ClientSender(session, senderOptions, senderId, protonSender);
    }

    public ClientConnectionSender connectionSender() {
        final String senderId = nextSenderId();
        final Sender protonSender = createSender(null, getDefaultSenderOptions(), senderId);

        return new ClientConnectionSender(session, getDefaultSenderOptions(), senderId, protonSender);
    }

    private Sender createSender(String address, SenderOptions options, String senderId) {
        Sender protonSender;

        if (options.getLinkName() != null) {
            protonSender = session.getProtonSession().sender(options.getLinkName());
        } else {
            protonSender = session.getProtonSession().sender("sender-" + nextSenderId());
        }

        protonSender.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.getOfferedCapabilities()));
        protonSender.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.getDesiredCapabilities()));
        protonSender.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.getProperties()));

        // TODO: flesh out target
        Target target = new Target();
        target.setAddress(address);

        Source source = new Source();
        // TODO - User somehow sets their own desired outcomes for this receiver source.
        source.setOutcomes(DEFAULT_SUPPORTED_OUTCOMES);

        protonSender.setTarget(target);
        protonSender.setSource(source);

        return protonSender;
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
                    senderOptions.setOpenTimeout(sessionOptions.getOpenTimeout());
                    senderOptions.setCloseTimeout(sessionOptions.getCloseTimeout());
                    senderOptions.setRequestTimeout(sessionOptions.getRequestTimeout());
                    senderOptions.setSendTimeout(sessionOptions.getSendTimeout());
                }

                defaultSenderOptions = senderOptions;
            }
        }

        return senderOptions;
    }
}
