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

import static org.messaginghub.amqperative.impl.ClientConstants.MODIFIED_FAILED;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.engine.Receiver;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.SessionOptions;
import org.messaginghub.amqperative.SourceOptions;

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

        return new ClientReceiver(session, rcvOptions, receiverId, protonReceiver);
    }

    public ClientReceiver dynamicReceiver(Map<String, Object> dynamicNodeProperties, ReceiverOptions receiverOptions) throws ClientException {
        final ReceiverOptions rcvOptions = receiverOptions != null ? receiverOptions : getDefaultReceiverOptions();
        final String receiverId = nextReceiverId();

        final Receiver protonReceiver = createReceiver(null, rcvOptions, receiverId);

        protonReceiver.getSource().setDynamic(true);
        protonReceiver.getSource().setDynamicNodeProperties(ClientConversionSupport.toSymbolKeyedMap(dynamicNodeProperties));

        return new ClientReceiver(session, rcvOptions, receiverId, protonReceiver);
    }

    private String nextReceiverId() {
        return session.id() + ":" + receiverCounter.incrementAndGet();
    }

    private Receiver createReceiver(String address, ReceiverOptions options, String receiverId) {
        final String linkName;

        if (options.getLinkName() != null) {
            linkName = options.getLinkName();
        } else {
            linkName = "receiver-" + receiverId;
        }

        final Receiver protonReceiver = session.getProtonSession().receiver(linkName);

        protonReceiver.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(options.getOfferedCapabilities()));
        protonReceiver.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(options.getDesiredCapabilities()));
        protonReceiver.setProperties(ClientConversionSupport.toSymbolKeyedMap(options.getProperties()));
        protonReceiver.setSource(createSource(address, options));
        protonReceiver.setTarget(createTarget(address, options));
        protonReceiver.setDefaultDeliveryState(Released.getInstance());

        return protonReceiver;
    }

    private Source createSource(String address, ReceiverOptions options) {
        final SourceOptions sourceOptions = options.sourceOptions();

        //TODO: flesh out source configuration
        Source source = new Source();
        source.setAddress(address);
        // TODO - User somehow sets their own desired outcomes for this receiver source.
        source.setOutcomes(ClientConversionSupport.outcomesToSymbols(sourceOptions.outcomes()));
        source.setDefaultOutcome(MODIFIED_FAILED);  // TODO set from source options.

        return source;
    }

    private Target createTarget(String address, ReceiverOptions options) {
        return new Target();
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
                    receiverOptions.setOpenTimeout(sessionOptions.getOpenTimeout());
                    receiverOptions.setCloseTimeout(sessionOptions.getCloseTimeout());
                    receiverOptions.setRequestTimeout(sessionOptions.getRequestTimeout());
                    receiverOptions.setSendTimeout(sessionOptions.getSendTimeout());
                }

                defaultReceivernOptions = receiverOptions;
            }
        }

        return receiverOptions;
    }
}
