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
package org.messaginghub.amqperative.client;

import org.apache.qpid.proton4j.engine.Session;
import org.messaginghub.amqperative.ReceiverOptions;
import org.messaginghub.amqperative.SenderOptions;
import org.messaginghub.amqperative.SessionOptions;

public final class ClientSessionOptions extends SessionOptions {

    private SenderOptions defaultSenderOptions;
    private ReceiverOptions defaultReceivernOptions;
    private ReceiverOptions defaultDynamicReceivernOptions;

    public ClientSessionOptions() {
        super();
    }

    public ClientSessionOptions(SessionOptions options) {
        super(options);
    }

    //----- Internal support APIs

    Session configureSession(Session protonSession) {
        protonSession.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(getOfferedCapabilities()));
        protonSession.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(getDesiredCapabilities()));
        protonSession.setProperties(ClientConversionSupport.toSymbolKeyedMap(getProperties()));

        return protonSession;
    }

    /*
     * Sender options used when none specified by the caller creating a new sender.
     */
    SenderOptions getDefaultSenderOptions() {
        SenderOptions options = defaultSenderOptions;
        if (options == null) {
            synchronized (this) {
                options = defaultSenderOptions;
                if (options == null) {
                    options = new SenderOptions();
                    options.setOpenTimeout(getOpenTimeout());
                    options.setCloseTimeout(getCloseTimeout());
                    options.setRequestTimeout(getRequestTimeout());
                    options.setSendTimeout(getSendTimeout());
                }

                defaultSenderOptions = options;
            }
        }

        return options;
    }

    /*
     * Receiver options used when none specified by the caller creating a new receiver.
     */
    ReceiverOptions getDefaultReceiverOptions() {
        ReceiverOptions options = defaultReceivernOptions;
        if (options == null) {
            synchronized (this) {
                options = defaultReceivernOptions;
                if (options == null) {
                    options = new ReceiverOptions();
                    options.setOpenTimeout(getOpenTimeout());
                    options.setCloseTimeout(getCloseTimeout());
                    options.setRequestTimeout(getRequestTimeout());
                    options.setSendTimeout(getSendTimeout());
                }

                defaultReceivernOptions = options;
            }
        }

        return options;
    }

    /*
     * Receiver options used when none specified by the caller creating a new dynamic receiver.
     */
    ReceiverOptions getDefaultDynamicReceiverOptions() {
        ReceiverOptions options = defaultDynamicReceivernOptions;
        if (options == null) {
            synchronized (this) {
                options = defaultDynamicReceivernOptions;
                if (options == null) {
                    options = new ReceiverOptions();
                    options.setOpenTimeout(getOpenTimeout());
                    options.setCloseTimeout(getCloseTimeout());
                    options.setRequestTimeout(getRequestTimeout());
                    options.setSendTimeout(getSendTimeout());
                    options.setDynamic(true);
                }

                defaultDynamicReceivernOptions = options;
            }
        }

        return options;
    }
}
