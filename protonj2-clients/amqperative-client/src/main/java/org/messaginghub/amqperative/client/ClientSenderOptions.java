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

import static org.messaginghub.amqperative.client.ClientConstants.DEFAULT_SUPPORTED_OUTCOMES;

import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.engine.Sender;
import org.messaginghub.amqperative.SenderOptions;

public final class ClientSenderOptions extends SenderOptions {

    public ClientSenderOptions() {
        super();
    }

    public ClientSenderOptions(SenderOptions options) {
        super(options);
    }

    Sender configureSender(Sender protonSender, String address) {
        protonSender.setOfferedCapabilities(ClientConversionSupport.toSymbolArray(getOfferedCapabilities()));
        protonSender.setDesiredCapabilities(ClientConversionSupport.toSymbolArray(getDesiredCapabilities()));
        protonSender.setProperties(ClientConversionSupport.toSymbolKeyedMap(getProperties()));

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
}
