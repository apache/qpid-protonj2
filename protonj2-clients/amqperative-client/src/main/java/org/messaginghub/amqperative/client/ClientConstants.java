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

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;

/**
 * Constants that are used throughout the client implemenatation.
 */
public class ClientConstants {

    public static final Symbol[] DEFAULT_SUPPORTED_OUTCOMES = new Symbol[]{ Accepted.DESCRIPTOR_SYMBOL, Rejected.DESCRIPTOR_SYMBOL,
                                                                            Released.DESCRIPTOR_SYMBOL, Modified.DESCRIPTOR_SYMBOL };

    // Symbols used for connection capabilities
    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");

    // Delivery states
    public static final Rejected REJECTED = new Rejected();
    public static final Modified MODIFIED_FAILED = new Modified();
    public static final Modified MODIFIED_FAILED_UNDELIVERABLE = new Modified();

    //----- Static initializer for constants that need configuration

    static {
        MODIFIED_FAILED.setDeliveryFailed(true);

        MODIFIED_FAILED_UNDELIVERABLE.setDeliveryFailed(true);
        MODIFIED_FAILED_UNDELIVERABLE.setUndeliverableHere(true);
    }

}
