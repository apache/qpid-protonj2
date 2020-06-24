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

import org.apache.qpid.protonj2.types.Symbol;

/**
 * Constants that are used throughout the client implementation.
 */
public class ClientConstants {

    // Symbols used to announce connection error information
    public static final Symbol CONNECTION_OPEN_FAILED = Symbol.valueOf("amqp:connection-establishment-failed");
    public static final Symbol INVALID_FIELD = Symbol.valueOf("invalid-field");
    public static final Symbol CONTAINER_ID = Symbol.valueOf("container-id");

    // Symbols used for connection capabilities
    public static final Symbol SOLE_CONNECTION_CAPABILITY = Symbol.valueOf("sole-connection-for-container");
    public static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    public static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");
    public static final Symbol SHARED_SUBS = Symbol.valueOf("SHARED-SUBS");

    // Symbols used to announce connection redirect ErrorCondition 'info'
    public static final Symbol PATH = Symbol.valueOf("path");
    public static final Symbol SCHEME = Symbol.valueOf("scheme");
    public static final Symbol PORT = Symbol.valueOf("port");
    public static final Symbol NETWORK_HOST = Symbol.valueOf("network-host");
    public static final Symbol OPEN_HOSTNAME = Symbol.valueOf("hostname");

    // Symbols used for receivers.
    public static final Symbol COPY = Symbol.getSymbol("copy");
    public static final Symbol SHARED = Symbol.valueOf("shared");
    public static final Symbol GLOBAL = Symbol.valueOf("global");

}
