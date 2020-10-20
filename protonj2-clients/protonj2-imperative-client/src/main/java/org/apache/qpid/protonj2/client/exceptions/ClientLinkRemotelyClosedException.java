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
package org.apache.qpid.protonj2.client.exceptions;

import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;

/**
 * Root exception type for cases of remote closure or client created {@link Sender} or
 * {@link Receiver}.
 */
public class ClientLinkRemotelyClosedException extends ClientResourceRemotelyClosedException {

    private static final long serialVersionUID = 5601827103553513599L;

    public ClientLinkRemotelyClosedException(String message) {
        this(message, (ErrorCondition) null);
    }

    public ClientLinkRemotelyClosedException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public ClientLinkRemotelyClosedException(String message, ErrorCondition condition) {
        super(message, condition);
    }

    public ClientLinkRemotelyClosedException(String message, Throwable cause, ErrorCondition condition) {
        super(message, cause, condition);
    }
}
