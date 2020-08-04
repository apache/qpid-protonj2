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
package org.apache.qpid.protonj2.client;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * A receive context allows for partial reads of incoming delivery data before
 * the complete message has arrived.  Requesting a {@link ReceiveContext} from
 * a {@link Receiver} will return a new context for the next available delivery
 * if one is available or block until one becomes available.  The resulting
 * context may comprise a partial message or could reference a completed message
 * if all frames of the delivery have already arrived from the remote peer.
 */
public interface ReceiveContext {

    /**
     * @return the associated Delivery object once a read has completed.
     */
    Delivery delivery();

    /**
     * Decode the {@link Delivery} payload and return an {@link Message} object if there
     * is a {@link Delivery} associated with this {@link ReceiveContext} and it has reached
     * the completed state.
     *
     * @return a {@link Message} instance that wraps the decoded payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     *
     * @param <E> The type of message body that should be contained in the returned {@link Message}.
     */
    <E> Message<E> message() throws ClientException;

    /**
     * @return true if this context has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * @return true if this context has been marked as being the complete.
     */
    boolean complete();

    /**
     * Creates an {@link RawInputStream} instance configured with the given options.
     * <p>
     * The {@link RawInputStream} can be used to read the payload of an AMQP Message in
     * chunks as it arrives from the remote peer.  The bytes read are the raw encoded
     * bytes of the AMQP {@link Transfer} frames and the caller is responsible for
     * the decoding and processing of those bytes.
     *
     * @param options
     *      The stream options to use to configure the returned {@link RawInputStream}
     *
     * @return a {@link RawInputStream} instance configured using the given options.
     */
    RawInputStream inputStream(RawInputStreamOptions options);

}
