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

import java.io.InputStream;
import java.util.Map;

import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * A specialized {@link Message} type that represents a streamed delivery possibly
 * spanning many incoming {@link Transfer} frames from the remote peer.  It is possible
 * for various calls in this {@link StreamReceiverMessage} to block while awaiting the
 * receipt of sufficient bytes to provide the result.
 */
public interface StreamReceiverMessage {

    /**
     * @return the {@link StreamReceiver} that this context was create under.
     */
    StreamReceiver receiver();

    /**
     * Decodes the {@link StreamReceiverMessage} payload and returns a {@link Map} containing a copy
     * of any associated {@link DeliveryAnnotations} that were transmitted with the {@link Message}
     * payload of this {@link StreamReceiverMessage}.
     *
     * @return copy of the delivery annotations that were transmitted with the {@link Message} payload.
     *
     * @throws ClientException if an error occurs while decoding the payload.
     */
    Map<String, Object> annotations() throws ClientException;

    /**
     * Check if the {@link Delivery} that was assigned to this {@link ReceiveContext} has been
     * marked as aborted by the remote.  If there is not yet a {@link Delivery} associated with
     * this context then this method returns <code>false</code> without blocking.
     *
     * @return true if this context has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Check if the {@link Delivery} that was assigned to this {@link ReceiveContext} has been
     * marked as complete by the remote.  If there is not yet a {@link Delivery} associated with
     * this context then this method returns <code>false</code> without blocking.
     *
     * @return true if this context has been marked as being the complete.
     */
    boolean completed();

    /**
     * Creates an {@link InputStream} instance configured with the given options that will
     * read the bytes delivered without additional decoding or transformation.
     * <p>
     * The returned {@link InputStream} can be used to read the payload of an AMQP Message
     * in chunks as it arrives from the remote peer.  The bytes read are the raw encoded
     * bytes of the AMQP {@link Transfer} frames and the caller is responsible for
     * the decoding and processing of those bytes.
     * <p>
     * Calls to read bytes from the returned {@link InputStream} when there are no bytes
     * available to read will block until there is available data from additional transfers
     * from the remote or until the remote aborts or completes the transfer.  Users can check
     * the stream {@link InputStream#available()} method to determine if any bytes are locally
     * ready for consumption.
     *
     * @return a {@link InputStream} instance configured using the context options.
     *
     * @throws ClientException if an error occurs while creating the input stream.
     */
    InputStream rawInputStream() throws ClientException;

    /**
     * Accepts and settles the delivery.
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage accept() throws ClientException;

    /**
     * Releases and settles the delivery.
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage release() throws ClientException;

    /**
     * Rejects and settles the delivery, sending supplied error information along
     * with the rejection.
     *
     * @param condition
     *      The error condition value to supply with the rejection.
     * @param description
     *      The error description value to supply with the rejection.
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage reject(String condition, String description) throws ClientException;

    /**
     * Modifies and settles the delivery.
     *
     * @param deliveryFailed
     *      Indicates if the modified delivery failed.
     * @param undeliverableHere
     *      Indicates if the modified delivery should not be returned here again.
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException;

    /**
     * Updates the DeliveryState, and optionally settle the delivery as well.
     *
     * @param state
     *            the delivery state to apply
     * @param settle
     *            whether to {@link #settle()} the delivery at the same time
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage disposition(DeliveryState state, boolean settle) throws ClientException;

    /**
     * Settles the delivery locally.
     *
     * @return this {@link StreamReceiverMessage} instance.
     *
     * @throws ClientException if an error occurs while sending the disposition
     */
    StreamReceiverMessage settle() throws ClientException;

    /**
     * @return true if the delivery has been locally settled.
     */
    boolean settled();

    /**
     * Gets the current local state for the delivery.
     *
     * @return the delivery state
     */
    DeliveryState state();

    /**
     * Gets the current remote state for the delivery.
     *
     * @return the remote delivery state
     */
    DeliveryState remoteState();

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean remoteSettled();

    /**
     * Gets the message format for the current delivery.
     *
     * @return the message format
     */
    int messageFormat();

}
