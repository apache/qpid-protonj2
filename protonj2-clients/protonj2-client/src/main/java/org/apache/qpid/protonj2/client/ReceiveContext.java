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
import java.util.concurrent.TimeUnit;

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
     * @return the {@link Receiver} that this context was create under.
     */
    Receiver receiver();

    /**
     * Returns immediately if a remote delivery is ready for consumption or blocks waiting on the
     * initial transfer from an incoming {@link Delivery} to arrive and be assigned this
     * {@link ReceiveContext}.  Once a Delivery is assigned the context can being processing the
     * incoming data associated with the delivery.
     *
     * @return the associated Delivery object once a read has completed.
     */
    Delivery awaitDelivery();

    /**
     * Blocking method that waits the given time interval for the remote to provide a {@link Delivery}
     * for consumption.  The amount of time this method blocks is based on the timeout value. If timeout
     * is equal to <code>-1</code> then it blocks until a Delivery is received. If timeout is equal to
     * zero then it will not block and simply return a {@link Delivery} if one is available  locally.
     * If timeout value is greater than zero then it blocks up to timeout amount of time.
     * <p>
     * This call will only grant credit for deliveries on its own if a credit window is configured in the
     * {@link ReceiverOptions} which is done by default.  If the client application has not configured
     * a credit window or granted credit manually this method will not automatically grant any credit
     * when it enters the wait for a new incoming {@link Delivery}.
     *
     * @param timeout
     *      The timeout value used to control how long this method waits for a new {@link Delivery}.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to receive is made.
     *
     * #see {@link #awaitDelivery()}
     */
    Delivery awaitDelivery(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Decode the {@link Delivery} payload and return an {@link Message} object if there
     * is a {@link Delivery} associated with this {@link ReceiveContext} and it has reached
     * the completed state.
     *
     * @return a {@link Message} instance that wraps the decoded payload.
     *
     * @throws ClientException if an error occurs while decoding the payload or the delivery
     *                         is not yet complete (received all remote transfers).
     *
     * @param <E> The type of message body that should be contained in the returned {@link Message}.
     */
    <E> Message<E> message() throws ClientException;

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
    boolean complete();

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
     * @param options
     *      The stream options to use to configure the returned {@link InputStream}
     *
     * @return a {@link InputStream} instance configured using the given options.
     *
     * @throws ClientException if an error occurs while creating the input stream.
     */
    InputStream rawInputStream(InputStreamOptions options) throws ClientException;

}
