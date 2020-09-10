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

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.exceptions.ClientException;

public interface Receiver {

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Receiver}.
     */
    Future<Receiver> openFuture();

    /**
     * Requests a close of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Receiver} link.
     */
    Future<Receiver> close();

    /**
     * Requests a close of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Receiver} link.
     */
    Future<Receiver> close(ErrorCondition error);

    /**
     * Requests a detach of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Receiver} link.
     */
    Future<Receiver> detach();

    /**
     * Requests a detach of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Receiver} link.
     */
    Future<Receiver> detach(ErrorCondition error);

    /**
     * Returns the address that the {@link Receiver} instance will be subscribed to.
     *
     * <ul>
     *  <li>
     *   If the Receiver was created with the dynamic receiver methods then the method will return
     *   the dynamically created address once the remote has attached its end of the receiver link.
     *   Due to the need to await the remote peer to populate the dynamic address this method will
     *   block until the open of the receiver link has completed.
     *  </li>
     *  <li>
     *   If not a dynamic receiver then the address returned is the address passed to the original
     *   {@link Session#openReceiver(String)} or {@link Session#openReceiver(String, ReceiverOptions)} methods.
     *  </li>
     * </ul>
     *
     * @return the address that this {@link Receiver} is sending to.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} address.
     */
    String address() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Source} object assigned to this receiver link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Source}.
     *
     * @return the remote {@link Source} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} remote {@link Source}.
     */
    Source source() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Target} object assigned to this receiver link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Source}.
     *
     * @return the remote {@link Target} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} remote {@link Target}.
     */
    Target target() throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Receiver}.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the receiver has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link Receiver}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the receiver has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link Receiver}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the receiver has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Receiver} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

    /**
     * @return the {@link Client} instance that holds this session's {@link Receiver}
     */
    Client client();

    /**
     * @return the {@link Session} that created and holds this {@link Receiver}.
     */
    Session session();

    /**
     * Adds credit to the {@link Receiver} link for use when there receiver has not been configured
     * with a credit window.  When credit window is configured credit replenishment is automatic and
     * calling this method will result in an exception indicating that the operation is invalid.
     * <p>
     * If the {@link Receiver} is draining and this method is called an exception will be thrown
     * to indicate that credit cannot be replenished until the remote has drained the existing link
     * credit.
     *
     * @param credits
     *      The number of credits to add to the {@link Receiver} link.
     *
     * @return this {@link Receiver} instance.
     *
     * @throws ClientException if an error occurs while attempting to add new {@link Receiver} link credit.
     */
    Receiver addCredit(int credits) throws ClientException;

    /**
     * Blocking receive method that waits forever for the remote to provide a {@link Delivery} for consumption.
     * <p>
     * Receive calls will only grant credit on their own if a credit window is configured in the
     * {@link ReceiverOptions} which is done by default.  If the client application has configured
     * no credit window than this method will not grant any credit when it enters the wait for new
     * incoming messages.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to receive is made.
     */
    Delivery receive() throws ClientException;

    /**
     * Blocking receive method that waits the given time interval for the remote to provide a
     * {@link Delivery} for consumption.  The amount of time this method blocks is based on the
     * timeout value. If timeout is equal to <code>-1</code> then it blocks until a Delivery is
     * received. If timeout is equal to zero then it will not block and simply return a
     * {@link Delivery} if one is available locally.  If timeout value is greater than zero then it
     * blocks up to timeout amount of time.
     * <p>
     * Receive calls will only grant credit on their own if a credit window is configured in the
     * {@link ReceiverOptions} which is done by default.  If the client application has not configured
     * a credit window or granted credit manually this method will not automatically grant any credit
     * when it enters the wait for a new incoming {@link Delivery}.
     *
     * @param timeout
     *      The timeout value used to control how long the receive method waits for a new {@link Delivery}.
     * @param unit
     *      The unit of time that the given timeout represents.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to receive is made.
     */
    Delivery receive(long timeout, TimeUnit unit) throws ClientException;

    /**
     * Non-blocking receive method that either returns a message is one is immediately available or
     * returns null if none is currently at hand.
     *
     * @return a new {@link Delivery} received from the remote or null if no pending deliveries are available.
     *
     * @throws ClientException if the {@link Receiver} or its parent is closed when the call to try to receive is made.
     */
    Delivery tryReceive() throws ClientException;

    /**
     * Creates and returns a new {@link ReceiveContext} that can be used to read incoming {@link Delivery}
     * data as it arrives regardless of the complete delivery date having been transmitted from the remote
     * peer.
     *
     * @param options
     *      The options to use when creating and configuring the new {@link ReceiveContext}.
     *
     * @return a new {@link ReceiveContext} that can be used to read incoming {@link Delivery} data.
     */
    ReceiveContext openReceiveContext(ReceiveContextOptions options);

    /**
     * Requests the remote to drain previously granted credit for this {@link Receiver} link.
     *
     * @return a {@link Future} that will be completed when the remote drains this {@link Receiver} link.
     *
     * @throws ClientException if an error occurs while attempting to drain the link credit.
     */
    Future<Receiver> drain() throws ClientException;

    /**
     * Returns the number of Deliveries that are currently held in the {@link Receiver} prefetched
     * queue.  This number is likely to change immediately following the call as more deliveries
     * arrive but can be used to determine if any pending {@link Delivery} work is ready.
     *
     * @return the number of deliveries that are currently buffered locally.
     */
    long prefetchedCount();

}
