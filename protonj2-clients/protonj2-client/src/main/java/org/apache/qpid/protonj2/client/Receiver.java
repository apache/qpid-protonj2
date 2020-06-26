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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

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

    // TODO - For these remote property reads do we support interruption on waiting for the open ?

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
     *
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

    // TODO - Receive calls throw ClientException to better describe why it failed

    /**
     * Blocking receive method that waits forever for the remote to provide a Message for consumption.
     *
     * @return a new {@link Delivery} received from the remote.
     *
     * @throws IllegalStateException if the {@link Receiver} is closed when the call to receive is made.
     */
    Delivery receive() throws IllegalStateException;

    /**
     * Non-blocking receive method that either returns a message is one is immediately available or
     * returns null if none is currently at hand.
     *
     * @return a new {@link Delivery} received from the remote or null if no pending message available.
     *
     * @throws IllegalStateException if the {@link Receiver} is closed when the call to try to receive is made.
     */
    Delivery tryReceive() throws IllegalStateException;

    Delivery receive(long timeout) throws IllegalStateException;
    // TODO: with credit window, above is fine...without, we would need to
    // manage the credit in one of various fashions (or say we dont).

    Future<Receiver> drain();

    // TODO: ideas

    // TODO: JMS 2 style 'receiveBody' that gets rid of delivery handling? Auto-acks
    //       (could extend later to client ack / transacted via session?)

    long getQueueSize();

    Receiver onMessage(Consumer<Delivery> handler);

    Receiver onMessage(Consumer<Delivery> handler, ExecutorService executor);

}
