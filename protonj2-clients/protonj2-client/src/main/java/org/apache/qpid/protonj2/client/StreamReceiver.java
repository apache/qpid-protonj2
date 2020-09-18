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

import org.apache.qpid.protonj2.client.exceptions.ClientException;

/**
 *
 */
public interface StreamReceiver {

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Receiver}.
     */
    Future<StreamReceiver> openFuture();

    /**
     * Requests a close of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Receiver} link.
     */
    Future<StreamReceiver> close();

    /**
     * Requests a close of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Receiver} link.
     */
    Future<StreamReceiver> close(ErrorCondition error);

    /**
     * Requests a detach of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Receiver} link.
     */
    Future<StreamReceiver> detach();

    /**
     * Requests a detach of the {@link Receiver} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Receiver} link.
     */
    Future<StreamReceiver> detach(ErrorCondition error);

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
     * @return the {@link Client} instance that holds this session's {@link StreamReceiver}
     */
    Client client();

    /**
     * @return the {@link Connection} instance that holds this session's {@link StreamReceiver}
     */
    Connection connection();

    /**
     * @return the {@link Session} that created and holds this {@link StreamReceiver}.
     */
    Session session();

    /**
     * Adds credit to the {@link StreamReceiver} link for use when there receiver has not been configured
     * with a credit window.  When credit window is configured credit replenishment is automatic and
     * calling this method will result in an exception indicating that the operation is invalid.
     * <p>
     * If the {@link StreamReceiver} is draining and this method is called an exception will be thrown
     * to indicate that credit cannot be replenished until the remote has drained the existing link
     * credit.
     *
     * @param credits
     *      The number of credits to add to the {@link StreamReceiver} link.
     *
     * @return this {@link StreamReceiver} instance.
     *
     * @throws ClientException if an error occurs while attempting to add new {@link StreamReceiver} link credit.
     */
    StreamReceiver addCredit(int credits) throws ClientException;

    /**
     * Creates and returns a new {@link StreamDelivery} that can be used to read incoming {@link Delivery}
     * data as it arrives regardless of the complete delivery date having been transmitted from the remote
     * peer.
     *
     * @return a new {@link StreamDelivery} that can be used to read incoming streamed message data.
     *
     * @throws ClientException if an error occurs while attempting to open a new {@link StreamDelivery}.
     */
    StreamDelivery openStream() throws ClientException;

    /**
     * Requests the remote to drain previously granted credit for this {@link StreamReceiver} link.
     *
     * @return a {@link Future} that will be completed when the remote drains this {@link StreamReceiver} link.
     *
     * @throws ClientException if an error occurs while attempting to drain the link credit.
     */
    Future<StreamReceiver> drain() throws ClientException;

}
