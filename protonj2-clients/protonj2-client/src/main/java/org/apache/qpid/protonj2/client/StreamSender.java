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
 * Sending link implementation that allows sending of large message payload data in
 * multiple transfers to reduce memory overhead of large message sends.
 */
public interface StreamSender {

    /**
     * @return the {@link Client} instance that holds this session's {@link StreamSender}
     */
    Client client();

    /**
     * @return the {@link Connection} instance that holds this session's {@link StreamSender}
     */
    Connection connection();

    /**
     * @return the {@link Session} that created and holds this {@link StreamSender}.
     */
    Session session();

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link StreamSender}.
     */
    Future<StreamSender> openFuture();

    /**
     * Requests a close of the {@link StreamSender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link StreamSender} link.
     */
    Future<StreamSender> close();

    /**
     * Requests a close of the {@link StreamSender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link StreamSender} link.
     */
    Future<StreamSender> close(ErrorCondition error);

    /**
     * Requests a detach of the {@link StreamSender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link StreamSender} link.
     */
    Future<StreamSender> detach();

    /**
     * Requests a detach of the {@link StreamSender} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link StreamSender} link.
     */
    Future<StreamSender> detach(ErrorCondition error);

    /**
     * Returns the address that the {@link StreamSender} instance will send streamed large messages to.
     * The value returned from this method is controlled by the configuration that was used to create the
     * {@link StreamSender}.
     *
     * @return the address that this {@link StreamSender} is sending to.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} address.
     */
    String address() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Source} object assigned to this sender link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Source}.
     *
     * @return the remote {@link Source} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} remote {@link Source}.
     */
    Source source() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Target} object assigned to this sender link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Target}.
     *
     * @return the remote {@link Target} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} remote {@link Target}.
     */
    Target target() throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link StreamSender}.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link StreamSender}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link StreamSender}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link StreamSender} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

    /**
     * Creates and returns a new {@link StreamTracker} that can be used by the caller to perform
     * multiple sends of custom encoded messages or perform chucked large message transfers to the
     * remote as part of a streaming send operation.
     *
     * @return a new {@link StreamTracker} that can be used to stream message data to the remote.
     *
     * @throws ClientException if an error occurs while initiating a new streaming send tracker.
     */
    StreamTracker openStream() throws ClientException;

}
