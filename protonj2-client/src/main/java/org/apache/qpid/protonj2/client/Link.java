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
 * Base for all AMQP link types (Sender, Receiver etc).
 *
 * @param <T> The actual link type that is being created (Sender or Receiver).
 */
public interface Link<T extends Link<T>> extends AutoCloseable {

    /**
     * @return a {@link Future} that will be completed when the remote opens this {@link Link}.
     */
    Future<T> openFuture();

    /**
     * @return the {@link Client} instance that holds this session's {@link Link}
     */
    Client client();

    /**
     * @return the {@link Connection} instance that holds this session's {@link Link}
     */
    Connection connection();

    /**
     * @return the {@link Session} that created and holds this {@link Link}.
     */
    Session session();

    /**
     * Requests a close of the {@link Link} at the remote and waits until the Link has been
     * fully closed or until the configured close timeout is exceeded.
     */
    @Override
    void close();

    /**
     * Requests a close of the {@link Link} at the remote and waits until the Link has been
     * fully closed or until the configured {@link SenderOptions#closeTimeout()} is exceeded.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the close operation.
     */
    void close(ErrorCondition error);

    /**
     * Requests a detach of the {@link Link} at the remote and waits until the Link has been
     * fully detached or until the configured {@link SenderOptions#closeTimeout()} is exceeded.
     */
    void detach();

    /**
     * Requests a detach of the {@link Link} at the remote and waits until the Link has been
     * fully detached or until the configured {@link SenderOptions#closeTimeout()} is exceeded.
     *
     * @param error
     *      The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     */
    void detach(ErrorCondition error);

    /**
     * Requests a close of the {@link Link} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Link} link.
     */
    Future<T> closeAsync();

    /**
     * Requests a close of the {@link Link} link at the remote and returns a {@link Future} that will be
     * completed once the link has been closed.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the close operation.
     *
     * @return a {@link Future} that will be completed when the remote closes this {@link Link} link.
     */
    Future<T> closeAsync(ErrorCondition error);

    /**
     * Requests a detach of the {@link Link} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Link} link.
     */
    Future<T> detachAsync();

    /**
     * Requests a detach of the {@link Link} link at the remote and returns a {@link Future} that will be
     * completed once the link has been detached.
     *
     * @param error
     * 		The {@link ErrorCondition} to transmit to the remote along with the detach operation.
     *
     * @return a {@link Future} that will be completed when the remote detaches this {@link Link} link.
     */
    Future<T> detachAsync(ErrorCondition error);

    /**
     * Returns the address that the {@link Link} instance will be subscribed to. This method can
     * block based on the type of link and how it was configured.
     *
     * <ul>
     *  <li>
     *    If the link is a Sender and it was configured as an anonymous sender then this method
     *    returns null as the link has no address.
     *  </li>
     *  <li>
     *   If a link was created with the dynamic node value enabled then the method will return
     *   the dynamically created address once the remote has attached its end of the opened link.
     *   Due to the need to await the remote peer to populate the dynamic address this method will
     *   block until the open of the link has completed.
     *  </li>
     *  <li>
     *   If not a dynamic link then the address returned is the address passed to the original
     *   link creation method.
     *  </li>
     * </ul>
     *
     * @return the address that this {@link Link} is was assigned to.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} address.
     */
    String address() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Source} object assigned to this link.  If the attach
     * has not completed yet this method will block to await the attach response which carries the remote
     * {@link Source}.
     *
     * @return the remote {@link Source} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} remote {@link Source}.
     */
    Source source() throws ClientException;

    /**
     * Returns an immutable view of the remote {@link Target} object assigned to this sender link.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * {@link Target}.
     *
     * @return the remote {@link Target} node configuration.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} remote {@link Target}.
     */
    Target target() throws ClientException;

    /**
     * Returns the properties that the remote provided upon successfully opening the {@link Link}.  If the
     * attach has not completed yet this method will block to await the attach response which carries the remote
     * properties.  If the remote provides no properties this method will return null.
     *
     * @return any properties provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} remote properties.
     */
    Map<String, Object> properties() throws ClientException;

    /**
     * Returns the offered capabilities that the remote provided upon successfully opening the {@link Link}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote offered capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} remote offered capabilities.
     */
    String[] offeredCapabilities() throws ClientException;

    /**
     * Returns the desired capabilities that the remote provided upon successfully opening the {@link Link}.
     * If the attach has not completed yet this method will block to await the attach response which carries the
     * remote desired capabilities.  If the remote provides no capabilities this method will return null.
     *
     * @return any desired capabilities provided from the remote once the sender has successfully opened.
     *
     * @throws ClientException if an error occurs while obtaining the {@link Link} remote desired capabilities.
     */
    String[] desiredCapabilities() throws ClientException;

}
