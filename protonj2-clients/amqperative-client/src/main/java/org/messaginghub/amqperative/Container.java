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
package org.messaginghub.amqperative;

import java.util.Objects;

import org.messaginghub.amqperative.impl.ProtonContainer;
import org.messaginghub.amqperative.impl.ProtonContainerOptions;

/**
 * The Container that hosts the AMQP Connection
 */
public interface Container {

    static Container create() {
        return create(new ProtonContainerOptions());
    }

    static Container create(ContainerOptions options) {
        Objects.requireNonNull(options, "options must be non-null");
        return new ProtonContainer(options);
    }

    /**
     * Connect to the specified host and port, without credentials.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @return connection, establishment not yet completed
     */
    Connection createConnection(String host, int port);

    /**
     * Connect to the specified host and port, with given options.
     *
     * @param host
     *            the host to connect to
     * @param port
     *            the port to connect to
     * @param options
     *            options.
     * @return connection, establishment not yet completed
     */
    Connection createConnection(String host, int port, ConnectionOptions options);

}
