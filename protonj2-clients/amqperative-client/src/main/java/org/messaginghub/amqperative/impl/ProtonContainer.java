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
package org.messaginghub.amqperative.impl;

import org.messaginghub.amqperative.Connection;
import org.messaginghub.amqperative.ConnectionOptions;
import org.messaginghub.amqperative.Container;
import org.messaginghub.amqperative.ContainerOptions;

/**
 *
 */
public class ProtonContainer implements Container {

    private final ProtonContainerOptions options;

    /**
     * @param options
     *      The container options to use to configure this container instance.
     */
    public ProtonContainer(ContainerOptions options) {
        this.options = new ProtonContainerOptions(options);
    }

    @Override
    public Connection createConnection(String host, int port) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Connection createConnection(String host, int port, ConnectionOptions options) {
        // TODO Auto-generated method stub
        return null;
    }
}
