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

/**
 * Container Options for customizing the behavior of the Container
 */
public class ContainerOptions {

    private String containerId;

    /**
     * @return the ID configured the Container
     */
    public String getContainerName() {
        return containerId;
    }

    /**
     * Sets the container ID that should be used when creating Connections
     *
     * @param containerId
     *      The container Id that should be assigned to container connections.
     *
     * @return this options class for chaining.
     */
    public ContainerOptions setContainerName(String containerId) {
        this.containerId = containerId;
        return this;
    }
}
