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
 * Options that control the behavior of the {@link Receiver} created from them.
 */
public class ReceiverOptions {

    private int creditWindow = -1;
    private String linkName;
    private boolean dynamic;

    public ReceiverOptions() {
    }

    public ReceiverOptions setLinkName(String linkName) {
        this.linkName = linkName;
        return this;
    }

    public String getLinkName() {
        return linkName;
    }

    public ReceiverOptions setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
        return this;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public int getCreditWindow() {
        return creditWindow;
    }

    public ReceiverOptions setCreditWindow(int creditWindow) {
        this.creditWindow = creditWindow;
        return this;
    }

    /**
     * Copy all options from this {@link ReceiverOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this options class for chaining.
     */
    public ReceiverOptions copyInto(ReceiverOptions other) {
        other.setCreditWindow(creditWindow);
        other.setDynamic(dynamic);
        other.setLinkName(linkName);

        return this;
    }
}
