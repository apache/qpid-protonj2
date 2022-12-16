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
package org.apache.qpid.protonj2.test.driver;

import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty5.buffer.Buffer;

/**
 * Link Tracker that manages tracking of the peer Receiver link which will
 * handle flows and receive transfers to a remote Sender link.
 */
public class ReceiverTracker extends LinkTracker {

    public ReceiverTracker(SessionTracker session) {
        super(session);
    }

    @Override
    protected void handleTransfer(Transfer transfer, Buffer payload) {
        // TODO: Update internal state
    }

    @Override
    protected void handleFlow(Flow flow) {

    }

    @Override
    public boolean isSender() {
        return false;
    }

    @Override
    public boolean isReceiver() {
        return true;
    }
}
