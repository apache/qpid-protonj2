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

import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;

import io.netty.buffer.ByteBuf;

/**
 * Link Tracker that manages tracking of the peer Sender link which will
 * handle flows and initiate transfers to a remote Receiver link.
 */
public class SenderTracker extends LinkTracker {

    public SenderTracker(SessionTracker session, Attach attach) {
        super(session, attach);
    }

    @Override
    protected void handleTransfer(Transfer transfer, ByteBuf payload) {
        throw new AssertionError("Sender links cannot process incoming Transfers");
    }

    @Override
    protected void handleFlow(Flow flow) {
        // TODO Auto-generated method stub
    }
}
