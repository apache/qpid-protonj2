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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Receiver;

/**
 * Proton Receiver link implementation.
 */
public class ProtonReceiver extends ProtonLink implements Receiver, Performative.PerformativeHandler<ProtonEngine> {

    //----- Internal handler methods

    @Override
    void initiateLocalOpen() {
        // TODO Auto-generated method stub
    }

    @Override
    void initiateLocalClose() {
        // TODO Auto-generated method stub
    }

    //----- Handle incoming performatives

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, ProtonEngine context) {

    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, ProtonEngine context) {

    }
}
