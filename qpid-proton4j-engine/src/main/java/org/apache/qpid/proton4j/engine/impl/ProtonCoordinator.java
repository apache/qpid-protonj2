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

import org.apache.qpid.proton4j.amqp.transactions.Declare;
import org.apache.qpid.proton4j.amqp.transactions.Declared;
import org.apache.qpid.proton4j.amqp.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.engine.EventHandler;

/**
 * Proton Coordinator link implementation.
 */
public class ProtonCoordinator extends ProtonLink<ProtonCoordinator> {

    // TODO - Should be two ends of a coordinator, sender and receiver
    //        one side drives the declare and discharge and the other
    //        handles those requests for TX boundaries.
    //
    // CoordinatorClient -> CoordinatorServer
    // CoordinatorSource -> CoordinatorSink
    // CoordinatorSender -> CoordinatorReceiver

    protected ProtonCoordinator(ProtonSession session, String name) {
        super(session, name);
    }

    @Override
    public int getCredit() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Role getRole() {
        return Role.SENDER;
    }

    @Override
    protected ProtonCoordinator self() {
        return this;
    }

    @Override
    protected ProtonLinkState<?> getState() {
        // TODO Auto-generated method stub
        return null;
    }

    public void declare() {

    }

    public void discharge(byte[] txnId, boolean failed) {

    }

    // TODO - Possible event points to handle declare of a TX and the response, and the discharge of a TX

    public ProtonCoordinator declareHandler(EventHandler<Declare> remoteDeclareHandler) {
        return this;
    }

    public ProtonCoordinator declaredHandler(EventHandler<Declared> remoteDeclaredHandler) {
        return this;
    }

    public ProtonCoordinator dischargeHandler(EventHandler<Discharge> remoteDischargeHandler) {
        return this;
    }
}
