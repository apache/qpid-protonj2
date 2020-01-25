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
package org.apache.qpid.proton4j.amqp.driver.actions;

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.End;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;

/**
 * AMQP End injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class EndInjectAction extends AbstractPerformativeInjectAction<End> {

    private final End end = new End();

    public EndInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public End getPerformative() {
        return end;
    }

    public EndInjectAction withErrorCondition(ErrorCondition error) {
        end.setError(TypeMapper.mapFromProtonType(error));
        return this;
    }

    public EndInjectAction withErrorCondition(String condition, String description) {
        end.setError(TypeMapper.mapFromProtonType(new ErrorCondition(Symbol.valueOf(condition), description)));
        return this;
    }

    public EndInjectAction withErrorCondition(Symbol condition, String description) {
        end.setError(TypeMapper.mapFromProtonType(new ErrorCondition(condition, description)));
        return this;
    }

    public EndInjectAction withErrorCondition(String condition, String description, Map<String, Object> info) {
        end.setError(TypeMapper.mapFromProtonType(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
        return this;
    }

    public EndInjectAction withErrorCondition(Symbol condition, String description, Map<Symbol, Object> info) {
        end.setError(TypeMapper.mapFromProtonType(new ErrorCondition(condition, description, info)));
        return this;
    }

    @Override
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // TODO - Process end in the session tracker ?
    }
}
