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
package org.apache.qpid.proton4j.amqp.driver.codec;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.AmqpSequence;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Data;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.DeleteOnClose;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.DeleteOnNoLinks;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.DeleteOnNoLinksOrMessages;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.DeleteOnNoMessages;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Footer;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Header;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Modified;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Properties;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Received;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Released;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslInit;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Declare;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Declared;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Discharge;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.TransactionalState;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Attach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Begin;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Close;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Disposition;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.End;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Flow;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Open;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.types.DescribedType;

/**
 * Registry of described types know to the Data type codec
 */
public abstract class DescribedTypeRegistry {

    private static Map<Object, Class<? extends DescribedType>> describedTypes = new HashMap<>();

    static {
        describedTypes.put(Accepted.DESCRIPTOR_CODE, Accepted.class);
        describedTypes.put(Accepted.DESCRIPTOR_SYMBOL, Accepted.class);
        describedTypes.put(Attach.DESCRIPTOR_CODE, Attach.class);
        describedTypes.put(Attach.DESCRIPTOR_SYMBOL, Attach.class);
        describedTypes.put(Begin.DESCRIPTOR_CODE, Begin.class);
        describedTypes.put(Begin.DESCRIPTOR_SYMBOL, Begin.class);
        describedTypes.put(Close.DESCRIPTOR_CODE, Close.class);
        describedTypes.put(Close.DESCRIPTOR_SYMBOL, Close.class);
        describedTypes.put(Coordinator.DESCRIPTOR_CODE, Coordinator.class);
        describedTypes.put(Coordinator.DESCRIPTOR_SYMBOL, Coordinator.class);
        describedTypes.put(Declare.DESCRIPTOR_CODE, Declare.class);
        describedTypes.put(Declare.DESCRIPTOR_SYMBOL, Declare.class);
        describedTypes.put(Declared.DESCRIPTOR_CODE, Declared.class);
        describedTypes.put(Declared.DESCRIPTOR_SYMBOL, Declared.class);
        describedTypes.put(DeleteOnClose.DESCRIPTOR_CODE, DeleteOnClose.class);
        describedTypes.put(DeleteOnClose.DESCRIPTOR_SYMBOL, DeleteOnClose.class);
        describedTypes.put(DeleteOnNoLinks.DESCRIPTOR_CODE, DeleteOnNoLinks.class);
        describedTypes.put(DeleteOnNoLinks.DESCRIPTOR_SYMBOL, DeleteOnNoLinks.class);
        describedTypes.put(DeleteOnNoLinksOrMessages.DESCRIPTOR_CODE, DeleteOnNoLinksOrMessages.class);
        describedTypes.put(DeleteOnNoLinksOrMessages.DESCRIPTOR_SYMBOL, DeleteOnNoLinksOrMessages.class);
        describedTypes.put(DeleteOnNoMessages.DESCRIPTOR_CODE, DeleteOnNoMessages.class);
        describedTypes.put(DeleteOnNoMessages.DESCRIPTOR_SYMBOL, DeleteOnNoMessages.class);
        describedTypes.put(Detach.DESCRIPTOR_CODE, Detach.class);
        describedTypes.put(Detach.DESCRIPTOR_SYMBOL, Detach.class);
        describedTypes.put(Discharge.DESCRIPTOR_CODE, Discharge.class);
        describedTypes.put(Discharge.DESCRIPTOR_SYMBOL, Discharge.class);
        describedTypes.put(Disposition.DESCRIPTOR_CODE, Disposition.class);
        describedTypes.put(Disposition.DESCRIPTOR_SYMBOL, Disposition.class);
        describedTypes.put(End.DESCRIPTOR_CODE, End.class);
        describedTypes.put(End.DESCRIPTOR_SYMBOL, End.class);
        describedTypes.put(ErrorCondition.DESCRIPTOR_CODE, ErrorCondition.class);
        describedTypes.put(ErrorCondition.DESCRIPTOR_SYMBOL, ErrorCondition.class);
        describedTypes.put(Flow.DESCRIPTOR_CODE, Flow.class);
        describedTypes.put(Flow.DESCRIPTOR_SYMBOL, Flow.class);
        describedTypes.put(Modified.DESCRIPTOR_CODE, Modified.class);
        describedTypes.put(Modified.DESCRIPTOR_SYMBOL, Modified.class);
        describedTypes.put(Open.DESCRIPTOR_CODE, Open.class);
        describedTypes.put(Open.DESCRIPTOR_SYMBOL, Open.class);
        describedTypes.put(Received.DESCRIPTOR_CODE, Received.class);
        describedTypes.put(Received.DESCRIPTOR_SYMBOL, Received.class);
        describedTypes.put(Rejected.DESCRIPTOR_CODE, Rejected.class);
        describedTypes.put(Rejected.DESCRIPTOR_SYMBOL, Rejected.class);
        describedTypes.put(Released.DESCRIPTOR_CODE, Released.class);
        describedTypes.put(Released.DESCRIPTOR_SYMBOL, Released.class);
        describedTypes.put(SaslChallenge.DESCRIPTOR_CODE, SaslChallenge.class);
        describedTypes.put(SaslChallenge.DESCRIPTOR_SYMBOL, SaslChallenge.class);
        describedTypes.put(SaslInit.DESCRIPTOR_CODE, SaslInit.class);
        describedTypes.put(SaslInit.DESCRIPTOR_SYMBOL, SaslInit.class);
        describedTypes.put(SaslMechanisms.DESCRIPTOR_CODE, SaslMechanisms.class);
        describedTypes.put(SaslMechanisms.DESCRIPTOR_SYMBOL, SaslMechanisms.class);
        describedTypes.put(SaslOutcome.DESCRIPTOR_CODE, SaslOutcome.class);
        describedTypes.put(SaslOutcome.DESCRIPTOR_SYMBOL, SaslOutcome.class);
        describedTypes.put(SaslResponse.DESCRIPTOR_CODE, SaslResponse.class);
        describedTypes.put(SaslResponse.DESCRIPTOR_SYMBOL, SaslResponse.class);
        describedTypes.put(Source.DESCRIPTOR_CODE, Source.class);
        describedTypes.put(Source.DESCRIPTOR_SYMBOL, Source.class);
        describedTypes.put(Target.DESCRIPTOR_CODE, Target.class);
        describedTypes.put(Target.DESCRIPTOR_SYMBOL, Target.class);
        describedTypes.put(TransactionalState.DESCRIPTOR_CODE, TransactionalState.class);
        describedTypes.put(TransactionalState.DESCRIPTOR_SYMBOL, TransactionalState.class);
        describedTypes.put(Transfer.DESCRIPTOR_CODE, Transfer.class);
        describedTypes.put(Transfer.DESCRIPTOR_SYMBOL, Transfer.class);
        describedTypes.put(AmqpSequence.DESCRIPTOR_CODE, AmqpSequence.class);
        describedTypes.put(AmqpSequence.DESCRIPTOR_SYMBOL, AmqpSequence.class);
        describedTypes.put(AmqpValue.DESCRIPTOR_CODE, AmqpValue.class);
        describedTypes.put(AmqpValue.DESCRIPTOR_SYMBOL, AmqpValue.class);
        describedTypes.put(ApplicationProperties.DESCRIPTOR_CODE, ApplicationProperties.class);
        describedTypes.put(ApplicationProperties.DESCRIPTOR_SYMBOL, ApplicationProperties.class);
        describedTypes.put(Data.DESCRIPTOR_CODE, Data.class);
        describedTypes.put(Data.DESCRIPTOR_SYMBOL, Data.class);
        describedTypes.put(DeliveryAnnotations.DESCRIPTOR_CODE, DeliveryAnnotations.class);
        describedTypes.put(DeliveryAnnotations.DESCRIPTOR_SYMBOL, DeliveryAnnotations.class);
        describedTypes.put(Footer.DESCRIPTOR_CODE, Footer.class);
        describedTypes.put(Footer.DESCRIPTOR_SYMBOL, Footer.class);
        describedTypes.put(Header.DESCRIPTOR_CODE, Header.class);
        describedTypes.put(Header.DESCRIPTOR_SYMBOL, Header.class);
        describedTypes.put(MessageAnnotations.DESCRIPTOR_CODE, MessageAnnotations.class);
        describedTypes.put(MessageAnnotations.DESCRIPTOR_SYMBOL, MessageAnnotations.class);
        describedTypes.put(Properties.DESCRIPTOR_CODE, Properties.class);
        describedTypes.put(Properties.DESCRIPTOR_SYMBOL, Properties.class);
    }

    private DescribedTypeRegistry() {
    }

    static DescribedType lookupDescribedType(Object descriptor, Object described) {
        Class<? extends DescribedType> typeClass = describedTypes.get(descriptor);
        if (typeClass != null) {
            try {
                Constructor<? extends DescribedType> constructor = typeClass.getConstructor(Object.class);
                return constructor.newInstance(described);
            } catch (Throwable err){
            }
        }

        return new DescribedTypeImpl(descriptor, described);
    }
}
