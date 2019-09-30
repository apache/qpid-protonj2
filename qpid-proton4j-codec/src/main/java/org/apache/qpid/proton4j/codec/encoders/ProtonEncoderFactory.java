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
package org.apache.qpid.proton4j.codec.encoders;

import org.apache.qpid.proton4j.codec.encoders.messaging.AcceptedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpSequenceTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpValueTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DataTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnCloseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoLinksOrMessagesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoLinksTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoMessagesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.FooterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.HeaderTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.MessageAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ModifiedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ReceivedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.RejectedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ReleasedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.SourceTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.TargetTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.security.SaslChallengeTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.security.SaslInitTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.security.SaslMechanismsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.security.SaslOutcomeTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.security.SaslResponseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.CoordinatorTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclareTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclaredTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DischargeTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.TransactionStateTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.AttachTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.BeginTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.CloseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DetachTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DispositionTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.EndTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.ErrorConditionTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.FlowTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.OpenTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.TransferTypeEncoder;

/**
 * Factory that create and initializes new BuiltinEncoder instances
 */
public class ProtonEncoderFactory {

    private ProtonEncoderFactory() {
    }

    public static ProtonEncoder create() {
        ProtonEncoder encoder = new ProtonEncoder();

        addMessagingTypeEncoders(encoder);
        addTransactionTypeEncoders(encoder);
        addTransportTypeEncoders(encoder);

        return encoder;
    }

    public static ProtonEncoder createSasl() {
        ProtonEncoder encoder = new ProtonEncoder();

        addSaslTypeEncoders(encoder);

        return encoder;
    }

    private static void addMessagingTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(new AcceptedTypeEncoder());
        encoder.registerDescribedTypeEncoder(new AmqpSequenceTypeEncoder());
        encoder.registerDescribedTypeEncoder(new AmqpValueTypeEncoder());
        encoder.registerDescribedTypeEncoder(new ApplicationPropertiesTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DataTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeleteOnCloseTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeleteOnNoLinksOrMessagesTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeleteOnNoLinksTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeleteOnNoMessagesTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeliveryAnnotationsTypeEncoder());
        encoder.registerDescribedTypeEncoder(new FooterTypeEncoder());
        encoder.registerDescribedTypeEncoder(new HeaderTypeEncoder());
        encoder.registerDescribedTypeEncoder(new MessageAnnotationsTypeEncoder());
        encoder.registerDescribedTypeEncoder(new ModifiedTypeEncoder());
        encoder.registerDescribedTypeEncoder(new PropertiesTypeEncoder());
        encoder.registerDescribedTypeEncoder(new ReceivedTypeEncoder());
        encoder.registerDescribedTypeEncoder(new RejectedTypeEncoder());
        encoder.registerDescribedTypeEncoder(new ReleasedTypeEncoder());
        encoder.registerDescribedTypeEncoder(new SourceTypeEncoder());
        encoder.registerDescribedTypeEncoder(new TargetTypeEncoder());
    }

    private static void addTransactionTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(new CoordinatorTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeclaredTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DeclareTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DischargeTypeEncoder());
        encoder.registerDescribedTypeEncoder(new TransactionStateTypeEncoder());
    }

    private static void addTransportTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(new AttachTypeEncoder());
        encoder.registerDescribedTypeEncoder(new BeginTypeEncoder());
        encoder.registerDescribedTypeEncoder(new CloseTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DetachTypeEncoder());
        encoder.registerDescribedTypeEncoder(new DispositionTypeEncoder());
        encoder.registerDescribedTypeEncoder(new EndTypeEncoder());
        encoder.registerDescribedTypeEncoder(new ErrorConditionTypeEncoder());
        encoder.registerDescribedTypeEncoder(new FlowTypeEncoder());
        encoder.registerDescribedTypeEncoder(new OpenTypeEncoder());
        encoder.registerDescribedTypeEncoder(new TransferTypeEncoder());
    }

    private static void addSaslTypeEncoders(ProtonEncoder encoder) {
        encoder.registerDescribedTypeEncoder(new SaslChallengeTypeEncoder());
        encoder.registerDescribedTypeEncoder(new SaslInitTypeEncoder());
        encoder.registerDescribedTypeEncoder(new SaslMechanismsTypeEncoder());
        encoder.registerDescribedTypeEncoder(new SaslOutcomeTypeEncoder());
        encoder.registerDescribedTypeEncoder(new SaslResponseTypeEncoder());
    }
}
