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
package org.apache.qpid.proton4j.codec.decoders;

import org.apache.qpid.proton4j.codec.decoders.messaging.AcceptedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.AmqpSequenceTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.AmqpValueTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnCloseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoLinksOrMessagesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoLinksTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoMessagesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.MessageAnnotationsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ModifiedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ReceivedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.RejectedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ReleasedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.SourceTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.TargetTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslChallengeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslInitTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslMechanismsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslOutcomeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslResponseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.CoordinatorTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclareTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclaredTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DischargeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.TransactionStateTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.AttachTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.BeginTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.CloseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DetachTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DispositionTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.EndTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.ErrorConditionTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.FlowTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.OpenTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.TransferTypeDecoder;

/**
 * Factory that create and initializes new BuiltinDecoder instances
 */
public final class ProtonDecoderFactory {

    private ProtonDecoderFactory() {
    }

    public static ProtonDecoder create() {
        ProtonDecoder decoder = new ProtonDecoder();

        addMessagingTypeDecoders(decoder);
        addTransactionTypeDecoders(decoder);
        addTransportTypeDecoders(decoder);

        return decoder;
    }

    public static ProtonDecoder createSasl() {
        ProtonDecoder decoder = new ProtonDecoder();

        addSaslTypeDecoders(decoder);

        return decoder;
    }

    private static void addMessagingTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new AcceptedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new AmqpSequenceTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new AmqpValueTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ApplicationPropertiesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DataTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnCloseTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoLinksOrMessagesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoLinksTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoMessagesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeliveryAnnotationsTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new FooterTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new HeaderTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new MessageAnnotationsTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ModifiedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new PropertiesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ReceivedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new RejectedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ReleasedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new SourceTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TargetTypeDecoder());
    }

    private static void addTransactionTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new CoordinatorTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeclaredTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeclareTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DischargeTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TransactionStateTypeDecoder());
    }

    private static void addTransportTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new AttachTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new BeginTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new CloseTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DetachTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DispositionTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new EndTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ErrorConditionTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new FlowTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new OpenTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TransferTypeDecoder());
    }

    private static void addSaslTypeDecoders(ProtonDecoder decoder) {
        decoder.registerDescribedTypeDecoder(new SaslChallengeTypeDecoder());
        decoder.registerDescribedTypeDecoder(new SaslInitTypeDecoder());
        decoder.registerDescribedTypeDecoder(new SaslMechanismsTypeDecoder());
        decoder.registerDescribedTypeDecoder(new SaslOutcomeTypeDecoder());
        decoder.registerDescribedTypeDecoder(new SaslResponseTypeDecoder());
    }
}
