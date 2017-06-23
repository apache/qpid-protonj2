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

import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpSequenceTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpValueTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DataTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.FooterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.HeaderTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.MessageAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BinaryTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BooleanTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.CharacterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal128TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal32TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal64TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.DoubleTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.FloatTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.IntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ListTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.LongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.MapTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.NullTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ShortTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.StringTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.SymbolTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.TimestampTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UUIDTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedIntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedLongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedShortTypeEncoder;

/**
 * Factory that create and initializes new BuiltinEncoder instances
 */
public class ProtonEncoderFactory {

    private ProtonEncoderFactory() {
    }

    public static ProtonEncoder create() {
        ProtonEncoder encoder = new ProtonEncoder();

        addPrimitiveTypeEncoders(encoder);
        addMessagingTypeDecoders(encoder);

        return encoder;
    }

    private static void addMessagingTypeDecoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new AmqpSequenceTypeEncoder());
        encoder.registerTypeEncoder(new AmqpValueTypeEncoder());
        encoder.registerTypeEncoder(new ApplicationPropertiesTypeEncoder());
        encoder.registerTypeEncoder(new DataTypeEncoder());
        encoder.registerTypeEncoder(new DeliveryAnnotationsTypeEncoder());
        encoder.registerTypeEncoder(new FooterTypeEncoder());
        encoder.registerTypeEncoder(new HeaderTypeEncoder());
        encoder.registerTypeEncoder(new MessageAnnotationsTypeEncoder());
        encoder.registerTypeEncoder(new PropertiesTypeEncoder());
    }

    private static void addPrimitiveTypeEncoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new BinaryTypeEncoder());
        encoder.registerTypeEncoder(new BooleanTypeEncoder());
        encoder.registerTypeEncoder(new ByteTypeEncoder());
        encoder.registerTypeEncoder(new CharacterTypeEncoder());
        encoder.registerTypeEncoder(new Decimal32TypeEncoder());
        encoder.registerTypeEncoder(new Decimal64TypeEncoder());
        encoder.registerTypeEncoder(new Decimal128TypeEncoder());
        encoder.registerTypeEncoder(new DoubleTypeEncoder());
        encoder.registerTypeEncoder(new FloatTypeEncoder());
        encoder.registerTypeEncoder(new IntegerTypeEncoder());
        encoder.registerTypeEncoder(new ListTypeEncoder());
        encoder.registerTypeEncoder(new LongTypeEncoder());
        encoder.registerTypeEncoder(new MapTypeEncoder());
        encoder.registerTypeEncoder(new NullTypeEncoder());
        encoder.registerTypeEncoder(new ShortTypeEncoder());
        encoder.registerTypeEncoder(new StringTypeEncoder());
        encoder.registerTypeEncoder(new SymbolTypeEncoder());
        encoder.registerTypeEncoder(new TimestampTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedByteTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedShortTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedIntegerTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedLongTypeEncoder());
        encoder.registerTypeEncoder(new UUIDTypeEncoder());
    }
}
