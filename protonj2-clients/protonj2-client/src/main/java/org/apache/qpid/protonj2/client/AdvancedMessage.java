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
package org.apache.qpid.protonj2.client;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;

/**
 * Advanced AMQP Message object that provides a thin abstraction to raw AMQP types
 *
 * @param <E> The type of the message body that this message carries
 */
public interface AdvancedMessage<E> extends Message<E> {

    Header header();

    AdvancedMessage<E> header(Header header);

    DeliveryAnnotations deliveryAnnotations();

    AdvancedMessage<E> deliveryAnnotations(DeliveryAnnotations deliveryAnnotations);

    MessageAnnotations messageAnnotations();

    AdvancedMessage<E> messageAnnotations(MessageAnnotations messageAnnotations);

    Properties properties();

    AdvancedMessage<E> properties(Properties properties);

    ApplicationProperties applicationProperties();

    AdvancedMessage<E> applicationProperties(ApplicationProperties applicationProperties);

    Footer footer();

    AdvancedMessage<E> footer(Footer footer);

    int messageFormat();

    AdvancedMessage<E> messageFormat(int messageFormat);

    ProtonBuffer encode();

}
