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
package org.apache.qpid.proton4j.amqp.driver.codec.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Modified;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Released;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source;
import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Target;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Coordinator;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.Declared;
import org.apache.qpid.proton4j.amqp.driver.codec.transactions.TransactionalState;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.amqp.messaging.Outcome;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

public abstract class TypeMapper {

    private TypeMapper() {
    }

    public static Symbol[] toSymbolArray(String[] stringArray) {
        Symbol[] result = null;

        if (stringArray != null) {
            result = new Symbol[stringArray.length];
            for (int i = 0; i < stringArray.length; ++i) {
                result[i] = Symbol.valueOf(stringArray[i]);
            }
        }

        return result;
    }

    public static Map<Symbol, Object> toSymbolKeyedMap(Map<String, Object> stringsMap) {
        final Map<Symbol, Object> result;

        if (stringsMap != null) {
            result = new HashMap<>(stringsMap.size());
            stringsMap.forEach((key, value) -> {
                result.put(Symbol.valueOf(key), value);
            });
        } else {
            result = null;
        }

        return result;
    }

    public static DescribedType mapFromProtonType(Outcome outcome) {
        if (outcome != null) {
            return mapFromProtonType((DeliveryState) outcome);
        } else {
            return null;
        }
    }

    public static DescribedType mapFromProtonType(DeliveryState state) {
        if (state != null) {
            switch (state.getType()) {
                case Accepted:
                    return new Accepted();
                case Declared:
                    Declared declared = new Declared();
                    declared.setTxnId(((org.apache.qpid.proton4j.amqp.transactions.Declared) state).getTxnId());
                    return declared;
                case Modified:
                    Modified modified = new Modified();
                    modified.setDeliveryFailed(
                        ((org.apache.qpid.proton4j.amqp.messaging.Modified) state).isDeliveryFailed());
                    modified.setUndeliverableHere(
                        ((org.apache.qpid.proton4j.amqp.messaging.Modified) state).isUndeliverableHere());
                    modified.setMessageAnnotations(
                        ((org.apache.qpid.proton4j.amqp.messaging.Modified) state).getMessageAnnotations());
                    return modified;
                case Rejected:
                    Rejected rejected = new Rejected();
                    rejected.setError(mapFromProtonType(((org.apache.qpid.proton4j.amqp.messaging.Rejected) state).getError()));
                    return rejected;
                case Released:
                    return new Released();
                case Transactional:
                    TransactionalState tx = new TransactionalState();
                    tx.setOutcome(mapFromProtonType((DeliveryState)
                        ((org.apache.qpid.proton4j.amqp.transactions.TransactionalState) state).getOutcome()));
                    tx.setTxnId(((org.apache.qpid.proton4j.amqp.transactions.TransactionalState) state).getTxnId());
                    return tx;
                default:
                    break;
            }
            return null;
        } else {
            return null;
        }
    }

    public static DeliveryState mapToProtonType(DescribedType state) {
        if (state != null) {
            DeliveryState result = null;
            if (state instanceof Accepted) {
                result = org.apache.qpid.proton4j.amqp.messaging.Accepted.getInstance();
            } else if (state instanceof Declared) {
                org.apache.qpid.proton4j.amqp.transactions.Declared declared =
                    new org.apache.qpid.proton4j.amqp.transactions.Declared();
                declared.setTxnId(((TransactionalState) state).getTxnId());
                result = declared;
            } else if (state instanceof Rejected) {
                org.apache.qpid.proton4j.amqp.messaging.Rejected rejected =
                    new org.apache.qpid.proton4j.amqp.messaging.Rejected();
                rejected.setError(mapToProtonType(((Rejected) state).getError()));
                result = rejected;
            } else if (state instanceof Released) {
                result = org.apache.qpid.proton4j.amqp.messaging.Released.getInstance();
            } else if (state instanceof Modified) {
                org.apache.qpid.proton4j.amqp.messaging.Modified modified =
                    new org.apache.qpid.proton4j.amqp.messaging.Modified();
                modified.setDeliveryFailed(((Modified) state).getDeliveryFailed());
                modified.setUndeliverableHere(((Modified) state).getUndeliverableHere());
                modified.setMessageAnnotations(((Modified) state).getMessageAnnotations());
                result = modified;
            } else if (state instanceof TransactionalState) {
                org.apache.qpid.proton4j.amqp.transactions.TransactionalState tx =
                    new org.apache.qpid.proton4j.amqp.transactions.TransactionalState();
                tx.setTxnId(((TransactionalState) state).getTxnId());
                tx.setOutcome((Outcome) mapToProtonType(((TransactionalState) state).getOutcome()));
                result = tx;
            }
            return result;
        } else {
            return null;
        }
    }

    public static ErrorCondition mapFromProtonType(org.apache.qpid.proton4j.amqp.transport.ErrorCondition error) {
        if (error != null) {
            ErrorCondition condition = new ErrorCondition();

            condition.setCondition(error.getCondition());
            condition.setDescription(error.getDescription());
            condition.setInfo(error.getInfo());

            return condition;
        } else {
            return null;
        }
    }

    public static org.apache.qpid.proton4j.amqp.transport.ErrorCondition mapToProtonType(ErrorCondition error) {
        if (error != null) {
            return new org.apache.qpid.proton4j.amqp.transport.ErrorCondition(
                error.getCondition(), error.getDescription(), error.getInfo());
        } else {
            return null;
        }
    }

    public static Source mapFromProtonType(org.apache.qpid.proton4j.amqp.messaging.Source source) {
        if (source != null) {
            Source mapped = new Source();

            mapped.setAddress(source.getAddress());
            mapped.setDurable(source.getDurable().getValue());
            mapped.setExpiryPolicy(source.getExpiryPolicy().getPolicy());
            mapped.setTimeout(source.getTimeout());
            mapped.setDynamic(source.isDynamic());
            mapped.setDynamicNodeProperties(source.getDynamicNodeProperties());
            mapped.setDistributionMode(source.getDistributionMode());
            mapped.setFilter(source.getFilter());
            mapped.setDefaultOutcome(mapFromProtonType((DeliveryState) source.getDefaultOutcome()));
            mapped.setOutcomes(source.getOutcomes());
            mapped.setCapabilities(source.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }

    public static org.apache.qpid.proton4j.amqp.messaging.Source mapToProtonType(Source source) {
        if (source != null) {
            org.apache.qpid.proton4j.amqp.messaging.Source mapped =
                new org.apache.qpid.proton4j.amqp.messaging.Source();

            mapped.setAddress(source.getAddress());
            mapped.setDurable(TerminusDurability.valueOf(source.getDurable()));
            mapped.setExpiryPolicy(TerminusExpiryPolicy.valueOf(source.getExpiryPolicy()));
            mapped.setTimeout(source.getTimeout());
            mapped.setDynamic(source.getDynamic());
            mapped.setDynamicNodeProperties(source.getDynamicNodeProperties());
            mapped.setDistributionMode(source.getDistributionMode());
            mapped.setFilter(source.getFilter());
            mapped.setDefaultOutcome((Outcome) mapToProtonType(source.getDefaultOutcome()));
            mapped.setOutcomes(source.getOutcomes());
            mapped.setCapabilities(source.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }

    public static Target mapFromProtonType(org.apache.qpid.proton4j.amqp.messaging.Target target) {
        if (target != null) {
            Target mapped = new Target();

            mapped.setAddress(target.getAddress());
            mapped.setDurable(target.getDurable().getValue());
            mapped.setExpiryPolicy(target.getExpiryPolicy().getPolicy());
            mapped.setTimeout(target.getTimeout());
            mapped.setDynamic(target.isDynamic());
            mapped.setDynamicNodeProperties(target.getDynamicNodeProperties());
            mapped.setCapabilities(target.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }

    public static org.apache.qpid.proton4j.amqp.messaging.Target mapToProtonType(Target target) {
        if (target != null) {
            org.apache.qpid.proton4j.amqp.messaging.Target mapped =
                new org.apache.qpid.proton4j.amqp.messaging.Target();

            mapped.setAddress(target.getAddress());
            mapped.setDurable(TerminusDurability.valueOf(target.getDurable()));
            mapped.setExpiryPolicy(TerminusExpiryPolicy.valueOf(target.getExpiryPolicy()));
            mapped.setTimeout(target.getTimeout());
            mapped.setDynamic(target.getDynamic());
            mapped.setDynamicNodeProperties(target.getDynamicNodeProperties());
            mapped.setCapabilities(target.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }

    public static Coordinator mapFromProtonType(org.apache.qpid.proton4j.amqp.transactions.Coordinator coordinator) {
        if (coordinator != null) {
            Coordinator mapped = new Coordinator();

            mapped.setCapabilities(coordinator.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }

    public static org.apache.qpid.proton4j.amqp.transactions.Coordinator mapToProtonType(Coordinator coordinator) {
        if (coordinator != null) {
            org.apache.qpid.proton4j.amqp.transactions.Coordinator mapped =
                new org.apache.qpid.proton4j.amqp.transactions.Coordinator();

            mapped.setCapabilities(coordinator.getCapabilities());

            return mapped;
        } else {
            return null;
        }
    }
}
