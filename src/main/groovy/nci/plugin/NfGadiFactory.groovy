/*
 * Copyright 2025, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nci.plugin

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/**
 * Implements a factory object required to create
 * the {@link NfGadiObserver} instance.
 */
@CompileStatic
class NfGadiFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        final result = new ArrayList()
        result.add( new NfGadiObserver(session) )
        return result
    }
    // Collection<TraceObserver> create(Session session) {
    //     return List.<TraceObserver>of(new NfGadiObserver())
    // }

}
