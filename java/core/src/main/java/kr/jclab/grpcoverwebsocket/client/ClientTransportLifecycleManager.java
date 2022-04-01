/*
 * Copyright 2016 The gRPC Authors
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

package kr.jclab.grpcoverwebsocket.client;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.grpc.Status;
import io.grpc.internal.ManagedClientTransport;

public class ClientTransportLifecycleManager {
    public interface LifecycleManagerListener {
        void afterShutdown();
        void afterTerminate();
    }

    private final LifecycleManagerListener lifecycleListener;
    private final ManagedClientTransport.Listener transportListener;
    private boolean transportReady;
    private boolean transportShutdown;
    private boolean transportInUse;
    /** null iff !transportShutdown. */
    private Status shutdownStatus;
    /** null iff !transportShutdown. */
    private Throwable shutdownThrowable;
    private boolean transportTerminated;

    public ClientTransportLifecycleManager(
            LifecycleManagerListener lifecycleListener,
            ManagedClientTransport.Listener listener
    ) {
        this.lifecycleListener = lifecycleListener;
        this.transportListener = listener;
    }

    public void notifyReady() {
        if (transportReady || transportShutdown) {
            return;
        }
        transportReady = true;
        transportListener.transportReady();
    }

    /**
     * Marks transport as shutdown, but does not set the error status. This must eventually be
     * followed by a call to notifyShutdown.
     */
    public void notifyGracefulShutdown(Status s) {
        if (transportShutdown) {
            return;
        }
        transportShutdown = true;
        transportListener.transportShutdown(s);
    }

    /** Returns {@code true} if was the first shutdown. */
    @CanIgnoreReturnValue
    public boolean notifyShutdown(Status s) {
        notifyGracefulShutdown(s);
        if (shutdownStatus != null) {
            return false;
        }
        shutdownStatus = s;
        shutdownThrowable = s.asException();
        lifecycleListener.afterShutdown();
        return true;
    }

    public void notifyInUse(boolean inUse) {
        if (inUse == transportInUse) {
            return;
        }
        transportInUse = inUse;
        transportListener.transportInUse(inUse);
    }

    public void notifyTerminated(Status s) {
        if (transportTerminated) {
            return;
        }
        transportTerminated = true;
        notifyShutdown(s);
        transportListener.transportTerminated();
        lifecycleListener.afterTerminate();
    }

    public Status getShutdownStatus() {
        return shutdownStatus;
    }

    public Throwable getShutdownThrowable() {
        return shutdownThrowable;
    }

    public boolean transportTerminated() {
        return this.transportTerminated;
    }
}
