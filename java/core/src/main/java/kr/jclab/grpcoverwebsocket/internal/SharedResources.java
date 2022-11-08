package kr.jclab.grpcoverwebsocket.internal;

import io.grpc.internal.GrpcUtil;
import io.grpc.internal.SharedResourceHolder;

import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;

public class SharedResources {
    public static final SharedResourceHolder.Resource<ExecutorService> TRANSPORT_EXECUTOR_SERVICE =
            new SharedResourceHolder.Resource<ExecutorService>() {
                @Override
                public ExecutorService create() {
                    // We don't use newSingleThreadScheduledExecutor because it doesn't return a
                    // ScheduledThreadPoolExecutor.
                    ExecutorService service = Executors.newCachedThreadPool(
                            GrpcUtil.getThreadFactory("gows-transport-%d", true)
                    );

                    // If there are long timeouts that are cancelled, they will not actually be removed from
                    // the executors queue.  This forces immediate removal upon cancellation to avoid a
                    // memory leak.  Reflection is used because we cannot use methods added in Java 1.7.  If
                    // the method does not exist, we give up.  Note that the method is not present in 1.6, but
                    // _is_ present in the android standard library.
                    try {
                        Method method = service.getClass().getMethod("setRemoveOnCancelPolicy", boolean.class);
                        method.invoke(service, true);
                    } catch (NoSuchMethodException e) {
                        // no op
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    return Executors.unconfigurableExecutorService(service);
                }

                @Override
                public void close(ExecutorService instance) {
                    instance.shutdown();
                }
            };
}
