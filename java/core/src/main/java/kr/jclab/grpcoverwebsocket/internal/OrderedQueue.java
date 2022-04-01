package kr.jclab.grpcoverwebsocket.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class OrderedQueue {
    // Dequeue in chunks, so we don't have to acquire the queue's log too often.
    @VisibleForTesting
    static final int DEQUE_CHUNK_SIZE = 128;

    /**
     * {@link Runnable} used to schedule work onto the tail of the event loop.
     */
    private final Runnable later = new Runnable() {
        @Override
        public void run() {
            flush();
        }
    };

    private final ExecutorService executorService;
    private final Queue<QueuedCommand> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final Consumer<QueuedCommand> commandRunner;

    public OrderedQueue(ExecutorService executorService, Consumer<QueuedCommand> commandRunner) {
        this.executorService = executorService;
        this.commandRunner = commandRunner;
    }

    /**
     * Schedule a flush on the channel.
     */
    void scheduleFlush() {
        if (scheduled.compareAndSet(false, true)) {
            // Add the queue to the tail of the event loop so writes will be executed immediately
            // inside the event loop. Note DO NOT do channel.write outside the event loop as
            // it will not wake up immediately without a flush.
            this.executorService.execute(later);
        }
    }

    /**
     * Enqueue a write command on the channel.
     *
     * @param command a write to be executed on the channel.
     * @param flush true if a flush of the write should be schedule, false if a later call to
     *              enqueue will schedule the flush.
     */
    @CanIgnoreReturnValue
    public Future<Void> enqueue(QueuedCommand command, boolean flush) {
        // Detect erroneous code that tries to reuse command objects.
        Preconditions.checkArgument(command.promise() == null, "promise must not be set on command");

        CompletableFuture<Void> promise = new CompletableFuture<>();
        command.promise(promise);
        queue.add(command);
        if (flush) {
            scheduleFlush();
        }
        return promise;
    }

    /**
     * Enqueue the runnable. It is not safe for another thread to queue an Runnable directly to the
     * event loop, because it will be out-of-order with writes. This method allows the Runnable to be
     * processed in-order with writes.
     */
    public void enqueue(Runnable runnable, boolean flush) {
        queue.add(new RunnableCommand(runnable));
        if (flush) {
            scheduleFlush();
        }
    }

    /**
     * Executes enqueued work directly on the current thread. This can be used to trigger writes
     * before performing additional reads. Must be called from the event loop. This method makes no
     * guarantee that the work queue is empty when it returns.
     */
    void drainNow() {
        // Preconditions.checkState(channel.eventLoop().inEventLoop(), "must be on the event loop");
        if (queue.peek() == null) {
            return;
        }
        flush();
    }

    /**
     * Process the queue of commands and dispatch them to the stream. This method is only
     * called in the event loop
     */
    private void flush() {
        try {
            QueuedCommand cmd;
            while ((cmd = queue.poll()) != null) {
                if (cmd.isRunnable()) {
                    cmd.run();
                } else {
                    this.commandRunner.accept(cmd);
                }
            }
        } finally {
            // Mark the write as done, if the queue is non-empty after marking trigger a new write.
            scheduled.set(false);
            if (!queue.isEmpty()) {
                scheduleFlush();
            }
        }
    }

    private static class RunnableCommand implements QueuedCommand {
        private final Runnable runnable;

        public RunnableCommand(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public final void promise(CompletableFuture<Void> promise) {
            throw new UnsupportedOperationException();
        }

        @Override
        public final CompletableFuture<Void> promise() {
            throw new UnsupportedOperationException();
        }

        @Override
        public final void run() {
            runnable.run();
        }

        @Override
        public boolean isRunnable() {
            return true;
        }
    }

    public static class AbstractQueuedCommand implements QueuedCommand {
        private CompletableFuture<Void> promise;

        @Override
        public final void promise(CompletableFuture<Void> promise) {
            this.promise = promise;
        }

        @Override
        public final CompletableFuture<Void> promise() {
            return promise;
        }

        @Override
        public final void run() {}

        @Override
        public boolean isRunnable() {
            return false;
        }
    }

    /**
     * Simple wrapper type around a command and its optional completion listener.
     */
    public interface QueuedCommand {
        /**
         * Returns the promise beeing notified of the success/failure of the write.
         */
        CompletableFuture<Void> promise();

        /**
         * Sets the promise.
         */
        void promise(CompletableFuture<Void> promise);

        void run();

        boolean isRunnable();
    }
}
