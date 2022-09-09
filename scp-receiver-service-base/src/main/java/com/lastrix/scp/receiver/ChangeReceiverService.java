package com.lastrix.scp.receiver;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public abstract class ChangeReceiverService<T> {
    private static final Logger log = LoggerFactory.getLogger(ChangeReceiverService.class);
    /**
     * How many message identifiers we should hold in our cache to prevent
     * duplicate messages registered in message queues
     */
    private static final int ID_SET_MAX_SIZE = 65535;
    public static final long DURATION_SINK_WAIT = Duration.ofMillis(1000).toNanos();
    public static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(1);
    public static final Duration DURATION_ONE_SECOND = Duration.ofSeconds(1);
    public static final long ONE_SECOND_IN_NANOS = DURATION_ONE_SECOND.toNanos();

    private final Set<Object> idSet = Collections.newSetFromMap(new LinkedHashMap<>(ID_SET_MAX_SIZE + 1) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Object, Boolean> eldest) {
            return size() > ID_SET_MAX_SIZE;
        }
    });
    private final ChangeSinkService<T> sink;
    private final ChangeReceiver<T> receiver;
    private final Thread sinkThread;
    private final Thread receiverThread;

    /**
     * Queue for storing data in sink
     */
    private final Queue<ChangeChunk<T>> queue = new LinkedList<>();
    private int messageQueueSize = 0;
    /**
     * Queue for storing commit slabs, sent back to receiver
     */
    private final AtomicReference<Object> commitSlab = new AtomicReference<>(null);
    private final Timer timer = Time.SYSTEM.timer(DURATION_ONE_SECOND);

    private volatile boolean running = true;
    private final int sinkChunkSize;
    private final int maxSinkChunkSize;
    private final int receiveBufferSize;
    private final int receiveBufferSizeHalf;

    protected ChangeReceiverService(ChangeSinkService<T> sink, ChangeReceiver<T> receiver, int sinkChunkSize, int maxSinkChunkSize, int receiveBufferSize) {
        this.sink = sink;
        this.receiver = receiver;
        this.sinkChunkSize = sinkChunkSize;
        this.maxSinkChunkSize = maxSinkChunkSize;
        this.receiveBufferSize = receiveBufferSize;
        this.receiveBufferSizeHalf = receiveBufferSize / 2;
        sinkThread = new Thread(this::doSink, "change-sink-thread");
        sinkThread.setDaemon(true);
        receiverThread = new Thread(this::doReceive, "change-receive-thread");
        receiverThread.setDaemon(true);
        sinkThread.start();
        receiverThread.start();
    }

    @PreDestroy
    public void preDestroy() {
        running = false;
    }

    protected abstract Object idOf(T c);

    private void doReceive() {
        while (running) {
            try {
                commitReceive();
                int i = 10;
                while (running && i > 0 && shouldReceive() && doReceiveUnsafe()) {
                    i--;
                }
                // if no receive occured
                if (i == 10) LockSupport.parkNanos(DURATION_SINK_WAIT);
            } catch (Throwable e) {
                log.error("Failed to process", e);
                waitOnError();
            }
        }
    }

    private boolean shouldReceive() {
        synchronized (queue) {
            return messageQueueSize < receiveBufferSizeHalf;
        }
    }

    private boolean doReceiveUnsafe() {
        ChangeChunk<T> chunk = receiver.receive(receiveBufferSize, RECEIVE_TIMEOUT);
        // we double-check running here to prevent further work
        if (running && !chunk.isEmpty()) {
            synchronized (queue) {
                queue.add(chunk);
                messageQueueSize += chunk.changes().size();
                if (messageQueueSize > sinkChunkSize) LockSupport.unpark(sinkThread);
            }
            return true;
        }
        return false;
    }

    private void commitReceive() {
        Object o = commitSlab.get();
        if (o != null) {
            receiver.commit(o);
            // replace that value with null to prevent us
            // from sending same commit again
            commitSlab.compareAndSet(o, null);
        }
    }

    private void doSink() {
        while (running) {
            try {
                timer.update();
                while (shouldCommit()) {
                    doCommit();
                    timer.update();
                }
                LockSupport.parkNanos(DURATION_SINK_WAIT);
            } catch (Throwable e) {
                log.error("Failed to process", e);
                waitOnError();
            }
        }
    }

    private void doCommit() {
        Object slab = null;
        List<T> list = new ArrayList<>(maxSinkChunkSize);
        int processed = 0;
        synchronized (queue) {
            while (list.size() < sinkChunkSize && !queue.isEmpty()) {
                ChangeChunk<T> chunk = queue.poll();
                for (T c : chunk.changes()) {
                    // there is no need to commit same messages
                    // again, so we skip them and reduce our
                    // storage strain significantly
                    if (idSet.add(idOf(c))) list.add(c);
                }
                // when update is done we should send this object
                // back to receiver in order to commit state
                slab = chunk.slab();
                // because there is no way to understand
                // how many messages we may process when
                // queue registration occurs, we should
                // do it here, that is why we are not
                // using list.size() later
                processed += chunk.changes().size();
            }
        }
        sink.commit(list);
        commitSlab.set(slab);
        synchronized (queue) {
            messageQueueSize -= processed;
        }
        timer.updateAndReset(DURATION_ONE_SECOND.toMillis());
    }

    private boolean shouldCommit() {
        synchronized (queue) {
            if (messageQueueSize == 0) {
                // we must notify receiver thread to
                // start fetching again, it may be asleep
                LockSupport.unpark(receiverThread);
                return false;
            }
            return (messageQueueSize > sinkChunkSize || timer.isExpired());
        }
    }

    private void waitOnError() {
        try {
            // wait 15 seconds if any error present,
            // so we won't spam our log too much
            Thread.sleep(Duration.ofSeconds(15).toMillis());
        } catch (InterruptedException ignored) {
            // do nothing
        }
    }
}
