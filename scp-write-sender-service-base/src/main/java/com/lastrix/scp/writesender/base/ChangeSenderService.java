package com.lastrix.scp.writesender.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class ChangeSenderService<T> {
    private static final Logger log = LoggerFactory.getLogger(ChangeSenderService.class);
    private static final int ID_SET_MAX_SIZE = 65535;
    private static final int MAX_FETCH = 4096;
    private final ChangeSourceService<T> source;
    private final ChangeSender<T> sender;
    private volatile boolean running = true;
    private final Thread fetchThread;
    private final ForkJoinPool workPool;
    private final Map<Integer, WorkerContext> map = new HashMap<>();
    private final long sleepTime = Duration.ofMillis(50).toNanos();
    private final Queue commitQueue = new ConcurrentLinkedQueue();
    private final int maxProcessingChunk;
    private int fetchCount = 0;
    private final Set<Object> idSet = Collections.newSetFromMap(new LinkedHashMap<>(ID_SET_MAX_SIZE + 1) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Object, Boolean> eldest) {
            return size() > ID_SET_MAX_SIZE;
        }
    });

    protected ChangeSenderService(ChangeSourceService<T> source, ChangeSender<T> sender, int parallelism, int channels, int maxProcessingChunk) {
        this.source = source;
        this.sender = sender;
        this.maxProcessingChunk = maxProcessingChunk;
        workPool = new ForkJoinPool(parallelism);
        for (int i = 0; i < channels; i++) {
            map.put(i, new WorkerContext(i));
        }
        fetchThread = new Thread(this::doBackground, "change-sender-fetch-thread");
        fetchThread.setDaemon(true);
        fetchThread.start();
    }

    @PreDestroy
    public void preDestroy() {
        running = false;
        workPool.shutdown();
    }

    public void doBackground() {
        while (running) {
            commitChanges();
            if (!fetchChanges()) {
                LockSupport.parkNanos(sleepTime);
            }
        }
    }

    private void notifyFetcher() {
        LockSupport.unpark(fetchThread);
    }

    protected abstract Object idOf(T o);

    protected abstract int channelOf(T o);

    private boolean fetchChanges() {
        boolean empty = false;
        int page = 0;
        int fetched = 0;
        while (!empty && fetchCount < MAX_FETCH) {
            var c = source.getChanges(page);
            registerChanges(c);
            page++;
            fetched += c.size();
            empty = c.isEmpty();
        }
        return fetched > 0;
    }

    private void registerChanges(List<T> c) {
        Map<Integer, List<T>> m = new HashMap<>();
        for (T o : c) {
            if (idSet.add(idOf(o))) {
                m.computeIfAbsent(channelOf(o), ignored -> new ArrayList<>(c.size()))
                        .add(o);
                fetchCount++;
            }
        }
        m.forEach((channel, list) -> map.get(channel).add(list));
    }


    private void commitChanges() {
        // flush all changes to persistent storage
        while (!commitQueue.isEmpty()) {
            List<T> changes = fetchFromQueue(commitQueue, 32);
            source.commitChanges(changes);
            fetchCount -= changes.size();
        }
    }

    private static <E> List<E> fetchFromQueue(Queue<E> q, int maxSize) {
        List l = new ArrayList(maxSize);
        while (l.size() < maxSize && !q.isEmpty()) {
            l.add(q.poll());
        }
        return l;
    }

    private final class WorkerContext {
        private final int channel;
        private final Queue<T> q = new ConcurrentLinkedQueue<>();
        private volatile boolean stopped = true;

        public WorkerContext(int channel) {
            this.channel = channel;
        }

        private void doWork() {
            boolean _stopped = true;
            try {
                // we should process at max maxProcessingChunk messages
                // and then reschedule ourselves in order to let other
                // channels to be processed
                int processed = 0;
                while (!q.isEmpty() && processed < maxProcessingChunk) {
                    var w = fetchFromQueue(q, 128);
                    var r = sender.send(w, channel);
                    commitQueue.addAll(r);
                    processed += r.size();
                    log.info("Sent {}/{} messages to kafka channel {}", r.size(), w.size(), channel);
                    // if something was not sent - we must reschedule it
                    if (r.size() != w.size()) {
                        w.removeAll(r);
                        q.addAll(w);
                    }
                }
                if (!q.isEmpty()) {
                    workPool.submit(this::doWork);
                    _stopped = false;
                } else {
                    notifyFetcher();
                }
            } catch (Throwable e) {
                log.error("Unable to process changes", e);
            } finally {
                stopped = _stopped;
            }
        }

        public void add(List<T> l) {
            q.addAll(l);
            if (stopped) {
                stopped = false;
                workPool.submit(this::doWork);
            }
        }
    }
}
