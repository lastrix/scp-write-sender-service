package com.lastrix.scp.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;

public abstract class ChangeSenderService<T> {
    private static final Logger log = LoggerFactory.getLogger(ChangeSenderService.class);
    /**
     * How many message identifiers we should hold in our cache to prevent
     * duplicate messages registered in message queues
     */
    private static final int ID_SET_MAX_SIZE = 65535;
    /**
     * The maximum number of messages planned to be sent
     */
    private static final int MAX_FETCH = 4096;
    /**
     * If buffer goes below this value - fetcher should read source again
     */
    private static final int MAX_FETCH_HALF = MAX_FETCH / 2;
    private final ChangeSourceService<T> source;
    private final ChangeSender<T> sender;
    /**
     * Set this to false if you want to stop service
     */
    private volatile boolean running = true;
    /**
     * Thread for fetching messages from source (database)
     */
    private final Thread fetchThread;
    /**
     * This pool is used to run message sending jobs
     */
    private final ForkJoinPool workPool;
    /**
     * Holds information about channels and their respective topics
     */
    private final Map<Integer, WorkerContext> map = new HashMap<>();
    /**
     * Nanosecond interval between fetcher thread executions
     */
    private final long sleepTime = Duration.ofMillis(200).toNanos();
    /**
     * This queue contains messages that should be persisted in database.
     * If for some reason we won't be able to store this information, then next restart
     * we'll send those messages again
     */
    private final Queue<T> commitQueue = new ConcurrentLinkedQueue<>();
    /**
     * Limit the amount of messages sent to single topic
     */
    private final int maxProcessingChunk;
    /**
     * How many messages we collected in buffers so far
     */
    private int fetchCount = 0;
    /**
     * This collection works similar to LRU cache, but without access order check in order
     * to prevent undesired operations with structure (performance requirement)
     */
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
            commit();
            if (running && (shouldNotFetch() || !fetch())) {
                LockSupport.parkNanos(sleepTime);
            }
        }
    }

    private boolean shouldNotFetch() {
        // start fetching only if we reach certain
        // margin in our buffers
        // we should not strain our database too often
        return fetchCount >= MAX_FETCH_HALF;
    }

    private void notifyFetcher() {
        LockSupport.unpark(fetchThread);
    }

    protected abstract Object idOf(T o);

    protected abstract int channelOf(T o);

    private boolean fetch() {
        boolean empty = false;
        int page = 0;
        int fetched = 0;
        // we collect messages from database till page contains no more
        // elements, or we reached our buffer size
        while (!empty && fetchCount < MAX_FETCH) {
            var c = source.fetch(page);
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
        m.forEach((channel, list) -> map.get(channel).addAll(list));
    }


    private void commit() {
        // flush all changes to persistent storage
        while (!commitQueue.isEmpty()) {
            // 32 items updated in batch per transaction
            List<T> changes = fetchFromQueue(commitQueue, 32);
            source.commit(changes);
            fetchCount -= changes.size();
        }
    }

    private static <E> List<E> fetchFromQueue(Queue<E> q, int maxSize) {
        List<E> l = new ArrayList<>(maxSize);
        while (l.size() < maxSize && !q.isEmpty()) {
            l.add(q.poll());
        }
        return l;
    }

    private final class WorkerContext {
        /**
         * This is our topic identifier
         */
        private final int channel;
        /**
         * Message queue to buffer messages before sending
         */
        private final Queue<T> q = new LinkedList<>();
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
                while (running && !isEmpty() && processed < maxProcessingChunk) {
                    processed += sendMessages();
                }
                if (isEmpty()) {
                    notifyFetcher();
                } else {
                    workPool.submit(this::doWork);
                    _stopped = false;
                }
            } catch (Throwable e) {
                log.error("Unable to process changes", e);
            } finally {
                stopped = _stopped;
            }
        }

        private int sendMessages() {
            List<T> w;
            synchronized (q) {
                w = fetchFromQueue(q, 128);
            }
            var r = sender.send(w, channel);
            commitQueue.addAll(r);
            // source messages used because we need to ensure that
            // if something goes bad with this channel - others will get their
            // place, otherwise we'll lock here indefinitely
            int processed = w.size();
            log.info("Successfully sent {} of {} messages to channel {}", r.size(), processed, channel);
            // if something was not sent - we must reschedule it
            if (r.size() != w.size()) {
                w.removeAll(r);
                addAll(w);
            }
            return processed;
        }

        public void addAll(List<T> l) {
            synchronized (q) {
                q.addAll(l);
                if (stopped && !q.isEmpty()) {
                    stopped = false;
                    workPool.submit(this::doWork);
                }
            }
        }

        boolean isEmpty() {
            synchronized (q) {
                return q.isEmpty();
            }
        }
    }
}
