package io.openmessaging;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.Constant.*;

/**
 * Created by xuzhe on 2019/8/22.
 */
public class MergeTask implements Runnable {
    private static final AtomicLong msg_counter = new AtomicLong(0);
    private static final AtomicInteger tid_counter = new AtomicInteger(0);
    private static ThreadLocal<Integer> tid = ThreadLocal.withInitial(() ->
            tid_counter.getAndIncrement()
    );

    private static ConcurrentMap<Integer, Thread> threadMap
            = new ConcurrentHashMap<>();
    public ConcurrentMap<Integer, MessageList> listConcurrentMap
            = new ConcurrentHashMap<>();
    private HashMap<Integer, BlockingQueue<MessageList>> bucketMap = new HashMap<>();
    private MessageList sortedList = MessageListPool.takeMessageList();
    private ThreadLocal<MessageList> localMsgList = ThreadLocal.withInitial(() ->
            MessageListPool.takeMessageList()
    );

    private CompletableFuture<Object> finishFuture = new CompletableFuture<>();

    public MergeTask() {
        for (int i = 0; i < 12; i++) {
            bucketMap.put(i, new ArrayBlockingQueue<>(BQ_SIZE));
        }

    }

    public void addMessage(Message message) {
        MessageList localList = localMsgList.get();
        if (localList.size >= LOCAL_LIST_SIZE) {
            threadMap.putIfAbsent(tid.get(), Thread.currentThread());
            try {
                bucketMap.get(tid.get()).put(localList);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            MessageList newList = null;
            newList = MessageListPool.takeMessageList();

            localMsgList.set(newList);
            listConcurrentMap.put(tid.get(), newList);
            localList = newList;
        }
        msg_counter.getAndIncrement();
        localList.add(message);
    }


    @Override
    public void run() {
        try {
            doRun();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void doRun() throws InterruptedException {
        long t1 = System.currentTimeMillis();
        PriorityQueue<Pair> pq = new PriorityQueue<>(bucketMap.size());
        for (int i = 0; i < bucketMap.size(); i++) {
            BlockingQueue<MessageList> bq = bucketMap.get(i);
            pq.add(new Pair(bq, i, bq.take(), 0));
        }

        long msgCount = 0;
        long start = System.currentTimeMillis();
        long startT = 0, endT;
        while (pq.size() > 0) {
            Pair pair = pq.poll();

            MessageList.insert(pair.current, pair.index, sortedList);
            if (sortedList.size == BUCKET_MSG_COUNT) {
                IndexBuilder.blockingQueue.put(sortedList);
                MessageList newList = MessageListPool.takeMessageList();
                sortedList = newList;
            }
            if (pair.current.size > pair.index + 1) {
                pair.reset(pair.source, pair.sourceIndex, pair.current, pair.index + 1);
                pq.add(pair);
            } else if (pair.source != null) {
                MessageListPool.putMessageList(pair.current);
                MessageList list = null;
                while (list == null) {
                    if (threadMap.get(pair.sourceIndex).isAlive() || pair.source.size() > 0) {
                        try {
                            list = pair.source.poll(50, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        break;
                    }
                }
                if (list == null) {
                    list = listConcurrentMap.get(pair.sourceIndex);
                    if (list != null && list.size > 0) {
                        pair.reset(null, pair.sourceIndex, list, 0);
                        pq.add(pair);
                        System.out.printf("[%d] ended startT %d lastT %d\n",
                                pair.sourceIndex, list.getT(0), list.getT(list.size - 1));
                    }
                } else {
                    pair.reset(pair.source, pair.sourceIndex, list, 0);
                    pq.add(pair);
                }
            }

            if (++msgCount % 10000000 == 0 && PRINT_WRITE_RATE) {
                endT = pair.current.getT(pair.index);
                long end = System.currentTimeMillis();
                System.out.printf("beginT: %d endT: %d count: %d elapsed:%d ms\n",
                        startT, endT, msgCount, (end - start));
                start = System.currentTimeMillis();
                startT = 0;
            }
        }

        if (sortedList.size != 0) {
            IndexBuilder.blockingQueue.put(sortedList);
        }
        IndexBuilder.blockingQueue.put(MessageListPool.takeMessageList());
        while (IndexBuilder.blockingQueue.size() != 0) {
            Thread.sleep(1);
        }
        finishFuture.complete(new Object());
        long t2 = System.currentTimeMillis();
        System.out.println("Merge Thread exit. " + (t2 - t1) + " ms");
    }

    public void waitFinish() {
        try {
            finishFuture.get();
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    private static class Pair implements Comparable<Pair> {
        BlockingQueue<MessageList> source;
        MessageList current;
        int sourceIndex;
        int index;

        public Pair(BlockingQueue<MessageList> source, int sourceIndex,
                    MessageList current, int index) {
            this.source = source;
            this.current = current;
            this.sourceIndex = sourceIndex;
            this.index = index;
        }

        public void reset(BlockingQueue<MessageList> source, int sourceIndex,
                          MessageList current, int index) {
            this.source = source;
            this.current = current;
            this.sourceIndex = sourceIndex;
            this.index = index;
        }

        @Override
        public int compareTo(Pair o) {
            return (int) (current.getT(index) - o.current.getT(o.index));
        }
    }
}
