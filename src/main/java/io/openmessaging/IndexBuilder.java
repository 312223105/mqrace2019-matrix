package io.openmessaging;

import java.util.Arrays;
import java.util.concurrent.*;

import static io.openmessaging.Constant.SAMPLE_COUNT;
import static io.openmessaging.Constant.T_INTERVAL;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class IndexBuilder implements Runnable {
    public static BlockingQueue<MessageList> blockingQueue = new ArrayBlockingQueue<>(3);
    public static long[] aNodeArray = null;
    private CompletableFuture<long[]> lastWriteFuture = null;
    private CompletableFuture<Object> finishFuture = new CompletableFuture<>();

    private ExecutorService builderService = Executors.newFixedThreadPool(3, (r) -> {
        Thread t = new Thread(r);
        t.setPriority(10);
        return t;
    });

    MessageList sampleList = new MessageList(SAMPLE_COUNT);

    MessageList currentList = MessageListPool.takeMessageList();

    long currentTIndex = -1;
    private long countOfPrevT = 0;
    public IndexBuilder() {

    }

    private void flushList(MessageList sortedList) {

        if(sortedList == null || sortedList.size == 0) {
            return;
        }
        if(aNodeArray == null) {
            if(sampleList.size + sortedList.size >= SAMPLE_COUNT) {
                long[] aArray = Arrays.copyOf(sampleList.aArray, sampleList.size);
                Arrays.sort(aArray);
                aNodeArray = new long[20];
                aNodeArray[0] = 0;
                int interval = sampleList.size / 20;
                for (int i = 1; i < 20; i++) {
                    aNodeArray[i] = aArray[i * interval];
                    System.out.printf("aNode[%2d] :%20d\n", i, aNodeArray[i]);
                }

                appendMessage(sampleList);
                appendMessage(sortedList);
                sampleList = null;
            } else {
                for (int i = 0; i < sortedList.size; i++) {
                    MessageList.insert(sortedList, i, sampleList);
                }
            }
        } else {
            appendMessage(sortedList);
        }
        MessageListPool.putMessageList(sortedList);
    }

    private void appendMessage(MessageList sortedList) {
        if(currentTIndex == -1) {
            currentTIndex = sortedList.getT(0) / T_INTERVAL;
        }
        for (int i = 0; i < sortedList.size; i++) {
            long tIndex = sortedList.getT(i) / T_INTERVAL;

            if(tIndex != currentTIndex) {
                if(tIndex < currentTIndex) {
                    throw new RuntimeException("error nowIndex " + tIndex + " curr " + currentTIndex);
                }
                IndexContext context = new IndexContext(currentList, lastWriteFuture, countOfPrevT);
                lastWriteFuture = context.writeFuture;
                builderService.submit(context);
                countOfPrevT += currentList.size; // 用于写ta文件时，计算写入偏移
                currentList = MessageListPool.takeMessageList();
                currentTIndex = tIndex;
            }
            MessageList.insert(sortedList, i, currentList);
        }
    }


    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doRun() throws InterruptedException, ExecutionException {
        while(true) {
            MessageList list = blockingQueue.take();
            if(list == null || list.size == 0) {
                if(currentList != null && currentList.size != 0) {
                    IndexContext context = new IndexContext(currentList, lastWriteFuture, countOfPrevT);

                    lastWriteFuture = context.writeFuture;
                    builderService.submit(context);
                }
                lastWriteFuture.get();
                finishFuture.complete(new Object());
                break;
            }
            flushList(list);
        }
    }

    public void waitFinish() {
        try {
            finishFuture.get();
            builderService.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
