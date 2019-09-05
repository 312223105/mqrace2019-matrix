package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.openmessaging.PerfCounter.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageStoreImpl extends MessageStore {
    IndexBuilder indexBuilder;
    Thread indexThread;

    MergeTask mergeTasks;
    Thread mergeThreads;

    boolean writeFinished = false;
    static int size1;
    static int size2;
    static byte[] data0 = new byte[785*1024*1024];
    static byte[] data1 = new byte[785*1024*1024];
    static ByteBuffer data18 = ByteBuffer.allocateDirect(785*1024*1024);
    static ByteBuffer data19 = ByteBuffer.allocateDirect(785*1024*1024);
//    static byte[] data0 = new byte[300*1024*1024];
//    static byte[] data1 = new byte[840*1024*1024];
//    static ByteBuffer data18 = ByteBuffer.allocateDirect(840*1024*1024);
//    static ByteBuffer data19 = ByteBuffer.allocateDirect(700*1024*1024);
    public DefaultMessageStoreImpl() {

        Runtime.getRuntime().addShutdownHook(new Thread(PerfCounter::printPerfDetail));
        MessageListPool.init();
        mergeTasks = new MergeTask();
        mergeThreads = new Thread(mergeTasks);
        mergeThreads.start();

        indexBuilder = new IndexBuilder();
        indexThread = new Thread(indexBuilder);
        indexThread.start();
        IndexSearcher.VoidFun();

        PUT_START = System.currentTimeMillis();
    }

    @Override
    public void put(Message message) {

        mergeTasks.addMessage(message);
    }



    @Override
    public List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) {
        if(!writeFinished) {
            synchronized (this) {
                if(!writeFinished) {
                    mergeTasks.waitFinish();
                    indexBuilder.waitFinish();
                    writeFinished = true;
                    try {
                        long t1 = System.currentTimeMillis();
                        List<Index.ATIndex[]> atIndexArray = IndexSearcher.atIndexArray;
                        Index.TAIndex[] taIndexArray = IndexSearcher.taIndexArray;

                        for (int i = 0; i < atIndexArray.size(); i++) {
                            Arrays.sort(atIndexArray.get(i), 0, IndexSearcher.atIndexCounters[i]);
                            System.out.printf("[%2d] at %d ta %d\n", i, IndexSearcher.atIndexCounters[i], IndexSearcher.taIndexCounter);

                        }
                        Arrays.sort(taIndexArray, 0, IndexSearcher.taIndexCounter);
                        long t2 = System.currentTimeMillis();
                        System.out.printf("sort time %d ms\n", t2 - t1);
                        PUT_END = System.currentTimeMillis();
                        GET_MSG_START = PUT_END;
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                }
            }
        }
        try {
            GET_MSG_CALL_COUNTER.getAndIncrement();
            return IndexSearcher.getMessage(aMin, aMax, tMin, tMax);
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }

        return null;
    }


    @Override
    public long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        int msg_check_counter = GET_AVG_CALL_COUNTER.getAndIncrement();
        if(msg_check_counter == 0) {
            GET_MSG_END = System.currentTimeMillis();
            GET_AVG_START = GET_MSG_END;
        }
//        if(msg_check_counter > 34000) {
//            System.exit(-1);
//        }
//        getMessageT2 = System.currentTimeMillis();
//        System.out.printf("getMessage elapsed %d ms\n", (getMessageT2-getMessageT1));
//        System.exit(-1);
        return IndexSearcher.getAvgValue(aMin, aMax, tMin, tMax);
    }

}
