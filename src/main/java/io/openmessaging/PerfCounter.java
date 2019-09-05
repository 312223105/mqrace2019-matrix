package io.openmessaging;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.dataDir;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class PerfCounter {
    public static AtomicInteger PUT_IO_COUNTER = new AtomicInteger();
    public static AtomicInteger GET_MSG_IO_COUNTER = new AtomicInteger();
    public static AtomicInteger GET_MSG_CALL_COUNTER = new AtomicInteger();
    public static AtomicInteger GET_AVG_IO_COUNTER = new AtomicInteger();
    public static AtomicInteger GET_AVG_CALL_COUNTER = new AtomicInteger();
    public static volatile long PUT_START = 0;
    public static volatile long PUT_END = 0;
    public static volatile long GET_MSG_START = 0;
    public static volatile long GET_MSG_END = 0;
    public static volatile long GET_AVG_START = 0;
    public static volatile long GET_AVG_END = 0;

    public static void printPerfDetail() {
        GET_AVG_END = System.currentTimeMillis();
        String msg = String.format(
                "PUT_ELAPSED          :%d\n" +
                "GET_MSG_ELAPSED      :%d\n" +
                "GET_AVG_ELAPSED      :%d\n" +
                "PUT_IO_COUNTER       :%d\n" +
                "GET_MSG_IO_COUNTER   :%d\n" +
                "GET_AVG_IO_COUNTER   :%d\n" +
                "GET_MSG_CALL_COUNTER :%d\n" +
                "GET_AVG_CALL_COUNTER :%d\n",
                (PUT_END-PUT_START),
                (GET_MSG_END-GET_MSG_START),
                (GET_AVG_END-GET_AVG_START),
                PUT_IO_COUNTER.get(),
                GET_MSG_IO_COUNTER.get(),
                GET_AVG_IO_COUNTER.get(),
                GET_MSG_CALL_COUNTER.get(),
                GET_AVG_CALL_COUNTER.get()

        );
        System.out.println(msg);

        File file = new File(dataDir);
        for(File f : file.listFiles()) {
            System.out.printf("%s size: %d\n", f.getName(), f.length());
        }

    }
}
