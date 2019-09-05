package io.openmessaging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static io.openmessaging.Constant.A_BUCKET_CNT;
import static io.openmessaging.Constant.dataDir;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class WriteContext {
    public static final WriteContext INSTANCE = new WriteContext();

    public static ThreadLocal<ByteBuffer[]> atBuffer = ThreadLocal.withInitial(() -> {
        ByteBuffer[] array = new ByteBuffer[A_BUCKET_CNT];
        for (int i = 0; i < array.length; i++) {
            array[i] = ByteBuffer.allocateDirect(4096*4);
        }
        return array;
    });


    public static ThreadLocal<ByteBuffer[]> bodyBuffer = ThreadLocal.withInitial(() -> {
        ByteBuffer[] array = new ByteBuffer[A_BUCKET_CNT];
        for (int i = 0; i < array.length; i++) {
            array[i] = ByteBuffer.allocateDirect(4096*16);
        }
        return array;
    });
    FileChannel taChannel;
    FileChannel[] atChannel = new FileChannel[A_BUCKET_CNT];
    FileChannel[] bodyChannel = new FileChannel[A_BUCKET_CNT];

    private WriteContext() {
        File f = new File(dataDir);
        if(!f.exists()) {
            f.mkdirs();
        }
        try {
            taChannel = new RandomAccessFile(dataDir + "ta.dat", "rw").getChannel();
            for (int i = 0; i < atChannel.length; i++) {
                atChannel[i] = new RandomAccessFile(dataDir + "at.dat" + i, "rw").getChannel();
                bodyChannel[i] = new RandomAccessFile(dataDir + "body.dat" + i, "rw").getChannel();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
