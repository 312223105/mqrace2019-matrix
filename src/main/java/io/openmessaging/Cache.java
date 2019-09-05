package io.openmessaging;

import java.nio.ByteBuffer;

import static io.openmessaging.DefaultMessageStoreImpl.*;

/**
 * Created by xuzhe on 2019/9/4.
 */
public class Cache {
    public static void write(ByteBuffer src, int writeOffset, int id) {
        if(id == 2) {
            synchronized (data0) {
                src.get(data0, writeOffset, src.limit());
            }
        } else if(id == 3) {
            synchronized (data1) {
                src.get(data1, writeOffset, src.limit());
            }
        } else if(id == 16) {
            synchronized (data18) {
                byte[] tmp = new byte[src.limit()];
                src.get(tmp);
                data18.position(writeOffset);
                data18.put(tmp);
            }
        } else if(id == 17) {
            synchronized (data19) {
                byte[] tmp = new byte[src.limit()];
                src.get(tmp);
                data19.position(writeOffset);
                data19.put(tmp);
            }
        }
    }

    public static void read(ByteBuffer dst, int readOffset, int size, int id) {
        dst.clear();
        if(id == 2) {
            dst.put(data0, readOffset, size);
        } else if(id == 3) {
            dst.put(data1, readOffset, size);
        } else if(id == 16) {
            synchronized (data18) {
                data18.limit(readOffset+size);
                data18.position(readOffset);
                dst.put(data18);
            }
        } else if(id == 17) {
            synchronized (data19) {
                data19.limit(readOffset+size);
                data19.position(readOffset);
                dst.put(data19);
            }
        }
    }
}
