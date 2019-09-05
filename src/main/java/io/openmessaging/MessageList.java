package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;

import static io.openmessaging.Constant.BUCKET_MSG_COUNT;

/**
 * Created by xuzhe on 2019/8/26.
 */
public class MessageList {
    public int size;
    public long[] tArray;
    public long[] aArray;
    public byte[] bodyArray;
    private AtomicInteger refCnt = new AtomicInteger(0);

    public MessageList() {
        tArray = new long[BUCKET_MSG_COUNT];
        aArray = new long[BUCKET_MSG_COUNT];
        bodyArray = new byte[34*BUCKET_MSG_COUNT];
    }

    public MessageList(int size) {
        tArray = new long[size];
        aArray = new long[size];
        bodyArray = new byte[34*size];
    }

    public void add(Message msg) {
        int index = size++;
        tArray[index] = msg.getT();
        aArray[index] = msg.getA();
        System.arraycopy(msg.getBody(), 0, bodyArray, 34*index, 34);
    }

    public long getT(int index) {
        if(index >= size) {
            throw new RuntimeException(String.format("index out of range, index %d size %d", index, size));
        }
        return tArray[index];
    }

    public long getA(int index) {
        if(index >= size) {
            throw new RuntimeException(String.format("index out of range, index %d size %d", index, size));
        }
        return aArray[index];
    }

    public void getBody(int index, byte[] dst) {
        if(index >= size) {
            throw new RuntimeException(String.format("index out of range, index %d size %d", index, size));
        }
        System.arraycopy(bodyArray, index * 34, dst, 0, 34);
    }

    public void clear() {
        size = 0;
    }

    public void retain(int cnt) {
        refCnt.addAndGet(cnt);
    }

    public void release() {
        if(refCnt.decrementAndGet() == 0) {
            this.clear();
            MessageListPool.putMessageList(this);
        }
    }

    public static void insert(MessageList src, int index, MessageList dst) {
        int dstIndex = dst.size++;
        dst.tArray[dstIndex] = src.tArray[index];
        dst.aArray[dstIndex] = src.aArray[index];
        System.arraycopy(src.bodyArray, index*34, dst.bodyArray, dstIndex*34, 34);
    }
}
