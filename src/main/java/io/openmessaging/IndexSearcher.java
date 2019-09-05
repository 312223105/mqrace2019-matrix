package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static io.openmessaging.Constant.T_INTERVAL;
import static io.openmessaging.Constant.UNIT_SIZE;
import static io.openmessaging.IndexBuilder.aNodeArray;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class IndexSearcher {
    public static void VoidFun() {}
    private static ThreadLocal<ByteBuffer> atByteBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocateDirect(4096*64);
    });
    private static ThreadLocal<ByteBuffer> bodyByteBuffer = ThreadLocal.withInitial(() -> {
        return ByteBuffer.allocateDirect(4096*256);
    });

    public static List<Index.ATIndex[]> atIndexArray = new ArrayList<>(20);
    public static Index.TAIndex[] taIndexArray = new Index.TAIndex[1050_0000];
    public static int[] atIndexCounters = new int[20];
    public static int taIndexCounter = 0;
    static {
        for (int i = 0; i < 20; i++) {
            atIndexArray.add(new Index.ATIndex[1050_0000/20]);
        }
    }
    public static List<Message> getMessage(long aMin, long aMax, long tMin, long tMax) throws IOException {
        List<Message> result = new ArrayList<>();
        long aMinIndex = Util.lower_bound(IndexBuilder.aNodeArray, aMin);
        long aMaxIndex = Util.lower_bound(IndexBuilder.aNodeArray, aMax);
        long tMinIndex = tMin / T_INTERVAL;
        long tMaxIndex = tMax / T_INTERVAL;
        for (long aIndex = aMinIndex; aIndex <= aMaxIndex ; aIndex++) {
            Index.ATIndex atIndexSearch = new Index.ATIndex();

            List<Index.ATIndex> indices = new ArrayList<>();
            int atLength = 0;
            int cnt = 0;
            long atReadOffset = -1;
            long bodyReadOffset = -1;

            for (long tIndex = tMinIndex; tIndex <= tMaxIndex; tIndex++) {
                atIndexSearch.key = (tIndex << 8) + aIndex;
                int index = Arrays.binarySearch(atIndexArray.get((int) aIndex), 0, atIndexCounters[(int) aIndex], atIndexSearch);
                if(index >= 0) {
                    Index.ATIndex index1 = atIndexArray.get((int) aIndex)[index];
                    atLength += (index1.itemOffset & 0xFFFFL);
                    cnt += index1.cnt;
                    indices.add(index1);
                    if (atReadOffset == -1) {
                        atReadOffset = (index1.itemOffset >>> 16);
                        bodyReadOffset = index1.bodyOffset * 34L;
                    }
                }
            }
            if(atReadOffset == -1) {
                continue;
            }
            ByteBuffer at = atByteBuffer.get();
            at.clear();
            at.limit(atLength);
            if(aIndex == 2 || aIndex == 3 || aIndex == 16 || aIndex == 17) {
                Cache.read(at, (int)atReadOffset, atLength, (int) aIndex);
            } else {
                int read = WriteContext.INSTANCE.atChannel[(int) aIndex].read(at, atReadOffset);
                if(read != atLength) {
                    System.out.println("read error");
                }
            }
            at.flip();
            ByteBuffer body = bodyByteBuffer.get();
            body.clear();
            body.limit(cnt * 34);

            int read = WriteContext.INSTANCE.bodyChannel[(int) aIndex].read(body, bodyReadOffset);
            if(read != cnt * 34) {
                System.out.println("read error");
            }
            body.flip();

            int total = 0;
            for (Index.ATIndex index : indices) {
                long tBase = (index.key >> 8) * T_INTERVAL;
                int aIndex1 = (int) (index.key & 0xFFL);
                long lastT = tBase;
                for (int i = 0; i < index.cnt; i++) {
                    long t = VarInt.getVarLong(at) + lastT;
                    lastT = t;
                    long a = VarInt.getVarLong(at) + aNodeArray[aIndex1];
                    try {
                        if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                            byte[] b = new byte[34];
                            body.position(total * 34);
                            body.get(b);
                            Message m = new Message(a, t, b);
                            result.add(m);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                    ++total;
                }
            }

            PerfCounter.GET_MSG_IO_COUNTER.getAndAdd(2);

        }
        result.sort(new MessageComparator());
//        System.out.printf("tMin %d size %d\n", tMin, result.size());
        return result;
    }

    public static long getAvgValueSlow(long aMin, long aMax, long tMin, long tMax) {
        if(aMax > 0) {
            throw new RuntimeException("已修改存储格式，不再可用");
        }
        try {
            int size = (int) WriteContext.INSTANCE.taChannel.size();
            ByteBuffer buffer = ByteBuffer.allocateDirect(size);
            WriteContext.INSTANCE.taChannel.read(buffer);
            buffer.flip();
            long sum = 0;
            long cnt = 0;
            while (buffer.remaining() >= UNIT_SIZE) {
                long t = buffer.getLong();
                long a = buffer.getLong();
                if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                    sum += a;
                    ++cnt;
                }
            }
            System.out.printf("sum %d cnt %d\n", sum, cnt);
            return cnt == 0 ? 0 : sum / cnt;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static long getAvgValue(long aMin, long aMax, long tMin, long tMax) {
        long aMinIndex = Util.lower_bound(IndexBuilder.aNodeArray, aMin);
        long aMaxIndex = Util.lower_bound(IndexBuilder.aNodeArray, aMax);
        long tMinIndex = tMin / T_INTERVAL;
        long tMaxIndex = tMax / T_INTERVAL;
        Index.ATIndex current = new Index.ATIndex();
        Index.ATIndex found = null;
        long sum = 0;
        int cnt = 0;
        int index;
        for (long aIndex = aMinIndex + 1; aIndex < aMaxIndex ; aIndex++) {
            for (long tIndex = tMinIndex+1; tIndex < tMaxIndex ; tIndex++) {
                current.key = (tIndex << 8) + aIndex;
                index = Arrays.binarySearch(atIndexArray.get((int) aIndex), 0, atIndexCounters[(int) aIndex], current);
                found = null;
                if(index >= 0) {
                    found = atIndexArray.get((int) aIndex)[index];
                    sum += found.sum;
                    cnt += found.cnt;
                } else {
//                    System.out.println("error");
                }
            }
        }
        ByteBuffer buffer = atByteBuffer.get();
        // 读AT文件
        long leftKeyStart = (tMinIndex << 8) + aMinIndex;
        long leftKeyEnd = (tMaxIndex << 8) + aMinIndex;
        long rightKeyStart = (tMinIndex << 8) + aMaxIndex;
        long rightKeyEnd = (tMaxIndex << 8) + aMaxIndex;
        if(leftKeyStart <= leftKeyEnd) {
            List<Index.ATIndex> indices = readATFile(leftKeyStart, leftKeyEnd, aMinIndex, buffer);
            buffer.flip();
            for(Index.ATIndex index1 : indices) {
                long tBase = (index1.key >> 8) * T_INTERVAL;
                long lastT = tBase;
                for (int i = 0; i < index1.cnt; i++) {
                    long t = VarInt.getVarLong(buffer) + lastT;
                    lastT = t;
                    int aIndex1 = (int) (index1.key & 0xFF);
                    long a = VarInt.getVarLong(buffer) + aNodeArray[aIndex1];
                    if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                        sum += a;
                        ++cnt;
                    }
                }
            }
        }
        if(aMinIndex < aMaxIndex && rightKeyStart <= rightKeyEnd) {
            List<Index.ATIndex> indices = readATFile(rightKeyStart, rightKeyEnd, aMaxIndex, buffer);

            buffer.flip();
            for(Index.ATIndex index1 : indices) {
                long tBase = (index1.key >> 8) * T_INTERVAL;
                long lastT = tBase;
                for (int i = 0; i < index1.cnt; i++) {
                    long t = VarInt.getVarLong(buffer) + lastT;
                    lastT = t;
                    int aIndex1 = (int) (index1.key & 0xFFL);
                    long a = VarInt.getVarLong(buffer) + aNodeArray[aIndex1];
                    if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                        sum += a;
                        ++cnt;
                    }
                }
            }
        }

        // 读ta文件
        long topKeyStart = (tMinIndex << 8) + aMinIndex + 1;
        long topKeyEnd = (tMinIndex << 8) + aMaxIndex - 1;
        long bottomKeyStart = (tMaxIndex << 8) + aMinIndex + 1;
        long bottomKeyEnd = (tMaxIndex << 8) + aMaxIndex - 1;

        if(topKeyStart <= topKeyEnd) {
            List<Index.TAIndex> indices = readTAFile(topKeyStart, topKeyEnd, buffer);
            buffer.flip();
            for(Index.TAIndex index1 : indices) {
                long tBase = (index1.key >> 8) * T_INTERVAL;
                long lastT = tBase;
                int lastPost = buffer.position();
                int dataSize = (int) (index1.itemOffset & 0xFFFFL);
                while(lastPost + dataSize > buffer.position()) {
                    long t = VarInt.getVarLong(buffer) + lastT;
                    lastT = t;
                    int aIndex1 = (int) (index1.key & 0xFF);
                    long a = VarInt.getVarLong(buffer) + aNodeArray[aIndex1];
                    if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                        sum += a;
                        ++cnt;
                    }
                }
            }
        }

        if(tMinIndex < tMaxIndex && bottomKeyStart <= bottomKeyEnd) {
            List<Index.TAIndex> indices = readTAFile(bottomKeyStart, bottomKeyEnd, buffer);
            buffer.flip();
            for(Index.TAIndex index1 : indices) {
                long tBase = (index1.key >> 8) * T_INTERVAL;
                long lastT = tBase;
                int lastPost = buffer.position();
                int dataSize = (int) (index1.itemOffset & 0xFFFFL);
                while(lastPost + dataSize > buffer.position()) {
                    long t = VarInt.getVarLong(buffer) + lastT;
                    lastT = t;
                    int aIndex1 = (int) (index1.key & 0xFF);
                    long a = VarInt.getVarLong(buffer) + aNodeArray[aIndex1];
                    if (aMin <= a && a <= aMax && tMin <= t && t <= tMax) {
                        sum += a;
                        ++cnt;
                    }
                }
            }
        }
//        System.out.printf("sum %d cnt %d\n", sum, cnt);
        return cnt == 0 ? 0 : sum / cnt;
    }

    public static List<Index.ATIndex> readATFile(long startKey, long endKey, long aIndex, ByteBuffer buf) {
        buf.clear();
        List<Index.ATIndex> indices = new ArrayList<>();
        long readStartOffset = -1;
        long readCnt = 0;
        for (long i = startKey; i <= endKey; i+=(1<<8)) {
            Index.ATIndex index1 = getATIndex(i, aIndex);
            if(index1 != null) {
                if(readStartOffset == -1) {
                    readStartOffset = index1.itemOffset >>> 16;
                }
                readCnt += index1.itemOffset & 0xFFFFL;
                indices.add(index1);
            }
        }
        if(readCnt == 0) {
            return indices;
        }

        try {
            buf.limit((int) readCnt);
            if(aIndex == 2 || aIndex == 3 || aIndex == 16 || aIndex == 17) {
                Cache.read(buf, (int)readStartOffset, (int) readCnt, (int) aIndex);
            } else {
                int read = WriteContext.INSTANCE.atChannel[(int) aIndex].read(buf, readStartOffset);
                PerfCounter.GET_AVG_IO_COUNTER.getAndAdd(1);
                if (read != readCnt) {
                    System.out.println("read error");
                }
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return indices;
    }

    public static List<Index.TAIndex> readTAFile(long startKey, long endKey, ByteBuffer buf) {
        buf.clear();
        List<Index.TAIndex> indices = new ArrayList<>();
        long readStartOffset = -1;
        long readCnt = 0;
        for (long i = startKey; i <= endKey; i+=1) {
            Index.TAIndex index1 = getTAIndex(i);
            if(index1 != null) {
                if(readStartOffset == -1) {
                    readStartOffset = index1.itemOffset >> 16;
                }
                readCnt += index1.itemOffset & 0xFFFFL;
                indices.add(index1);
            }
        }
        if(readCnt == 0) {
            return indices;
        }

        try {
            buf.limit((int) readCnt);
            int read = WriteContext.INSTANCE.taChannel.read(buf, readStartOffset);
            PerfCounter.GET_AVG_IO_COUNTER.getAndAdd(1);
            if(read != readCnt) {
                System.out.println("read error");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return indices;
    }

    public static Index.ATIndex getATIndex(long key, long aIndex) {
        Index.ATIndex found = new Index.ATIndex();
        found.key = key;
        int index = Arrays.binarySearch(atIndexArray.get((int) aIndex), 0, atIndexCounters[(int) aIndex], found);
        if(index >= 0) {
            return atIndexArray.get((int) aIndex)[index];
        } else {
//            System.out.println("error");
            return null;
        }
    }

    public static Index.TAIndex getTAIndex(long key) {
        Index.TAIndex found = new Index.TAIndex();
        found.key = key;
        int index = Arrays.binarySearch(taIndexArray, 0, taIndexCounter, found);
        if(index >= 0) {
            return taIndexArray[index];
        } else {
//            System.out.println("error");
            return null;
        }
    }

    public static class MessageComparator implements Comparator<Message> {

        @Override
        public int compare(Message o1, Message o2) {
            return (int) (o1.getT() - o2.getT());
        }
    }
}
