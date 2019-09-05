package io.openmessaging;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class Index {
    public static class ATIndex implements Comparable<ATIndex> {
        long key;
        public long itemOffset; // 0-47 atDataOffset 48-63 dataSize
        public long sum;
        public short cnt;
        public int bodyOffset; // 需要乘以34

        @Override
        public int compareTo(ATIndex o) {
            long sub = key-o.key;
            if(sub < 0) {
                return -1;
            } else if(sub == 0) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static class TAIndex implements Comparable<TAIndex> {
        public long key;
        public long itemOffset; // 0-47 offset  48-63 datasize

        @Override
        public int compareTo(TAIndex o) {
            long sub = key-o.key;
            if(sub < 0) {
                return -1;
            } else if(sub == 0) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
