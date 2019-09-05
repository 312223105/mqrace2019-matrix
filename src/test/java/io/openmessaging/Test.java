package io.openmessaging;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class Test {
    public static void main(String[] args) throws IOException {
        NavigableSet<Long> set = new TreeSet<>();
        long[] nums = new long[]{
                3682313404613L,
                18310395058882L,
                32983058831326L,
                47735627847657L,
                62546473551635L,
                77396425696249L,
                91950975784405L,
                106773655558066L,
                121375068225907L,
                136301862337143L,
                151304184676553L,
                166205686888323L,
                181136764908252L,
                196133855579541L,
                210903824077692L,
                225554434511296L,
                240629273978283L,
                255533021268333L,
                270486412591867L};
        for (int i = 0; i < nums.length; i++) {
            set.add(nums[i]);
        }

        long[] values = new long[10000000];
        Random r = new Random();
        for (int i = 0; i < values.length; i++) {
            values[i] = r.nextInt() * 100000L;
        }
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 20_0000_0000; i++) {
            int offset = i % values.length;
            long val = values[offset];
            Util.lower_bound(nums, val);
        }
        long t2 = System.currentTimeMillis();
        System.out.println(t2-t1);
//        for (int i = 0; i < 20; i++) {
//            long size = WriteContext.INSTANCE.atChannel[i].size();
//            long itemCnt = size / 16;
//            ByteBuffer at = ByteBuffer.allocateDirect((int) (itemCnt * 16));
//            ByteBuffer body = ByteBuffer.allocateDirect((int) (itemCnt * 34));
//
//            WriteContext.INSTANCE.atChannel[i].read(at);
//            WriteContext.INSTANCE.bodyChannel[i].read(body);
//            at.flip();
//            body.flip();
//            for (int j = 0; j < itemCnt; j++) {
//                long t = at.getLong();
//                long a = at.getLong();
//                byte[] b = new byte[34];
//                body.get(b);
//                Message m = new Message(a, t, b);
//                String rst = MQRaceTesterRandomFast.checkMessageBody(m);
//                if(rst.length() > 5) {
//                    System.out.println(rst);
//                }
//            }
//        }
    }
}
