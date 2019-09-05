package io.openmessaging;

import java.util.Comparator;

/**
 * Created by xuzhe on 2019/9/2.
 */
public class ComparatorByA implements Comparator<Message> {
    @Override
    public int compare(Message o1, Message o2) {
        long sub = o1.getA() - o2.getA();
        if(sub == 0) {
            return 0;
        } else if(sub < 0) {
            return -1;
        } else {
            return 1;
        }
    }
}
