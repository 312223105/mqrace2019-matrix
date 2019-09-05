package io.openmessaging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static io.openmessaging.Constant.MSGPOOL_SIZE;

/**
 * Created by xuzhe on 2019/9/2.
 */
public class MessageListPool {
    private static BlockingQueue<MessageList> messageListPool =
            new ArrayBlockingQueue<>(MSGPOOL_SIZE);

    public static void init() {
        for (int i = 0; i < MSGPOOL_SIZE; i++) {
            messageListPool.add(new MessageList());
        }
    }

    public static MessageList takeMessageList() {
        try {
//            System.out.println("take, size " + messageListPool.size());
            return messageListPool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public static void putMessageList(MessageList messageList) {
        try {
            messageList.clear();
            messageListPool.put(messageList);
//            System.out.println("put, size " + messageListPool.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
