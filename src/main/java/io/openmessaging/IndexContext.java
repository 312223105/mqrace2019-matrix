package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.openmessaging.Constant.A_BUCKET_CNT;
import static io.openmessaging.Constant.T_INTERVAL;
import static io.openmessaging.IndexBuilder.aNodeArray;

/**
 * Created by xuzhe on 2019/9/3.
 */
public class IndexContext implements Callable {
    public CompletableFuture<long[]> writeFuture = new CompletableFuture<>();
    public CompletableFuture<long[]> dependedFuture;
    public MessageList sortedList;
    public final long countOfPrevT;
    private static ThreadLocal<MessageList[]> bucketListArray = ThreadLocal.withInitial(() -> {
        MessageList[] array = new MessageList[20];
        for (int i = 0; i < array.length; i++) {
            array[i] = MessageListPool.takeMessageList();
        }
        return array;
    });

    public IndexContext(MessageList sortedList, CompletableFuture<long[]> dependedFuture, long countOfPrevT) {
        this.sortedList = sortedList;
        this.dependedFuture = dependedFuture;
        this.countOfPrevT = countOfPrevT;
    }

    @Override
    public Object call() throws Exception {
        try {
            splitAndWrite(sortedList);
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
        return null;
    }

    private void splitAndWrite(MessageList oneLine) throws ExecutionException, InterruptedException, IOException {
        byte[] tmpBody = new byte[34];
        WriteContext writeContext = WriteContext.INSTANCE;
        ByteBuffer[] atBuffers = WriteContext.atBuffer.get();
        ByteBuffer[] bodyBuffer = WriteContext.bodyBuffer.get();
        long tIndex = oneLine.getT(0) / T_INTERVAL;
        long tBase = tIndex * T_INTERVAL;

        long[] lastT = new long[A_BUCKET_CNT];
        for (int i = 0; i < A_BUCKET_CNT; i++) {
            lastT[i] = tBase;
        }
        Index.TAIndex[] taIndices = new Index.TAIndex[20];
        Index.ATIndex[] atIndices = new Index.ATIndex[20];
        for (int i = 0; i < taIndices.length; i++) {
            long key = (tIndex << 8) + i;
            taIndices[i] = new Index.TAIndex();
            taIndices[i].key = key;
            atIndices[i] = new Index.ATIndex();
            atIndices[i].key = key;
        }

//        taBuffers.clear();
        for (int i = 0; i < atBuffers.length; i++) {
            atBuffers[i].clear();
            bodyBuffer[i].clear();
        }

        for (int i = 0; i < oneLine.size; i++) {
            int aIndex = Util.lower_bound(aNodeArray, oneLine.getA(i));

            long t = oneLine.getT(i);
            long a = oneLine.getA(i);
            long subT = t - lastT[aIndex];
            lastT[aIndex] = t;
            long subA = a - aNodeArray[aIndex];
            VarInt.putVarLong(subT, atBuffers[aIndex]);
            VarInt.putVarLong(subA, atBuffers[aIndex]);

            atIndices[aIndex].sum += oneLine.getA(i);
            ++atIndices[aIndex].cnt;

            oneLine.getBody(i, tmpBody);
            bodyBuffer[aIndex].put(tmpBody);
        }
        long[] offsets = new long[41];
        long[] dependOffsets = null;
        long atTotalSize = 0;
        if(dependedFuture != null) {
            dependOffsets = dependedFuture.get();
            for (int i = 0; i < 20; i++) {
                // 1. 先是at文件的偏移
                offsets[2*i] = dependOffsets[2*i] + atBuffers[i].position();
                // 2. 然后是body文件的偏移/ 34
                offsets[2*i+1] = dependOffsets[2*i+1] + bodyBuffer[i].position() / 34;
                atTotalSize += atBuffers[i].position();
            }
            // 3. 最后是ta文件的偏移
            offsets[40] = dependOffsets[40] + atTotalSize;
        } else {
            for (int i = 0; i < 20; i++) {
                offsets[2*i] = atBuffers[i].position();
                offsets[2*i+1] = bodyBuffer[i].position() / 34;
                atTotalSize += atBuffers[i].position();
            }
            offsets[40] = atTotalSize;
        }
        writeFuture.complete(offsets);

        long taWriteOffset = offsets[40] - atTotalSize;
        for (int i = 0; i < 20; i++) {
            long atWriteOffset = 0;
            long bodyWriteOffset = 0;
            if(dependOffsets != null) {
                atWriteOffset += dependOffsets[i*2];
                bodyWriteOffset += dependOffsets[i*2+1];
            }

            atBuffers[i].flip();
            bodyBuffer[i].flip();
            if(i == 2 || i == 3 || i == 16 || i == 17) {
                Cache.write(atBuffers[i], (int) atWriteOffset, i);
            } else {
                writeContext.atChannel[i].write(atBuffers[i], atWriteOffset);
            }
            atBuffers[i].position(0);
            writeContext.taChannel.write(atBuffers[i], taWriteOffset);
            writeContext.bodyChannel[i].write(bodyBuffer[i], bodyWriteOffset * 34L);

            atIndices[i].itemOffset = (atWriteOffset << 16) + atBuffers[i].position();
            taIndices[i].itemOffset = (taWriteOffset << 16) + atBuffers[i].position();

            atIndices[i].bodyOffset = (int) bodyWriteOffset;

            taWriteOffset += atBuffers[i].limit();

            PerfCounter.PUT_IO_COUNTER.getAndAdd(3);
        }

        synchronized (IndexSearcher.taIndexArray) {
            for (int i = 0; i < 20; i++) {
//                long key = (tIndex << 8) + i;
//                IndexBuilder.atIndexArray[i].put(key, atIndices[i]);
//                IndexBuilder.taIndexArray.put(key, taIndices[i]);
                IndexSearcher.atIndexArray.get(i)
                        [IndexSearcher.atIndexCounters[i]++] = atIndices[i];
                IndexSearcher.taIndexArray[IndexSearcher.taIndexCounter++] = taIndices[i];
            }
        }
        MessageListPool.putMessageList(oneLine);
    }
}
