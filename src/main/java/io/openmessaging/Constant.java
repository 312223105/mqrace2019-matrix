package io.openmessaging;

/**
 * Created by xuzhe on 2019/8/21.
 */
public class Constant {
    public static final int LOCAL_LIST_SIZE = 4096;
    public static final int BQ_SIZE = 1;
    public static final int MSGPOOL_SIZE = 20*4;
    public static final int BUCKET_MSG_COUNT = 4096 * 4;
    public static final int T_INTERVAL = 2048;
    public static final int SAMPLE_COUNT = 20_0000;

    public static final int A_BUCKET_CNT = 20;

    public static final int UNIT_SIZE = 10; // t 2B a 8B

    public static final String dataDir;

    public static final boolean PRINT_WRITE_RATE = true;

    static {
        if(System.getProperty("dev") != null) {
            dataDir = "./data/";
        } else {
            dataDir = "/alidata1/race2019/data/";
        }
    }
}
