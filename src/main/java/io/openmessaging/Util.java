package io.openmessaging;

/**
 * Created by xuzhe on 2019/9/2.
 */
public class Util {
    public static void main(String[] args) {
        long[] arr = new long[50];
        for (int i = 0; i < 50; i++) {
            arr[i] = i*2;
        }
        for (int i = 1; i < 50; i += 3) {
            int index = lower_bound(arr, i);
            int index2 = floorSearch(arr, 0, arr.length, i);
            System.out.printf("%d value=%d %d\n", index, i, arr[index]);
            System.out.printf("%d value=%d %d\n", index, i, arr[index2]);
        }
    }
    public static int lower_bound(long[] list, long value) {
        if (list.length == 0) return -1;
        int head = 0;
        while (list[head] <= value) {
            if (++head == list.length) {
                return head - 1;
            }
        }
        return head - 1;
    }

    static int  floorSearch(long a[], int low, int high, int x)
    {
        if(low > high){
            return low;
        }
        int mid = (low+high)/2;
        if(a[mid]>x){
            return floorSearch(a, low, mid-1, x);
        }
        else if(a[mid]<x){
            return floorSearch(a, mid+1, high, x);
        }
        else{
            return mid;
        }
    }
}
