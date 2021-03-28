package org.apache.flink.streaming.srcutils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;

public class SrcCount {

    private static int arrayInitLength = 0;
    private SrcCount() {
        this.arrayCounter = new ThreadLocal<ArrayCounter>();
        this.lastPrintTime = new  ThreadLocal<AtomicLong>();
    }

    private static SrcCount instance=null;

    public static SrcCount getInstance(){
        if(instance==null){
            synchronized (SrcCount.class){
                if(instance==null) instance = new SrcCount();
            }
        }
        return instance;
    }


    private ThreadLocal<ArrayCounter> arrayCounter;
    private ThreadLocal<AtomicLong> lastPrintTime;

    private static long[] threadTimeCounter= new long[]{arrayInitLength};
    public static void printByFixTime(int waitMillis, int... indexs) {
        long lastPrintTime = getInstance().getPrintTimeCounter().get();
        if(System.currentTimeMillis() - lastPrintTime > waitMillis ){
            SrcCount.getInstance().getArrayCounter().printCounterByIndexs(indexs);
            getInstance().getPrintTimeCounter().set(System.currentTimeMillis());
        }
    }

    private AtomicLong getPrintTimeCounter() {
        AtomicLong last = lastPrintTime.get();
        if(last==null){
            AtomicLong cur = new AtomicLong(System.currentTimeMillis());
            lastPrintTime.set(cur);
            last = cur;
        }
        return last;
    }



    private ArrayCounter getArrayCounter(){
        ArrayCounter keyCounter = this.arrayCounter.get();
        if(keyCounter==null){
            synchronized (this){
                keyCounter = new ArrayCounter(arrayInitLength);
                this.arrayCounter.set(keyCounter);
            }
        }
        return keyCounter;
    }


    public static void addAndGet(int key,final long delta){
        getInstance().getArrayCounter().addAndGet(key,delta);
    }


    public static void initCountArray(int... indexs){
        int maxIndex = Integer.MIN_VALUE;
        for(int index:indexs){
            if(index > maxIndex){
                maxIndex = index;
            }
        }
        arrayInitLength = maxIndex + 1;
        getInstance().resetCountArray();
    }

    private void resetCountArray() {
        ArrayCounter newArrayCounter = new ArrayCounter(arrayInitLength);
        this.arrayCounter.set(newArrayCounter);
    }

    public class ArrayCounter{
        private final long ONE = 0L;
        private int size;
        private AtomicLong[] keyCounter;

        private ArrayCounter(int initSize) {
            this.size = initSize;
            this.keyCounter = new AtomicLong[initSize];
            for (int i = 0; i < initSize; i++) {
                keyCounter[i] = new AtomicLong(ONE);
            }
        }


        public long addAndGet(int key,final long delta){
            return this.keyCounter[key].addAndGet(delta);
        }

        private void ensureArraySize(int key){
            if(key>= this.size){
                synchronized (this){
                    int newSize= (int)(this.size * 1.75);
                    AtomicLong[] newCounter = new AtomicLong[newSize];
                    System.arraycopy(this.keyCounter,0,newCounter,0,this.size);
                    this.keyCounter = newCounter;
                    this.size= newSize;
                }
            }
        }

        public long initCounterForKey(int key){
            ensureArraySize(key);
            this.keyCounter[key] = new AtomicLong(ONE);
            return this.keyCounter[key].get();
        }

        @Override
        public String toString() {
            return "ArrayCounter{" +
                    "size=" + size +
                    ", keyCounter=" + Arrays.toString(keyCounter) +
                    '}';
        }


        public void destroy() {
            if(this.keyCounter!=null){
                keyCounter = null;
            }

        }

        public void printCounterByIndexs(int[] indexs) {
            StringJoiner joiner = new StringJoiner(", ");
            for(int index:indexs){
                AtomicLong counter = keyCounter[index];
                joiner.add("index_"+index+":"+counter.get());
            }
            System.out.println(joiner.toString());
        }

    }

    public static void destroy() {
        SrcCount instance = getInstance();
        instance.arrayCounter.get().destroy();
        if(instance.arrayCounter!=null){
            instance.arrayCounter.remove();
            instance.arrayCounter = null;
        }
    }

    public static double divide(double num,double b,int newScale){
        BigDecimal result = BigDecimal.valueOf(num).divide(BigDecimal.valueOf(b),50, RoundingMode.HALF_UP);
        int scale = result.scale();
        double value= result.doubleValue();
        if(scale > newScale){
            try{
                value=result.setScale(newScale,RoundingMode.HALF_UP).doubleValue();
            }catch (Exception e){}
        }
        return value;
    }




    public static class Key {

        public  static final int Index_0 = 0;
        public  static final int Index_1 = 1;
        public  static final int Index_2 = 2;
        public  static final int Index_3 = 3;
        public  static final int Index_4 = 4;
        public  static final int Index_5= 5;
        public  static final int Index_6 = 6;


    }



}
