package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest;

public class MultiArgSumAcc {

    public long count;

    public MultiArgSumAcc(){
        System.out.println("创建 MulitSum 累加器");
    }
    public MultiArgSumAcc(long count) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public long add(long delta){
        count += delta;
        return count;
    }

}
