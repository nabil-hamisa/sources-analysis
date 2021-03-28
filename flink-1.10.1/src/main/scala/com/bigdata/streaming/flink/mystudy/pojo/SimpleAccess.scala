package com.bigdata.streaming.flink.mystudy.pojo

case class SimpleAccess(timeStr:String,
                        city:String,
                        telco:String,
                        os:String,
                        triffic:Long
                       )

case class SimpleAccess2(timeStr:String,
                        city:String,
                        telco:String,
                        os:String,
                        cnt:Int,
                        triffic:Long
                       )