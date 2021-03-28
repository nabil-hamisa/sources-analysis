package com.bigdata.streaming.flink.mystudy.pojo

import java.text.SimpleDateFormat
import java.util.Date

case class KeyValueTime(ts:Long,key:String,value:Double) {

  def getTime():Long = ts

  def asTimeStrKV():(String,Double,String)={
    val timeStr: String = KeyValueTime.formatTime(ts)
    (key,value,timeStr)
  }

}

object KeyValueTime{

  val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def formatTime(ts:Long):String =  format2.format(new Date(ts))

  def parseStrAsTimeMillis(str:String):Long = {
    try{
      format1.parse(str).getTime
    }catch{
      case ex: Exception =>
        format2.parse(str).getTime
    }
  }


  def apply(inputStr: String, sep:String=","): KeyValueTime ={
    val splits: Array[String] = inputStr.split(sep)
    val ts: Long = parseStrAsTimeMillis(splits(0))
    new KeyValueTime(ts,  splits(1).trim, splits(2).trim.toDouble)
  }

}