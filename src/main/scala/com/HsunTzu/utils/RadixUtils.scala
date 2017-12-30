package com.HsunTzu.utils

import java.math.BigInteger

class RadixUtils {

}
object  RadixUtils{


  /***
    * char 转 数字
    * @param s
    * @return
    */
  def charToInts(s: Char): Option[Int] = {
    try {
      Some(s.toString.toInt)
    } catch {
      case e: Exception =>Some(0)
    }

  }

  /**
    * 二进制 转 大数
    * @param binaryStr
    * @return
    */
  def binaryToBigInteger(binaryStr:String): String={
    val big=new BigInteger(binaryStr,2)toString(10)
    return big
  }

  /**
    * 大数转  二进制
    * @param big
    * @return
    */
  def bigIntegerToBinary(big:String):String={
    val binary=new BigInteger(big,10)toString(2)
    return binary
  }

  def main(args: Array[String]): Unit = {
    val sew="20105208883772533738546659328"
    val binary="111011010100011100101011000000011001000000010000000000000000000000000000000000000000000"
    val ss="10000001111011010100011100101011000000011001000000010000000000000000000000000000000000000000000"
    val bin=binaryToBigInteger(ss)
    println(bin)
    println(bigIntegerToBinary(sew))
  }
  /***
    * 根据bigInteger 反解 得到二进制 ，去掉第一位的符号位
    * @param bigzint
    * @return
    */
  def bigIntegerToBytes(bigzint:String): String = {
    val bigInteger = new BigInteger(bigzint)
    val bytes = bigInteger.toByteArray
    val sb = new StringBuilder(bytes.length * java.lang.Byte.SIZE)
    var i = 0
    val mac:Int =java.lang.Byte.SIZE * bytes.length
    val byteSize=java.lang.Byte.SIZE
    while ( {
      i < mac
    }) sb.append(if ((bytes(i / byteSize) << i % byteSize & 0x80) == 0) '0'
    else '1') {
      i += 1; i - 1
    }
    return sb.toString.substring(1)
  }

}