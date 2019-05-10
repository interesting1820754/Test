package com.org.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object a  {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
    val sc=new SparkContext(sparkConf)
    val tableName="student"

    val conf=HBaseConfiguration.create()

    val job=new JobConf(conf)
    job.setOutputFormat(classOf[TableOutputFormat])

    job.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val dataRDD=sc.makeRDD(Array("15,hadoop,B,59","11,spark,G,56"))
    val rdd=dataRDD.map(_.split(",")).map{arr=>{
      val put=new Put(Bytes.toBytes(arr(0))) //行健的值   Put.add方法接收三个参数：列族,列名,数据
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1))) //info:name列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2))) //info:gender列的值
      put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt))//info:age列的值
      (new ImmutableBytesWritable,put)  ////转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
    }}
    rdd.saveAsHadoopDataset(job)
    sc.stop()
  }
}