package com.monkeyboy.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.monkeyboy.scala.spark.util.MultiOutputRDD._
import com.monkeyboy.java.hadoop.util.HadoopUtil
import org.apache.hadoop.fs.Path
object MultioutputDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("MultioutputDemo-job")
    val sc = new SparkContext(sparkConf)

    val inputPath=args(0)
    val rootPath = args(1)

    val rdd = sc.textFile(inputPath).map { ("/aa/", _) }

    val hadoopConf = sc.hadoopConfiguration
    if (HadoopUtil.exist(new Path(rootPath), hadoopConf)) {
      System.out.println("the output dir is exists! deleted it!");
      HadoopUtil.delete(new Path(rootPath), hadoopConf);
    }

    rdd.saveAsMultiTextFiles(rootPath)

  }

}