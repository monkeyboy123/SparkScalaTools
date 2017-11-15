package com.monkeyboy.demo

import com.monkeyboy.java.hadoop.util.HadoopUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import com.monkeyboy.java.hadoop.util.EmptiableTextInputFormat
import org.apache.hadoop.io.Text

object MultiInputDemo {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("MultiInputDemo-job")
    val sc = new SparkContext(sparkConf)

    val inputDirs=args(0)
    val outPath = args(1)

    val rdd = sc.newAPIHadoopFile(inputDirs, classOf[EmptiableTextInputFormat], classOf[LongWritable], classOf[Text], sc.hadoopConfiguration)

    val hadoopConf = sc.hadoopConfiguration
    if (HadoopUtil.exist(new Path(outPath), hadoopConf)) {
      System.out.println("the output dir is exists! deleted it!");
      HadoopUtil.delete(new Path(outPath), hadoopConf);
    }

    rdd.saveAsTextFile(outPath)

  }
}