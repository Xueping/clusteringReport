package clustering.report.clustering

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class DataProcessor {

  def obtainPreprocessData() = {

    // Set application name
    val appName: String = "ClusteringExample";

    // Initialise Spark configuration & context
    val sparkConf: SparkConf = new SparkConf().setAppName(appName)
      .setMaster("local[10]")
//      .set("spark.executor.memory", "2gb")
      .set("spark.driver.memory","60gb")
      .set("spark.rdd.compress","true")
//          .set("spark.memory.useLegacyMode","true")
      .set("spark.storage.memoryFraction", "0.9")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","2000mb")
      .set("spark.kryoserializer.buffer","64mb")
      .set("spark.default.parallelism","32")
      .set("spark.eventLog.enabled","true")

    val sc: SparkContext = new SparkContext(sparkConf);

    val rawData = sc.textFile("./resource/source/demo.csv").cache()
    val header = rawData.first()
    val rawData_withoutHeader = rawData.filter { row => (row != header)&&(!row.contains(",,")) }
    
    vectorization(rawData_withoutHeader)
//    val dataAndLabel = vectorization(rawData_withoutHeader)
//    val data = dataAndLabel.map(_._1)
//    dataAndLabel
  }


  def vectorization(normalizedData: RDD[String]) = {

    val ethnicities = normalizedData.map(_.split(",")(1)).distinct.collect.zipWithIndex.toMap
    val payors      = normalizedData.map(_.split(",")(2)).distinct.collect.zipWithIndex.toMap
    val admissions  = normalizedData.map(_.split(",")(3)).distinct.collect.zipWithIndex.toMap

//    val dataAndLabel = 
      normalizedData.map {
      line =>{
        val buffer = ArrayBuffer[String]()
        val labelBuffer = StringBuilder.newBuilder

        buffer.appendAll(line.split(","))
        
        val label     = buffer.remove(0)
        val ethnicity = buffer.remove(0)
        val payor     = buffer.remove(0)
        val admission = buffer.remove(0)
        
        val vector    = buffer.map(_.toDouble)
        
        val newEthnicityFeatures = new Array[Double](ethnicities.size)
        newEthnicityFeatures(ethnicities(ethnicity)) = 1.0
        
        val newPayorFeatures = new Array[Double](payors.size)
        newPayorFeatures(payors(payor)) = 1.0
        
        val newAdmissionFeatures = new Array[Double](admissions.size)
        newAdmissionFeatures(admissions(admission)) = 1.0
        
        vector.insertAll(0, newAdmissionFeatures)
        vector.insertAll(0, newPayorFeatures)
        vector.insertAll(0, newEthnicityFeatures)
        
        labelBuffer.append(label).append(",")
                   .append(ethnicity).append(",")
                   .append(payor).append(",")
                   .append(admission)
        
        (Vectors.dense(vector.toArray), labelBuffer.toString())
      }
    }
  }
}