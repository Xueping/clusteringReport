package clustering.report.clustering

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

abstract class ClusteringAPCData {

  def obtainClusters() = {

    // Set application name
    val appName: String = "ClusteringExample";

    // Initialize Spark configuration & context
    val sparkConf: SparkConf = new SparkConf().setAppName(appName)
      .setMaster("local[1]").set("spark.executor.memory", "1g");

    val sc: SparkContext = new SparkContext(sparkConf);

    val rawData = sc.textFile("D:\\dev\\R_project\\C3\\demo.csv").cache()
    val dataAndLabel = vectorization(rawData)
    
    val data = dataAndLabel.map(_._1)


  }

  // Loads and parses data
  def parse(line: String): Vector = Vectors.dense(line.split(" ").map(_.toDouble))

  def vectorization(normalizedData: RDD[String]) = {

    val ethnicities = normalizedData.map(_.split(",")(2)).distinct.collect.zipWithIndex.toMap
    val payors = normalizedData.map(_.split(",")(3)).distinct.collect.zipWithIndex.toMap
    val religions = normalizedData.map(_.split(",")(4)).distinct.collect.zipWithIndex.toMap
    val admissions = normalizedData.map(_.split(",")(5)).distinct.collect.zipWithIndex.toMap

//    val dataAndLabel = 
      normalizedData.map {
      line =>
        val buffer = ArrayBuffer[String]()

        buffer.appendAll(line.split(","))
        val ethnicity = buffer.remove(2)
        val payor = buffer.remove(3)
        val religion = buffer.remove(4)
        val admission = buffer.remove(5)
        val label = buffer.remove(1)
        val vector = buffer.map(_.toDouble)
        val newEthnicityFeatures = new Array[Double](ethnicities.size)
        newEthnicityFeatures(ethnicities(ethnicity)) = 1.0
        val newPayorFeatures = new Array[Double](payors.size)
        newPayorFeatures(payors(payor)) = 1.0
        val newReligionFeatures = new Array[Double](religions.size)
        newReligionFeatures(religions(religion)) = 1.0
        val newAdmissionFeatures = new Array[Double](admissions.size)
        newReligionFeatures(admissions(admission)) = 1.0
        vector.insertAll(1, newEthnicityFeatures)
        vector.insertAll(1, newPayorFeatures)
        vector.insertAll(1, newReligionFeatures)
        vector.insertAll(1, newAdmissionFeatures)
        (vector.toArray, label)
    }

  }

}