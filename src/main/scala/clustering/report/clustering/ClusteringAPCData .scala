package clustering.report.clustering

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

abstract class ClusteringAPCData  {
  
   protected def  obtainClusters() = {
     
        // Set application name
        val appName:String  = "ClusteringExample";
        
        // Initialize Spark configuration & context
        val sparkConf:SparkConf = new SparkConf().setAppName(appName)
                .setMaster("local[1]").set("spark.executor.memory", "1g");
        
        val sc:SparkContext = new SparkContext(sparkConf);
        
//        https://dzone.com/articles/cluster-analysis-using-apache-spark-exploring-colo
        
    
  }
  
}