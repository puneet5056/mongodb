

package com.bdt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.tools.nsc.interpreter.Logger
import 	com.mongodb.spark.MongoSpark

object SparkDemo {
  
  //for developing spark application create scala project
  //Add spark dependent jar --> project->buildpath->configure build path ->add external jars
  
  
//Add all jars in spar/jars folder
  //Or -use SBT/MVN
  //For Sparj applications:
  //Testing -we create sparksession with sparkconfiguration hard coded
  //Running in production->we pass empty spark confguration and provide all configuration
  //inn comand line as arguments using --cong or --properties file
  
  
  
  def main(args: Array[String]): Unit = {
  
    
    
    //create configuration and create spark session object
    val conf=new SparkConf().setMaster("local[*]").setAppName("SparkDemo1").set("key1","value")
    //local[*] means take all the local cpu core available in machine
    /* But while running in production 
     val conf=new SparkConf()
     we are goin supply through commanfd line
     
     
     configuration object we set it from command line
     */
    
   
  /*  
    val spark=SparkSession.
              builder()
              .appName("SparkDemo1")
              .config(conf)
              .getOrCreate()
              */
      
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config(conf)
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()
              
     //val sc=spark.sparkContext 
     
     
   val df = MongoSpark.load(spark)
   
   
   df.show()
/*logger.info(df.show())
logger.info("Reading documents from Mongo : OK")*/
     
     
    
     
  }
  
  
  
}