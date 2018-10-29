

package com.bdt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.tools.nsc.interpreter.Logger


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
    
   
    
    val spark=SparkSession.
              builder()
              .appName("SparkDemo1")
              .config(conf)
              .getOrCreate()
              
     val sc=spark.sparkContext   
     
     import spark.implicits._
     
     //cleansing or  working with RDD
     //in spark session we can only create DF or DS 
     
     val inputRDD=sc.textFile("D:/spark.txt");
     
     
     inputRDD.foreach(println)
     
   inputRDD.filter(line=> !line.contains("id,name,sal")).map(line=>(line.split(",")(0),
     line.split(",")(1),line.split(",")(2),line.split(",")(3))).foreach(println)
     
     
     val empDF= inputRDD.filter(line=> !line.contains("id,name,sal")).map(line=>(line.split(",")(0),
     line.split(",")(1),line.split(",")(2),line.split(",")(3))).toDF("id","name","sal","dept")
     
     
     empDF.printSchema()
     empDF.show()
     
     
     
     
     empDF.createOrReplaceTempView("emp_table")
     val empDF1=spark.sql("select id,name,sal+100 as sal1,dept from emp_table")
     empDF1.show()
     
     
     
     
     
  }
  
  
  
}