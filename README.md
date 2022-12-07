//With Column - Scala Sparkpackage pack

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object obj {


def main(args:Array[String]):Unit={


		println("================Started1============")
		val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df = spark
		         .read
		         .format("json")
		         .option("multiline","true")
		         .load("file:///C:/Big Data/data/pets.json")
		df.show()
		df.printSchema()

		val flatdf = df
	                .withColumn( "Pets", explode(col("Pets")))
	                .withColumn( "Permanent address", expr( expr= "Address.'Permanent address'"))
	                .withColumn( "current address", expr( expr= "Address.'Permanent address'"))
	                .drop("Address")
		         
		
		flatdf.show()
		flatdf.printSchema()		          
						}

}
