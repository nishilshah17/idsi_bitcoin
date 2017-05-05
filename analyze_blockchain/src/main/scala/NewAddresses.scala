/**
  * Created by NishilShah on 4/30/17.
  */
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

object NewAddresses {

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val newFormat = new SimpleDateFormat("MM yyyy")

    val outputAddressIndex = AnalyzeBlockchain.outputAddressIndex()

    //input path
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output path
    val outputFilePath = "hdfs://" + outputPath + "/new_addresses"

    //import data
    val transactionsRDD = sc.textFile(transactionsPath)
    //split data by commas
    val transactions = transactionsRDD.map(line => line.split(","))
    val outputAddresses = transactions.flatMap(arr => arr(outputAddressIndex).split(":")
      .map(addr => (addr, dateFormat.parse(arr(2)))))
    val validOutputAddresses = outputAddresses.filter(tx => tx._1 != "null")

    //get month each address was first used
    val addressFirstSeen = validOutputAddresses.reduceByKey((a, b) => if(a.before(b)) a else b)
      .map(entry => (entry._1, newFormat.format(entry._2)))
    //get number of new addresses per month
    val newAddressesPerMonth = addressFirstSeen.map(entry => (entry._2, 1)).reduceByKey(_ + _)

    //save output
    newAddressesPerMonth.repartition(1).saveAsTextFile(outputFilePath)
  }
}
