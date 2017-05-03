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

    //input paths
    val blocksPath = "hdfs://" + inputPath + "/blocks*"
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output path
    val outputFilePath = "hdfs://" + outputPath + "/new_addresses"

    //import data
    val blocksRDD = sc.textFile(blocksPath)
    val transactionsRDD = sc.textFile(transactionsPath)
    //split data by commas
    val blocks = blocksRDD.map(line => line.split(",")).map(arr => (arr(0), dateFormat.parse(arr(3))))
    val transactions = transactionsRDD.map(line => line.split(","))
    val outputAddresses = transactions.flatMap(arr => arr(outputAddressIndex).split(":").map(addr => (arr(0), addr)))
      .filter(tx => tx._2 != "null")
    //join blocks and output addresses
    val combined = blocks.join(outputAddresses).map(entry => (entry._2._2, entry._2._1))
    //get month each address was first used
    val addressFirstSeen = combined.reduceByKey((a, b) => if(a.before(b)) a else b)
      .map(entry => (entry._1, newFormat.format(entry._2)))
    //get number of new addresses per month
    addressFirstSeen.map(entry => (entry._2, 1)).reduceByKey(_ + _).repartition(1).saveAsTextFile(outputFilePath)
  }
}
