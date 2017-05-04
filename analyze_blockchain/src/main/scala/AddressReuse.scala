/**
  * Created by NishilShah on 4/30/17.
  */
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

object AddressReuse {

  def totalReuse(ts: Iterable[Long]): Double = {
    val reuseCount = ts.size - 1
    if(reuseCount < 1) {
      return 0
    }
    val dates = ts.toList.sorted
    var totalTime = 0.0
    for(i <- 0 until reuseCount) {
      val diff = dates(i+1) - dates(i)
      totalTime += diff
    }
    return totalTime
  }

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val inputAddressIndex = AnalyzeBlockchain.inputAddressIndex()
    val outputAddressIndex = AnalyzeBlockchain.outputAddressIndex()

    //input paths
    val blocksPath = "hdfs://" + inputPath + "/blocks*"
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output path
    val outputFilePath = "hdfs://" + outputPath + "/address_reuse"
    val statsFilePath = outputPath + "/address_reuse/address_reuse_statistics.txt"

    //import data
    val blocksRDD = sc.textFile(blocksPath)
    val transactionsRDD = sc.textFile(transactionsPath)
    //split data by commas
    val blocks = blocksRDD.map(line => line.split(",")).map(arr => (arr(0), dateFormat.parse(arr(3)).getTime))
    val transactions = transactionsRDD.map(line => line.split(","))
    //extract addresses from transactions
    val inputAddresses = transactions.flatMap(arr => arr(inputAddressIndex).split(":").map(addr => (arr(0), addr)))
      .filter(tx => tx._2 != "null")
    val outputAddresses = transactions.flatMap(arr => arr(outputAddressIndex).split(":").map(addr => (arr(0), addr)))
      .filter(tx => tx._2 != "null")
    val addresses = inputAddresses.union(outputAddresses)
    val combined = blocks.join(addresses)

    //number of times each address used
    val addressUseCount = combined.map(entry => (entry._2._2, 1)).reduceByKey(_ + _)
    //key: use count, value: num addresses used count times
    val addressUseTimes = addressUseCount.map(_.swap).map(entry => (entry._1, 1)).reduceByKey(_ + _)

    //total time between address reuse
    val datesAddressUsed = combined.map(entry => (entry._2._2, entry._2._1)).groupByKey()
      .filter(entry => entry._2.size > 1)
    val totalTimeBetweenReuse: Double = datesAddressUsed.map(entry => (0, totalReuse(entry._2)))
      .reduceByKey(_ + _).first()._2

    //calculations
    val totalAddressCount: Double = combined.count()
    val uniqueAddressCount = addressUseCount.count()
    val singleUseAddressCount = addressUseCount.filter(entry => entry._2 == 1).count()
    val multiUseAddressCount = uniqueAddressCount - singleUseAddressCount
    val reuseCount = totalAddressCount - uniqueAddressCount
    val avgTimesAddressUsed = totalAddressCount / uniqueAddressCount
    val avgTimesAddressUsedNoSingles = (totalAddressCount - singleUseAddressCount) / multiUseAddressCount
    val avgTimeBetweenAddressReuse = totalTimeBetweenReuse / reuseCount / 1000

    //build output
    var stats = "Unique Addresses: " + uniqueAddressCount + "\n"
    stats += "Single Use Addresses: " + singleUseAddressCount + "\n"
    stats += "Avg Times Address Used: " + avgTimesAddressUsed + "\n"
    stats += "Avg Times Address Used (omitting single use addresses): " + avgTimesAddressUsedNoSingles + "\n"
    stats += "Avg Time Between Address Reuse: " + avgTimeBetweenAddressReuse + " s\n"

    //save output
    addressUseTimes.repartition(1).saveAsTextFile(outputFilePath)
    val writer = AnalyzeBlockchain.printWriter(statsFilePath)
    try {
      writer.write(stats)
    } finally {
      writer.close()
    }
  }
}
