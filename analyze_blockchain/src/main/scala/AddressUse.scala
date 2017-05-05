/**
  * Created by NishilShah on 4/30/17.
  */
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

object AddressUse {

  def totalReuseTime(ts: Iterable[(Long, Int)]): Double = {
    val reuseCount = ts.size - 1
    //sort by date
    val values = ts.toList.sortBy(entry => entry._1)
    values(reuseCount)._1 - values.head._1
  }

  def timeBeforeSpend(ts: Iterable[(Long, Int)]): Double = {
    var firstReceived, firstSpent: Long = -1
    //sort by date
    val values = ts.toList.sortBy(entry => entry._1)

    for(i <- 0 until ts.size) {
      if(firstReceived < 0 && values(i)._2 == 1) {
        //address first received BTC at this time
        firstReceived = values(i)._1
      }
      if(firstReceived > 0 && values(i)._2 == 0) {
        //address first spent BTC at this time
        firstSpent = values(i)._1
        //return elapsed time
        return firstSpent - firstReceived
      }
    }
    -1 //don't count this address
  }

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

    val inputAddressIndex = AnalyzeBlockchain.inputAddressIndex()
    val outputAddressIndex = AnalyzeBlockchain.outputAddressIndex()
    val input = 0
    val output = 1

    //input paths
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output path
    val outputFilePath = "hdfs://" + outputPath + "/address_use"
    val statsFilePath = outputPath + "/address_use/address_use_statistics.txt"

    //import data
    val transactionsRDD = sc.textFile(transactionsPath)
    //split data by commas
    val transactions = transactionsRDD.map(line => line.split(","))
    //extract addresses from transactions
    val inputAddresses = transactions.flatMap(arr => arr(inputAddressIndex).split(":")
      .map(addr => (addr, (dateFormat.parse(arr(2)).getTime, input)))).filter(tx => tx._1 != "null")
    val outputAddresses = transactions.flatMap(arr => arr(outputAddressIndex).split(":")
      .map(addr => (addr, (dateFormat.parse(arr(2)).getTime, output)))).filter(tx => tx._1 != "null")
    val addresses = inputAddresses.union(outputAddresses)

    //number of times each address used
    val addressUseCount = addresses.map(entry => (entry._1, 1)).reduceByKey(_ + _)
    //key: use count, value: num addresses used count times
    val addressUseTimes = addressUseCount.map(_.swap).map(entry => (entry._1, 1)).reduceByKey(_ + _)

    //total time between address reuse
    val datesAddressUsed = addresses.groupByKey()
    val datesAddressUsedNoSingles = datesAddressUsed.filter(entry => entry._2.size > 1)
    val addressTimeBetweenReuse = datesAddressUsedNoSingles.map(entry => (0, totalReuseTime(entry._2)))
    val totalTimeBetweenReuse = addressTimeBetweenReuse.reduceByKey(_ + _).first()._2

    //total time before address spent
    val addressTimeBeforeSpend = datesAddressUsedNoSingles.map(entry => (0, timeBeforeSpend(entry._2)))
    val spentAddresses = addressTimeBeforeSpend.filter(entry => entry._2 >= 0)
    val totalTimeBeforeSpend = spentAddresses.reduceByKey(_ + _).first()._2

    //calculations
    val totalAddressCount: Double = addresses.count()
    val uniqueAddressCount = addressUseCount.count()
    val singleUseAddressCount = addressUseCount.filter(entry => entry._2 == 1).count()
    val multiUseAddressCount = uniqueAddressCount - singleUseAddressCount
    val reuseCount = totalAddressCount - uniqueAddressCount
    val avgTimesAddressUsed = totalAddressCount / uniqueAddressCount
    val avgTimesAddressUsedNoSingles = (totalAddressCount - singleUseAddressCount) / multiUseAddressCount
    val avgTimeBetweenAddressReuse = totalTimeBetweenReuse / reuseCount / 1000
    val avgTimeBeforeFirstSpend = totalTimeBeforeSpend / spentAddresses.count() / 1000

    //build output
    var stats = "Unique Addresses: " + uniqueAddressCount + "\n"
    stats += "Single Use Addresses: " + singleUseAddressCount + "\n"
    stats += "Avg Times Address Used: " + avgTimesAddressUsed + "\n"
    stats += "Avg Times Address Used (omitting single use addresses): " + avgTimesAddressUsedNoSingles + "\n"
    stats += "Avg Time Between Address Reuse: " + avgTimeBetweenAddressReuse + "s\n"
    stats += "Avg Time Before First Spend: " + avgTimeBeforeFirstSpend + "s\n"

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
