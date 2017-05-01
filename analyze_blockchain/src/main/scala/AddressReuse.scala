/**
  * Created by NishilShah on 4/30/17.
  */
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.io._

object AddressReuse {

  def averageReuse(ts: Iterable[Long]): Double = {
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
    return totalTime / reuseCount
  }

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")

    //input
    val blockFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_data/*blocks*"
    val transactionFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_data/*transactions*"
    //output
    val outputFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_output/address_reuse"
    val statsFilePath = outputFilePath + "/stats.txt"

    //import data
    val blockLines = sc.textFile(blockFilePath)
    val transactionLines = sc.textFile(transactionFilePath)
    //split data by commas
    val blocks = blockLines.map(line => line.split(",")).map(arr => (arr(0), dateFormat.parse(arr(3)).getTime))
    val transactions = transactionLines.map(line => line.split(","))
    //extract addresses from transactions
    val inputAddresses = transactions.flatMap(arr => arr(6).split(":").map(addr => (arr(0), addr)))
      .filter(tx => tx._2 != "null")
    val outputAddresses = transactions.flatMap(arr => arr(7).split(":").map(addr => (arr(0), addr)))
      .filter(tx => tx._2 != "null")
    val addresses = inputAddresses.union(outputAddresses)
    val combined = blocks.join(addresses)

    //key: address, value: num times used
    val addressUse = combined.map(entry => (entry._2._2, 1)).reduceByKey(_ + _)
    //key: count, value: num addresses used count times
    val addressUseTimes = addressUse.map(_.swap).map(entry => (entry._1, 1)).reduceByKey(_ + _)
    val totalAddressUse: Float = addressUseTimes.map(entry => (0, entry._1 * entry._2)).reduceByKey(_ + _).first()._2
    //only counting addresses used more than once
    val singleUseAddressCount = addressUseTimes.filter(entry => entry._1 == 1).first()._2
    val addressReuseTimes = addressUseTimes.filter(entry => entry._1 != 1)
    val totalAddressReuse: Float = addressReuseTimes.map(entry => (0, entry._1 * entry._2)).reduceByKey(_ + _).first()._2

    val datesAddressUsed = combined.map(entry => (entry._2._2, entry._2._1)).groupByKey()
    val totalAvgUseTime = datesAddressUsed.map(entry => (0, averageReuse(entry._2))).reduceByKey(_ + _).first()._2
    val totalAvgReuseTime = datesAddressUsed.filter(entry => entry._2.size > 1)
      .map(entry => (0, averageReuse(entry._2))).reduceByKey(_ + _).first()._2

    //calculations
    val uniqueAddressCount = addressUse.count()
    val averageAddressUse = totalAddressUse / uniqueAddressCount
    val averageAddressReuse = totalAddressReuse / (uniqueAddressCount - singleUseAddressCount)
    val averageUseTime = totalAvgUseTime / uniqueAddressCount
    val averageReuseTime = totalAvgReuseTime / (uniqueAddressCount - singleUseAddressCount)
    var stats = "Unique Addresses: " + uniqueAddressCount + "\n"
    stats += "Single Use Addresses: " + singleUseAddressCount + "\n"
    stats += "Avg Times Address Used: " + averageAddressUse + "\n"
    stats += "Avg Times Address Used (omitting single use addresses): " + averageAddressReuse + "\n"
    stats += "Avg Time Between Address Use: " + averageUseTime.toString + "\n"
    stats += "Avg Time Between Address Use (omitting single use addresses): " + averageReuseTime.toString()

    //save output
    addressUseTimes.repartition(1).saveAsTextFile(outputFilePath + "/address_use_times")
    AnalyzeBlockchain.printToFile(new File(statsFilePath)) { printer => printer.println(stats)}
  }
}
