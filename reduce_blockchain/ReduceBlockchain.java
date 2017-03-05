import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Utils;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.core.ProtocolException;

import blockparser.BlockFileInputFormat;
import datatypes.BlockWritable;
import datatypes.TransactionWritable;

public class ReduceBlockchain {

  public static class BlockMapper extends Mapper<NullWritable, BytesWritable, BlockWritable, TransactionWritable> {

    private Text word = new Text();

    public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //get block
        Block block = getBlock(value.getBytes());

        BlockWritable bw = new BlockWritable(block.getHash().toString());
        //get transactions
        for(Transaction tx : block.getTransactions()) {
            TransactionWritable tw = new TransactionWritable(tx.getHash().toString());
            context.write(bw, tw);
        }
    }

    public Block getBlock(byte[] bytes) {
        NetworkParameters params = MainNetParams.get();
        org.bitcoinj.core.Context.getOrCreate(params);

        int index = 0;
        int mask = 0xff;
        int nextChar = bytes[index++] & mask;
        while (nextChar != -1) {
            if (nextChar != ((params.getPacketMagic() >>> 24) & mask)) {
                nextChar = bytes[index++] & mask;
                continue;
            }   
            nextChar = bytes[index++] & mask;
            if (nextChar != ((params.getPacketMagic() >>> 16) & mask))
                continue;
            nextChar = bytes[index++] & mask;
            if (nextChar != ((params.getPacketMagic() >>> 8) & mask))
                continue;
            nextChar = bytes[index++] & mask;
            if (nextChar == (params.getPacketMagic() & mask))
                break;
        }   
        byte[] sizeBytes = Arrays.copyOfRange(bytes, index, index+4);
        long size = Utils.readUint32BE(Utils.reverseBytes(sizeBytes), 0); 
        index += 4;
        byte[] blockBytes = Arrays.copyOfRange(bytes, index, index + (int)size);
        Block nextBlock;
        try {
            nextBlock = params.getDefaultSerializer().makeBlock(blockBytes);
        } catch (ProtocolException e) {
            nextBlock = null;
        }
        return nextBlock;
    }
  }

  public static class BlockReducer extends Reducer<BlockWritable, TransactionWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private IntWritable one = new IntWritable(1);
    private Text blockTag = new Text("block");
    private Text transactionTag = new Text("transaction");

    public void reduce(BlockWritable key, Iterable<TransactionWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (TransactionWritable tx : values) {
        sum += 1;
      }
      result.set(sum);
      context.write(blockTag, one);
      context.write(transactionTag, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "reduce blockchain");
	//include in classpath
    job.setJar("rbc.jar");
    job.addFileToClassPath(new Path("/user/nishil/bitcoin/bitcoinj.jar"));
    job.setMapperClass(BlockMapper.class);
    job.setReducerClass(BlockReducer.class);
    job.setMapOutputKeyClass(BlockWritable.class);
    job.setMapOutputValueClass(TransactionWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(BlockFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
