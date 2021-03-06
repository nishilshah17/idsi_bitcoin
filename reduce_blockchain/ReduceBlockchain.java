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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;

import blockparser.BlockFileInputFormat;
import blockparser.BlockUtils;
import datatypes.BlockWritable;
import datatypes.TransactionWritable;
import datatypes.MessageWritable;

public class ReduceBlockchain {

    public static class BlockMapper extends Mapper<NullWritable, BytesWritable, Text, MessageWritable> {

        private Text outKey;
        private MessageWritable outValue;

        public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            //parse block
            byte[] blockBytes = value.getBytes();
            Block block = BlockUtils.parseBlock(blockBytes);

            //write block
            BlockWritable blockWritable = new BlockWritable(block);
            outKey = BlockUtils.getKey(blockWritable);
            outValue = new MessageWritable(blockWritable);
            context.write(outKey, outValue);

            //write transactions
            String blockHash = blockWritable.getHash();
            String time = blockWritable.getTime();
            for(Transaction transaction : block.getTransactions()) {
                TransactionWritable transactionWritable = new TransactionWritable(transaction, blockHash, time);
                outValue = new MessageWritable(transactionWritable);
                context.write(outKey, outValue);
            }
        }
    }

    public static class BlockReducer extends Reducer<Text, MessageWritable, Text, NullWritable> {

        private String blockTag = "blocks";
        private String transactionTag = "transactions";
        private String monthActivityTag = "activity-by-month";

        private Text outKey;
        private NullWritable outValue = NullWritable.get();
        private MultipleOutputs multipleOutputs;

        private long blockCount;
        private long txCount;
        private double volume;

        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
            blockCount = 0;
            txCount = 0;
            volume = 0;
        }

        private double toBTC(long satoshis) {
            return ((double)satoshis) / 100000000;
        }

        public void reduce(Text key, Iterable<MessageWritable> values, Context context) throws IOException, InterruptedException {
            for(MessageWritable messageWritable : values) {
                Writable message = messageWritable.get();

                if(message instanceof BlockWritable) {
                    outKey = ((BlockWritable)message).toText();
                    multipleOutputs.write(outKey, outValue, blockTag + "-" + key);

                    blockCount++;
                }
                if(message instanceof TransactionWritable) {
                    outKey = ((TransactionWritable)message).toText();
                    multipleOutputs.write(outKey, outValue, transactionTag + "-" + key);
                    
                    long transactionValue = ((TransactionWritable)message).getValue();
                    volume += toBTC(transactionValue);
                    txCount++;
                }
            }

            outKey = new Text(key + "," + blockCount + "," + txCount + "," + volume);
            multipleOutputs.write(outKey, outValue, monthActivityTag);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.map.out.compress", true);
        Job job = Job.getInstance(conf, "reduce_blockchain");
        //include jars in classpath
        job.setJar("rbc.jar");
        job.addFileToClassPath(new Path("/user/nishil/bitcoin/bitcoinj.jar"));
        job.setMapperClass(BlockMapper.class);
        job.setReducerClass(BlockReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MessageWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(BlockFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(8);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
