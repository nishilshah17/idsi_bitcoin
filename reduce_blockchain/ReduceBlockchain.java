import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.text.DateFormatSymbols;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Utils;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ProtocolException;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.params.MainNetParams;

import blockparser.BlockFileInputFormat;
import blockparser.BlockUtils;
import datatypes.BlockWritable;
import datatypes.TransactionWritable;

public class ReduceBlockchain {

    public static class BlockMapper extends Mapper<NullWritable, BytesWritable, BlockWritable, TransactionWritable> {

        private Text word = new Text();

        public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            //get block
            Block block = BlockUtils.getBlock(value.getBytes());
            BlockWritable blockWritable = new BlockWritable(block);
            String blockHash = blockWritable.getHash();

            //get transactions
            for(Transaction transaction : block.getTransactions()) {
                context.write(blockWritable, new TransactionWritable(transaction, blockHash));
            }
        }
    }

    public static class BlockReducer extends Reducer<BlockWritable, TransactionWritable, Text, IntWritable> {
        private final String BLOCK_SUFFIX = "-blocks";
        private final String TRANSACTION_SUFFIX = "-transactions";

        private IntWritable one = new IntWritable(1);
        private Text blockTag = new Text("block");
        private Text transactionTag = new Text("transaction");

        private MultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);
        }

        public void reduce(BlockWritable key, Iterable<TransactionWritable> values, Context context) throws IOException, InterruptedException {
            String[] outputFileNames = getOutputFileNames(key);
            multipleOutputs.write(key.toText(), one, outputFileNames[0]);
            for (TransactionWritable transactionWritable : values) {
                multipleOutputs.write(transactionWritable.toText(), one, outputFileNames[1]);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        public String[] getOutputFileNames(BlockWritable key) {
            String fileName;
            String[] fileNames = new String[2];
            try {
                String time = key.getTime();
                DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
                Date date = dateFormat.parse(time);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(date);
                String month = new DateFormatSymbols().getMonths()[calendar.get(Calendar.MONTH)];
                fileName = month + "-" + calendar.get(Calendar.YEAR);
            } catch (ParseException pe) {
                fileName = "date-fail";
            }
            fileNames[0] = fileName + BLOCK_SUFFIX;
            fileNames[1] = fileName + TRANSACTION_SUFFIX;
            return fileNames;
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
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
