package blockparser;

import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;

class BlockFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    
    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processedBlock = false;

    private NullWritable key = NullWritable.get();
    private BytesWritable value = new BytesWritable();
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit)inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException {
        if(!processedBlock) {           
            byte[] blockBytes = new byte[(int)fileSplit.getLength()];

            //get file
            Path filePath = fileSplit.getPath();
            FileSystem fileSystem = filePath.getFileSystem(conf);

            FSDataInputStream in = null;
            try {
                in = fileSystem.open(filePath);
                IOUtils.readFully(in, blockBytes, 0, blockBytes.length);
                value.set(blockBytes, 0, blockBytes.length);
            } finally {
                IOUtils.closeStream(in);
            }
            processedBlock = true;
            return true;

            /* File tempFile = File.createTempFile(filePath.getName(), "");
            fileSystem.copyToLocalFile(filePath, new Path(tempFile.getAbsolutePath()));

            List<File> blockFiles = new ArrayList<>();
            blockFiles.add(tempFile);
            
            Context.getOrCreate(MainNetParams.get());
            BlockFileLoader blockFileLoader = new BlockFileLoader(MainNetParams.get(), blockFiles); 
            Block block = null;
            if(blockFileLoader.hasNext()) {
                block = blockFileLoader.next();
            }
    
            tempFile.delete();
            return true; */
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processedBlock ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

}
