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

class BlockFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    
    private InputSplit inputSplit;
    private Configuration conf;
    private boolean processedBlock = false;

    private NullWritable key = NullWritable.get();
    private BytesWritable value = new BytesWritable();
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.inputSplit = inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException {
        if(!processedBlock) {           
            FileSplit fileSplit = (FileSplit)inputSplit;
            int splitLength = (int)fileSplit.getLength();
            byte[] blockBytes = new byte[splitLength];

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
            return processedBlock = true;
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
