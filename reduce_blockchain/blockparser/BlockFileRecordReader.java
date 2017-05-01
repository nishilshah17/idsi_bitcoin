package blockparser;

import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

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

import blockparser.BlockUtils;

class BlockFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private NullWritable key = NullWritable.get();
    private BytesWritable value = new BytesWritable();

    private InputSplit inputSplit;
    private TaskAttemptContext taskAttemptContext;

    private boolean processedFile;
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.inputSplit = inputSplit;
        this.taskAttemptContext = taskAttemptContext;    
        this.processedFile = false;
    }

    public boolean nextKeyValue() throws IOException {
        if(!processedFile) {
            byte[] fileBytes = readFile();
            value.set(fileBytes, 0, fileBytes.length);
            return processedFile = true;
        }
        return false;
    }
   
    private byte[] readFile() throws IOException {
        //setup
        Configuration conf = taskAttemptContext.getConfiguration();
        FileSplit fileSplit = (FileSplit)inputSplit;
        int splitLength = (int)fileSplit.getLength();
        byte[] blockFileBytes = new byte[splitLength];
        //get file
        Path filePath = fileSplit.getPath();
        FileSystem fileSystem = filePath.getFileSystem(conf);
        //read bytes
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(filePath);
            IOUtils.readFully(in, blockFileBytes, 0, blockFileBytes.length);
        } finally {
            IOUtils.closeStream(in);
        }
        return blockFileBytes;
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
        return (processedFile ? (float)1.0 : (float)0.0);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

}
