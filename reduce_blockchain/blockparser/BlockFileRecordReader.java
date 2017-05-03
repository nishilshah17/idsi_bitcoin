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

    private byte[] fileBytes;
    private int fileIndex;
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.inputSplit = inputSplit;
        this.taskAttemptContext = taskAttemptContext;    
        this.fileBytes = readFile();
        this.fileIndex = 0;
    }

    public boolean nextKeyValue() throws IOException {
        byte[] blockBytes = BlockUtils.nextBlockBytes(fileBytes, fileIndex);

        if(blockBytes != null) {
            value.set(blockBytes, 0, blockBytes.length);
            fileIndex += (4 + blockBytes.length);
            return true;
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
        return (float)fileIndex / (float)fileBytes.length;
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

}
