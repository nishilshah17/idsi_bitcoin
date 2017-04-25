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

import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Utils;
import org.bitcoinj.params.MainNetParams;

import blockparser.BlockUtils;

class BlockFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private final NetworkParameters NETWORK_PARAMETERS = BlockUtils.getNetworkParameters();    
    private final int mask = 0xff;

    private int fileIndex = 0;
    private byte[] fileBytes;

    private NullWritable key = NullWritable.get();
    private BytesWritable value = new BytesWritable();
    
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileBytes = readFile(inputSplit, taskAttemptContext.getConfiguration());
    }

    public boolean nextKeyValue() throws IOException {
        if(fileIndex < fileBytes.length) {
            int nextChar = fileBytes[fileIndex++] & mask;
            while (nextChar != -1) {
                if(fileIndex >= fileBytes.length) return false;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 24) & mask)) {
                    nextChar = fileBytes[fileIndex++] & mask;
                    continue;
                }   
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 16) & mask))
                    continue;
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 8) & mask))
                    continue;
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar == (NETWORK_PARAMETERS.getPacketMagic() & mask))
                    break;
            }   
            byte[] sizeBytes = Arrays.copyOfRange(fileBytes, fileIndex, fileIndex+4);
            long size = Utils.readUint32BE(Utils.reverseBytes(sizeBytes), 0); 
            fileIndex += 4;
            byte[] blockBytes = Arrays.copyOfRange(fileBytes, fileIndex, fileIndex + (int)size);
            fileIndex += (int)size;
            //set value
            value.set(blockBytes, 0, blockBytes.length);
            return true;
        }
        return false;
    }
   
    private byte[] readFile(InputSplit inputSplit, Configuration conf) throws IOException {
        //setup
        FileSplit fileSplit = (FileSplit)inputSplit;
        int splitLength = (int)fileSplit.getLength();
        byte[] blockBytes = new byte[splitLength];
        //get file
        Path filePath = fileSplit.getPath();
        FileSystem fileSystem = filePath.getFileSystem(conf);
        //read bytes
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(filePath);
            IOUtils.readFully(in, blockBytes, 0, blockBytes.length);
        } finally {
            IOUtils.closeStream(in);
        }
        return blockBytes;
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
        return ((float) fileIndex / fileBytes.length);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

}
