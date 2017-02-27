package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class BlockWritable implements WritableComparable<BlockWritable> {

    private Text hash;

    public BlockWritable() {
        this.hash = new Text();
    }

    public BlockWritable(String hash) {
        this.hash = new Text(hash);   
    }

    public void set(Text hash) {
        this.hash = hash;
    }

    public Text getHash() {
        return hash;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        hash.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hash.readFields(in);
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof BlockWritable) {
            BlockWritable otherBlock = (BlockWritable) other;
            return hash.equals(otherBlock.hash);
        }
        return false;
    }

    @Override
    public int compareTo(BlockWritable other) {
        return hash.compareTo(other.hash);
    }
    
    @Override
    public int hashCode() {
        return hash.hashCode();
    }
}
