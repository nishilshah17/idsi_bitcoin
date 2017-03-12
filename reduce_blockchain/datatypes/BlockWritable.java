package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import org.bitcoinj.core.Block;

public class BlockWritable implements WritableComparable<BlockWritable> {

    private Text hash;
    private Text prevHash;
    private Text merkleRoot;
    private Text time;
    private long version;
    private int transactionCount;

    public BlockWritable() {
        this.hash = new Text();
        this.prevHash = new Text();
        this.merkleRoot = new Text();
        this.time = new Text();
    }

    public BlockWritable(Block block) {
        this.hash = new Text(block.getHashAsString());   
        this.prevHash = new Text(block.getPrevBlockHash().toString());
        this.merkleRoot = new Text(block.getMerkleRoot().toString());
        this.time = new Text(block.getTime().toString());
        this.version = block.getVersion();
        this.transactionCount = block.getTransactions().size();
    }

    public Text getHash() {
        return hash;
    }

    public Text getPrevHash() {
        return prevHash;
    }

    public Text getMerkleRoot() {
        return merkleRoot;
    }

    public Text getTime() {
        return time;
    }

    public long getVersion() {
        return version;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        hash.write(out);
        prevHash.write(out);
        merkleRoot.write(out);
        time.write(out);
        out.writeLong(version);
        out.writeInt(transactionCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hash.readFields(in);
        prevHash.readFields(in);
        merkleRoot.readFields(in);
        time.readFields(in);
        version = in.readLong();
        transactionCount = in.readInt();
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
