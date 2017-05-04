package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Date;
import java.math.BigInteger;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.VerificationException;

public class BlockWritable implements WritableComparable<BlockWritable> {

    private String hash;
    private String prevHash;
    private String merkleRoot;
    private String time;
    private long work;
    private long version;
    private int transactionCount;

    public BlockWritable() {
        hash = new String();
        prevHash = new String();
        merkleRoot = new String();
        time = new String();
    }

    public BlockWritable(Block block) {
        this.hash = block.getHashAsString();   
        this.prevHash = block.getPrevBlockHash().toString();
        this.merkleRoot = block.getMerkleRoot().toString();
        this.time = block.getTime().toString();
        this.work = getWork(block);
        this.version = block.getVersion();
        this.transactionCount = block.getTransactions().size();
    }

    public String getHash() {
        return hash;
    }

    public String getTime() {
        return time;
    }

    private long getWork(Block block) {
        BigInteger work;
        try {
            work = block.getWork();
        } catch(VerificationException e) {
            return 0;
        }
        return work.longValue();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(hash);
        out.writeUTF(prevHash);
        out.writeUTF(merkleRoot);
        out.writeUTF(time);
        out.writeLong(work);
        out.writeLong(version);
        out.writeInt(transactionCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hash = in.readUTF();
        prevHash = in.readUTF();
        merkleRoot = in.readUTF();
        time = in.readUTF();
        work = in.readLong();
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

    public Text toText() {
        String str = hash;
        str += "," + prevHash;
        str += "," + merkleRoot;
        str += "," + time;
        str += "," + Long.toString(work);
        str += "," + Long.toString(version);
        str += "," + Integer.toString(transactionCount);
        return new Text(str);
    }
}
