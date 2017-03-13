package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import org.bitcoinj.core.Block;

public class BlockWritable implements WritableComparable<BlockWritable> {

    private String hash;
    private String prevHash;
    private String merkleRoot;
    private String time;
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
        this.version = block.getVersion();
        this.transactionCount = block.getTransactions().size();
    }

    public String getHash() {
        return hash;
    }

    public String getPrevHash() {
        return prevHash;
    }

    public String getMerkleRoot() {
        return merkleRoot;
    }

    public String getTime() {
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
        out.writeUTF(hash);
        out.writeUTF(prevHash);
        out.writeUTF(merkleRoot);
        out.writeUTF(time);
        out.writeLong(version);
        out.writeInt(transactionCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hash = in.readUTF();
		prevHash = in.readUTF();
		merkleRoot = in.readUTF();
		time = in.readUTF();
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
		String str = hash + "," + prevHash + "," + merkleRoot + "," + time + "," + Long.toString(version) + "," + Integer.toString(transactionCount);
		return new Text(str);
	}
}
