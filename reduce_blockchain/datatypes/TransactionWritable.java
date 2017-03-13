package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import org.bitcoinj.core.Transaction;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private String hash;
	private String time;
	private long value;
	private long fee;
	private boolean isCoinBase;

    public TransactionWritable() {
        this.hash = new String();
		this.time = new String();
    }

    public TransactionWritable(Transaction tx) {
        this.hash = tx.getHashAsString();
		this.time = tx.getUpdateTime().toString();
		this.fee = (tx.getFee() == null ? 0 : tx.getFee().getValue());
		this.isCoinBase = tx.isCoinBase();
    }

    public String getHash() {
        return hash;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(hash);
		out.writeUTF(time);
		//out.writeLong(value);
		out.writeLong(fee);
		out.writeBoolean(isCoinBase);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
		hash = in.readUTF();
		time = in.readUTF();
		//value = in.readLong();
		fee = in.readLong();
		isCoinBase = in.readBoolean();
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof TransactionWritable) {
            TransactionWritable otherTransaction = (TransactionWritable) other;
            return hash.equals(otherTransaction.hash);
        }
        return false;
    }
    
    @Override
    public int compareTo(TransactionWritable other) {
        return hash.compareTo(other.hash);
    } 

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

	public Text toText() {
		String str = hash + "," + time + "," + Long.toString(fee) + "," + Boolean.toString(isCoinBase);
		return new Text(str);
	}
} 
