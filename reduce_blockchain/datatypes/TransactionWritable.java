package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private Text hash;

    public TransactionWritable() {
        this.hash = new Text();
    }

    public TransactionWritable(String hash) {
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
} 
