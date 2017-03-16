package datatypes;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

import org.bitcoinj.core.Coin;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private String hash;
	private String time;
	private long fee;
	private boolean isCoinBase;
    private String inputs;
    private String outputs;

    public TransactionWritable() {
        this.hash = new String();
		this.time = new String();
        this.inputs = new String();
        this.outputs = new String();
    }

    public TransactionWritable(Transaction tx) {
        this.hash = tx.getHashAsString();
		this.time = tx.getUpdateTime().toString();
		this.fee = (tx.getFee() == null ? 0 : tx.getFee().getValue());
		this.isCoinBase = tx.isCoinBase();
        this.inputs = String.join(":", tx.getInputs().stream().map(input -> getInputValue(input)).collect(Collectors.toList()));
        this.outputs = String.join(":", tx.getOutputs().stream().map(output -> getOutputValue(output)).collect(Collectors.toList()));

    }

    private String getInputValue(TransactionInput input) {
        Coin inputValue = input.getValue();
        long satoshis = (inputValue != null ? inputValue.getValue() : 0);
        return Long.toString(satoshis);
    }

    private String getOutputValue(TransactionOutput output) {
        Coin outputValue = output.getValue();
        long satoshis = (outputValue != null ? outputValue.getValue() : 0);
        return Long.toString(satoshis);
    }

    public String getHash() {
        return hash;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(hash);
		out.writeUTF(time);
		out.writeLong(fee);
		out.writeBoolean(isCoinBase);
        out.writeUTF(inputs);
        out.writeUTF(outputs);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
		hash = in.readUTF();
		time = in.readUTF();
		fee = in.readLong();
		isCoinBase = in.readBoolean();
        inputs = in.readUTF();
        outputs = in.readUTF();
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
		String str = hash + "," + time + "," + Long.toString(fee) + "," + Boolean.toString(isCoinBase) + "," + inputs + "," + outputs;
		return new Text(str);
	}
} 
