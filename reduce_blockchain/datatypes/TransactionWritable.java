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
import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.params.MainNetParams;

import blockparser.BlockUtils;

public class TransactionWritable implements WritableComparable<TransactionWritable> {

    private String blockHash;
    private String hash;
	private String time;
	private long fee;
    private int size;
	private boolean isCoinBase;
    private String[] inputSplit = new String[2];
    private String[] outputSplit = new String[4];
    private String inputValues;
    private String outputValues;

    private final NetworkParameters NETWORK_PARAMETERS = BlockUtils.getNetworkParameters();

    public TransactionWritable() {
        this.blockHash = new String();
        this.hash = new String();
		this.time = new String();
        for(int i = 0; i < inputSplit.length; i++) this.inputSplit[i] = new String();
        for(int i = 0; i < outputSplit.length; i++) this.outputSplit[i] = new String();
        this.inputValues = new String();
        this.outputValues = new String();
    }

    public TransactionWritable(Transaction tx, String blockHash) {
        this.blockHash = blockHash;
        this.hash = tx.getHashAsString();
		this.time = tx.getUpdateTime().toString();
		this.fee = (tx.getFee() == null ? 0 : tx.getFee().getValue());
        this.size = tx.getMessageSize();
		this.isCoinBase = tx.isCoinBase();
        String inputs = String.join(":", tx.getInputs().stream().map(input -> getInputAddress(input)).collect(Collectors.toList()));
        String outputs = String.join(":", tx.getOutputs().stream().map(output -> getOutputAddress(output)).collect(Collectors.toList()));
        setInputs(inputs);
        setOutputs(outputs);
        this.inputValues = String.join(":", tx.getInputs().stream().map(input -> getInputValue(input)).collect(Collectors.toList()));
        this.outputValues = String.join(":", tx.getOutputs().stream().map(output -> getOutputValue(output)).collect(Collectors.toList()));
    }

    private void setInputs(String inputs) {
        int mid = inputs.length() / 2;

        this.inputSplit[0] = inputs.substring(0, mid);
        this.inputSplit[1] = inputs.substring(mid);
    }

    private void setOutputs(String outputs) {
        int mid = outputs.length() / 2;
        int quarter = mid / 2;

        this.outputSplit[0] = outputs.substring(0, quarter);
        this.outputSplit[1] = outputs.substring(quarter, mid);
        this.outputSplit[2] = outputs.substring(mid, mid + quarter);
        this.outputSplit[3] = outputs.substring(mid + quarter);
    }

    private String getInputAddress(TransactionInput input) {
        Address inputAddress;
        try {
            inputAddress = input.getFromAddress();
        } catch (ScriptException e) {
            inputAddress = null;
        }
        return (inputAddress == null ? "null" : inputAddress.toString()); 
    }

    private String getOutputAddress(TransactionOutput output) {
        Address outputAddress;
        try {
            outputAddress = output.getAddressFromP2PKHScript(NETWORK_PARAMETERS);
        } catch (ScriptException e) {
            outputAddress = null;
        }
        return (outputAddress == null ? getAlternateOutputAddress(output) : outputAddress.toString());
    }

    private String getAlternateOutputAddress(TransactionOutput output) {
        Address outputAddress;
        try {
            outputAddress = output.getAddressFromP2SH(NETWORK_PARAMETERS);
        } catch (ScriptException e) {
            outputAddress = null;
        }
        return (outputAddress == null ? "null" : outputAddress.toString());
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
        out.writeUTF(blockHash);
        out.writeUTF(hash);
		out.writeUTF(time);
		out.writeLong(fee);
        out.writeInt(size);
		out.writeBoolean(isCoinBase);
        for(int i = 0; i < inputSplit.length; i++) out.writeUTF(inputSplit[i]);
        for(int i = 0; i < outputSplit.length; i++) out.writeUTF(outputSplit[i]);
        out.writeUTF(inputValues);
        out.writeUTF(outputValues);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        blockHash = in.readUTF();
		hash = in.readUTF();
		time = in.readUTF();
		fee = in.readLong();
        size = in.readInt();
		isCoinBase = in.readBoolean();
        for(int i = 0; i < inputSplit.length; i++) inputSplit[i] = in.readUTF();
        for(int i = 0; i < outputSplit.length; i++) outputSplit[i] = in.readUTF();
        inputValues = in.readUTF();
        outputValues = in.readUTF();
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
		String str = blockHash;
        str += "," + hash;
        str += "," + time;
        str += "," + Long.toString(fee);
        str += "," + size;
        str += "," + Boolean.toString(isCoinBase);
        str += "," + inputSplit[0] + inputSplit[1];
        str += "," + outputSplit[0] + outputSplit[1] + outputSplit[2] + outputSplit[3];
        str += "," + inputValues;
        str += "," + outputValues;
		return new Text(str);
	}
} 
