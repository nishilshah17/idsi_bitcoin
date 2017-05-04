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
    private long value;
    private int size;
    private boolean isCoinBase;
    private String inputs;
    private String outputs;
    private String outputValues;

    private final NetworkParameters NETWORK_PARAMETERS = BlockUtils.getNetworkParameters();

    public TransactionWritable() {
        this.blockHash = new String();
        this.hash = new String();
        this.value = 0;
        this.isCoinBase = false;
        this.inputs = new String();
        this.outputs = new String();
        this.outputValues = new String();
    }

    public TransactionWritable(Transaction tx, String blockHash) {
        this.blockHash = blockHash;
        this.hash = tx.getHashAsString();
        this.value = tx.getOutputSum().getValue();
        this.size = tx.getMessageSize();
        this.isCoinBase = tx.isCoinBase();
        this.inputs = String.join(":", tx.getInputs().stream().map(input -> getInputAddress(input)).collect(Collectors.toList()));
        this.outputs = String.join(":", tx.getOutputs().stream().map(output -> getOutputAddress(output)).collect(Collectors.toList()));
        this.outputValues = String.join(":", tx.getOutputs().stream().map(output -> getOutputValue(output)).collect(Collectors.toList()));
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
        return (outputAddress == null ? getP2SHOutputAddress(output) : outputAddress.toString());
    }

    private String getP2SHOutputAddress(TransactionOutput output) {
        Address outputAddress;
        try {
            outputAddress = output.getAddressFromP2SH(NETWORK_PARAMETERS);
        } catch (ScriptException e) {
            outputAddress = null;
        }
        return (outputAddress == null ? "null" : outputAddress.toString());
    }

    private String getOutputValue(TransactionOutput output) {
        Coin outputValue = output.getValue();
        long satoshis = (outputValue != null ? outputValue.getValue() : 0);
        return Long.toString(satoshis);
    }

    private void writeLongString(String string, DataOutput out) throws IOException {
        byte[] data = string.getBytes("UTF-8");
        out.writeInt(data.length);
        out.write(data);
    }

    private String readLongString(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return new String(data, "UTF-8");
    } 

    @Override
     public void write(DataOutput out) throws IOException {
        out.writeUTF(blockHash);
        out.writeUTF(hash);
        out.writeLong(value);
        out.writeInt(size);
        out.writeBoolean(isCoinBase);
        writeLongString(inputs, out);
        writeLongString(outputs, out);
        writeLongString(outputValues, out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        blockHash = in.readUTF();
        hash = in.readUTF();
        value = in.readLong();
        size = in.readInt();
        isCoinBase = in.readBoolean();
        inputs = readLongString(in);
        outputs = readLongString(in);
        outputValues = readLongString(in);
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

    public String getHash() {
        return hash;
    }

    public Text toText() {
        String str = blockHash;
        str += "," + hash;
        str += "," + Long.toString(value);
        str += "," + size;
        str += "," + Boolean.toString(isCoinBase);
        str += "," + inputs;
        str += "," + outputs;
        str += "," + outputValues;
        return new Text(str);
    }
} 
