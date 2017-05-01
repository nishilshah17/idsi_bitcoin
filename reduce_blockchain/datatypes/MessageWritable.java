package datatypes;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.GenericWritable;

import datatypes.BlockWritable;
import datatypes.TransactionWritable;

public class MessageWritable extends GenericWritable {

    private static Class[] CLASSES = { BlockWritable.class, TransactionWritable.class };
   
    public MessageWritable() {
    
    }

    public MessageWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class[] getTypes() {
        return CLASSES;
    }
}
