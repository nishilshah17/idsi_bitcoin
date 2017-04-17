package blockparser;

import java.util.Date;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.text.DateFormatSymbols;
import java.util.Arrays;

import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.ProtocolException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;

import datatypes.BlockWritable;

public class BlockUtils {

    private static final String BLOCK_SUFFIX = "-blocks";
    private static final String TRANSACTION_SUFFIX = "-transactions";
    private static final NetworkParameters NETWORK_PARAMETERS = MainNetParams.get();

    public static Block parseBlock(byte[] bytes) {
        org.bitcoinj.core.Context.getOrCreate(NETWORK_PARAMETERS);

        Block block;
        try {
            block = NETWORK_PARAMETERS.getDefaultSerializer().makeBlock(bytes);
        } catch (ProtocolException e) {
            block = null;
        }
        return block;
    }

    public static String[] getOutputFileNames(BlockWritable key) {
        String fileName;
        String[] fileNames = new String[2];
        try {
            String time = key.getTime();
            DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            Date date = dateFormat.parse(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            String month = new DateFormatSymbols().getMonths()[calendar.get(Calendar.MONTH)];
            fileName = month + "-" + calendar.get(Calendar.YEAR);
        } catch (ParseException pe) {
            fileName = "date-fail";
        }
        fileNames[0] = fileName + BLOCK_SUFFIX;
        fileNames[1] = fileName + TRANSACTION_SUFFIX;
        return fileNames;
    }

    public static NetworkParameters getNetworkParameters() {
        return NETWORK_PARAMETERS;
    }

}
