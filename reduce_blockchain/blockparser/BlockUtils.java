package blockparser;

import java.util.Date;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.text.DateFormatSymbols;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

import org.bitcoinj.core.Utils;
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
    private static final int mask = 0xff;

    public static byte[] nextBlockBytes(byte[] fileBytes, int fileIndex) {
        if(fileIndex < fileBytes.length) {
            int nextChar = fileBytes[fileIndex++] & mask;
            while (nextChar != -1) {
                if(fileIndex >= fileBytes.length) return null;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 24) & mask)) {
                    nextChar = fileBytes[fileIndex++] & mask;
                    continue;
                }   
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 16) & mask))
                    continue;
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar != ((NETWORK_PARAMETERS.getPacketMagic() >>> 8) & mask))
                    continue;
                nextChar = fileBytes[fileIndex++] & mask;
                if (nextChar == (NETWORK_PARAMETERS.getPacketMagic() & mask))
                    break;
            }   
            byte[] sizeBytes = Arrays.copyOfRange(fileBytes, fileIndex, fileIndex + 4);
            long size = Utils.readUint32BE(Utils.reverseBytes(sizeBytes), 0); 
            fileIndex += 4;
            byte[] blockBytes = Arrays.copyOfRange(fileBytes, fileIndex, fileIndex + (int)size);
            fileIndex += (int)size;
            return blockBytes;
        }
        return null;
    }

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

    public static Text getKey(BlockWritable block) {
        String key = "";
        try {
            String time = block.getTime();
            DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            Date date = dateFormat.parse(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            key += calendar.get(Calendar.YEAR);
            key += "-" + calendar.get(Calendar.MONTH);
        } catch (ParseException pe) {
            key = "parse-exception";
        }
        return new Text(key);
    }

    public static NetworkParameters getNetworkParameters() {
        return NETWORK_PARAMETERS;
    }

}
