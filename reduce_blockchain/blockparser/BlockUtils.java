package blockparser;

import java.util.Arrays;

import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Utils;
import org.bitcoinj.core.ProtocolException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;

public class BlockUtils {

    public static Block getBlock(byte[] bytes) {
        NetworkParameters params = MainNetParams.get();
        org.bitcoinj.core.Context.getOrCreate(params);

        int index = 0;
        int mask = 0xff;
        int nextChar = bytes[index++] & mask;
        while (nextChar != -1) {
            if (nextChar != ((params.getPacketMagic() >>> 24) & mask)) {
                nextChar = bytes[index++] & mask;
                continue;
            }   
            nextChar = bytes[index++] & mask;
            if (nextChar != ((params.getPacketMagic() >>> 16) & mask))
                continue;
            nextChar = bytes[index++] & mask;
            if (nextChar != ((params.getPacketMagic() >>> 8) & mask))
                continue;
            nextChar = bytes[index++] & mask;
            if (nextChar == (params.getPacketMagic() & mask))
                break;
        }   
        byte[] sizeBytes = Arrays.copyOfRange(bytes, index, index+4);
        long size = Utils.readUint32BE(Utils.reverseBytes(sizeBytes), 0); 
        index += 4;
        byte[] blockBytes = Arrays.copyOfRange(bytes, index, index + (int)size);
        Block nextBlock;
        try {
            nextBlock = params.getDefaultSerializer().makeBlock(blockBytes);
        } catch (ProtocolException e) {
            nextBlock = null;
        }
        return nextBlock;
    }

}
