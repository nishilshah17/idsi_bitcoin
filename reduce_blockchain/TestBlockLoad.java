//java
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;

//bitcoinj
import org.bitcoinj.core.Context;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.core.ProtocolException;
import org.bitcoinj.core.Utils;

//apache
import sun.misc.IOUtils;

public class TestBlockLoad {

    public static void main(String args[]) {
        String blockPath = "/home/hdfs/nishil/bitcoin/reduce_blockchain/blk00001.dat";
        File blockFile = new File(blockPath);
        NetworkParameters params = MainNetParams.get();
        Context.getOrCreate(params);
        FileInputStream currentFileStream;
        try {
            currentFileStream = new FileInputStream(blockFile);
        } catch (FileNotFoundException e) {
            currentFileStream = null;
        }
        byte[] bytes;
        try {
            bytes = IOUtils.readFully(currentFileStream, -1, false);
        } catch (IOException e) {
            bytes = null;
        }
        int index = 0;
            int nextChar = bytes[index++];
            while (nextChar != -1) {
                if (nextChar != ((params.getPacketMagic() >>> 24) & 0xff)) {
                    nextChar = bytes[index++];
                    continue;
                }
                nextChar = bytes[index++];
                if (nextChar != ((params.getPacketMagic() >>> 16) & 0xff))
                    continue;
                nextChar = bytes[index++];
                if (nextChar != ((params.getPacketMagic() >>> 8) & 0xff))
                    continue;
                nextChar = bytes[index++];
                if (nextChar == (params.getPacketMagic() & 0xff))
                    break;
            }
            System.out.println("" + index);
            byte[] sizeBytes = Arrays.copyOfRange(bytes, index, index+4);
            long size = Utils.readUint32BE(Utils.reverseBytes(sizeBytes), 0);
            System.out.println("" + size);
            index += 4;
            byte[] blockBytes = Arrays.copyOfRange(bytes, index, index + (int)size);
            Block nextBlock;
            try {
                nextBlock = params.getDefaultSerializer().makeBlock(blockBytes);
            } catch (ProtocolException e) {
                nextBlock = null;
            }
            System.out.println(nextBlock.getHash().toString());
    }
}
