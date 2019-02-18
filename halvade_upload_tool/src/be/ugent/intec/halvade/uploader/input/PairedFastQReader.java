/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.uploader.input;

import be.ugent.intec.halvade.uploader.Logger;
import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author ddecap
 */
public class PairedFastQReader extends BaseFileReader {
    protected BufferedReader readerA, readerB;
    protected ReadBlock block;
    

    public PairedFastQReader(boolean fromHDFS, String fileA, String fileB) throws IOException {
        super(true);
        readerA = getReader(fromHDFS, fileA);
        readerB = getReader(fromHDFS, fileB);
        toStr = fileA.substring(fileA.lastIndexOf("/")+1) + " & " + fileB.substring(fileB.lastIndexOf("/")+1);
        Logger.DEBUG("Paired: " + toStr);
    }
    
    @Override
    public void closeReaders() throws IOException {
        readerA.close();
        readerB.close();
        Logger.INFO("closed: " + toStr);        
    }

    @Override
    protected int addNextRead(ReadBlock block) throws IOException {
        if(block.checkCapacity(LINES_PER_READ*2)) {
            block.setCheckPoint();
            boolean check = true;
            int i = 0;
            while(i <LINES_PER_READ && check) {
                check = block.fastAddLine(readerA.readLine());
                i++;
            }
            i = 0;
            while(i <LINES_PER_READ && check) {
                check = check && block.fastAddLine(readerB.readLine());
                i++;
            }
            if(!check) {
                // open next read here....
                block.revertToCheckPoint();
                return -1;
            }
            return 0;
        } else 
            return 1;
    }
    
    
}
