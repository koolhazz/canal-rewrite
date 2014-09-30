package com.alibaba.otter.canal.parser;

import com.alibaba.otter.canal.protocol.position.EntryPosition;

import java.io.*;
import java.nio.Buffer;

/**
 * Created by hp on 14-8-22.
 */
public class PositionManager {
    public EntryPosition getEntryPosition() {
        return entryPosition;
    }

    private EntryPosition entryPosition;

    private String fileName = "position.dat";

    public PositionManager(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public PositionManager() {
    }

    public boolean readDatFile() throws IOException{
        File datFile = new File(fileName);
        if(!datFile.exists()||datFile.isDirectory()){
            return(false);
        }
        BufferedReader br = new BufferedReader(new FileReader(datFile));
        String dataString = br.readLine();
        String [] datSplit = dataString.split(":");
        entryPosition = new EntryPosition(datSplit[0],Long.valueOf(datSplit[1]));
        br.close();
        return(true);
    }

    public void writeDatFile() throws IOException{
        if(entryPosition!=null&&entryPosition.getJournalName()!=null&&entryPosition.getPosition()!=0) {
            File datFile = new File("position.dat");
            if (!datFile.exists()) {
                datFile.createNewFile();
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(datFile));
            String dataString = entryPosition.getJournalName()+":"+entryPosition.getPosition();
            bw.write(dataString);
            bw.newLine();
            bw.flush();
            bw.close();
        }
    }
}
