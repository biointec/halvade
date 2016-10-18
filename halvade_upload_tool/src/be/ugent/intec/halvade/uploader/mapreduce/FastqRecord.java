/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author dries
 */
public class FastqRecord implements Writable {
    protected String id;
    protected String read;
    protected String qual;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    public String getQual() {
        return qual;
    }

    public void setQual(String qual) {
        this.qual = qual;
    }

    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(id);
        d.writeUTF(read);
        d.writeUTF(qual);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id = di.readUTF();
        read = di.readUTF();
        qual = di.readUTF();
    }

    @Override
    public String toString() {
        return "FastqRecord{" + "id=" + id + ", read=" + read + ", qual=" + qual + '}';
    }
    
}
