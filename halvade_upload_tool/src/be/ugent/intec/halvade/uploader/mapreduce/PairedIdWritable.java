/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author dries
 */
public class PairedIdWritable implements WritableComparable {
    protected int idHashCode;
    protected String id;

    public int getIdHashCode() {
        return idHashCode;
    }

    public void setIdHashCode(int hash) {
        this.idHashCode = (hash & Integer.MAX_VALUE);
//        System.err.println("hashcode: " + idHashCode + " -> " + (idHashCode % 5000));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(idHashCode);
        d.writeUTF(id.toString());
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        idHashCode = di.readInt();
        id = di.readUTF();
    }

    @Override
    public int compareTo(Object o) {
        return ((PairedIdWritable)o).getIdHashCode() - idHashCode;
    }

    @Override
    public String toString() {
        return "PairedIdWritable{" + "idHashCode=" + idHashCode + ", id=" + id + '}';
    }
}
