/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author dries
 */
public class BamIdSortComparator  extends WritableComparator {
    protected BamIdSortComparator() {
        super(PairedIdWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairedIdWritable r1 = (PairedIdWritable) a;
        PairedIdWritable r2 = (PairedIdWritable) b;
                
        return r1.getId().compareTo(r2.getId());
    }
}