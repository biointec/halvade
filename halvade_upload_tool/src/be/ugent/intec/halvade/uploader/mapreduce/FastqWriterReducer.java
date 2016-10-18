/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 *
 * @author dries
 */
public class FastqWriterReducer  extends Reducer<PairedIdWritable, FastqRecord, PairedIdWritable, FastqRecord> {
    String prevId = "";
    
    @Override
    protected void reduce(PairedIdWritable key, Iterable<FastqRecord> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        Iterator<FastqRecord> it = values.iterator();
//        System.err.println("key: " + key.toString());
        while (it.hasNext()) {
            FastqRecord s = it.next();
//            System.err.println("val: " + s.toString() + " and prev: " + prevId);
            if(!s.getId().equals(prevId)) {
                context.write(key, s);
            }
            prevId = s.getId();
        }
    }
    
}
