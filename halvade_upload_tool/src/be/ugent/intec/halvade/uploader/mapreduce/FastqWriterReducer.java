/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.seqdoop.hadoop_bam.SequencedFragment;

/**
 *
 * @author dries
 */
public class FastqWriterReducer  extends Reducer<PairedIdWritable, FastqRecord, PairedIdWritable, FastqRecord> {
//    SequencedFragment seq = new SequencedFragment();
//    Text sequence = new Text();
//    Text qual = new Text();
//    Text id = new Text();
    String prevId = "";
    
    @Override
    protected void reduce(PairedIdWritable key, Iterable<FastqRecord> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context); //To change body of generated methods, choose Tools | Templates.
        Iterator<FastqRecord> it = values.iterator();
        while (it.hasNext()) {
            FastqRecord s = it.next();
//            sequence.set(s.getRead());
//            qual.set(s.getQual());
//            seq.setSequence(sequence);
//            seq.setQuality(qual);
//            id.set(s.getId());
            if(!s.getId().equals(prevId))
                context.write(key, s);
            prevId = s.getId();
        }
    }
    
}
