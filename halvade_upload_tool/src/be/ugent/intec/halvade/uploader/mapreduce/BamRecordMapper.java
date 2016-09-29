/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import htsjdk.samtools.SAMRecord;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

/**
 *
 * @author dries
 */
public class BamRecordMapper extends Mapper<LongWritable, SAMRecordWritable, PairedIdWritable, FastqRecord> {
    protected FastqRecord out = new FastqRecord();
    protected PairedIdWritable id = new PairedIdWritable();
    protected String idString;
    
    @Override
    protected void map(LongWritable key, SAMRecordWritable value, Context context) throws IOException, InterruptedException {
        SAMRecord sam = value.get();
        if(!sam.getReadPairedFlag()) return;
        idString = sam.getReadName() + "/" + (sam.getFirstOfPairFlag() ? "1" : "2");
        id.setId(idString);
        id.setIdHashCode(sam.getReadName().hashCode());
        
        out.setId(idString);
        out.setRead(sam.getReadString());
        out.setQual(sam.getBaseQualityString());
        context.write(id, out);
    }
    
}
