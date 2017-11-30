/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package be.ugent.intec.halvade.hadoop.mapreduce;

import be.ugent.intec.halvade.utils.Logger;
import be.ugent.intec.halvade.utils.HalvadeConf;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author ddecap
 */
public class VCFCombineReducer extends Reducer<LongWritable, VariantContextWritable, LongWritable, VariantContextWritable> {
    
    KeyIgnoringVCFOutputFormat outpFormat;
    RecordWriter<LongWritable,VariantContextWritable> recordWriter;
    VariantContextWritable tmpVar;
    VariantContextWritable bestVar;
    boolean reportAllVariants = false;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context); //To change body of generated methods, choose Tools | Templates.
        recordWriter.close(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<VariantContextWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<VariantContextWritable> it = values.iterator();
        // find vcf with best quality
        if(reportAllVariants) {
            while(it.hasNext()){
                recordWriter.write(key, it.next());
            }
        } else {
            if(it.hasNext())
                bestVar = it.next();
            while(it.hasNext()){
                tmpVar = it.next();
                if(bestVar.get().getPhredScaledQual() < tmpVar.get().getPhredScaledQual())
                    bestVar = tmpVar;            
            }
            recordWriter.write(key, bestVar);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            // read header from input
            outpFormat = new KeyIgnoringVCFOutputFormat(VCFFormat.VCF);
            String input = HalvadeConf.getInputDir(context.getConfiguration());
            String output = HalvadeConf.getOutDir(context.getConfiguration());
            reportAllVariants = HalvadeConf.getReportAllVariant(context.getConfiguration());
            FileSystem fs = FileSystem.get(new URI(input), context.getConfiguration());
            Path firstVcfFile = null;
            if (fs.getFileStatus(new Path(input)).isDirectory()) {
                // get first file
                FileStatus[] files = fs.listStatus(new Path(input));
                int i = 0, l = files.length;
                while(i < l && !files[i].getPath().getName().endsWith(".vcf")) {
                    i++;
                }
                if(i < l) {
                    firstVcfFile = files[i].getPath();
                } else {
                    throw new InterruptedException("VCFCombineReducer: No files in input folder.");
                }
            } else {
                throw new InterruptedException("VCFCombineReducer: Input directory is not a directory.");
            }
            Logger.DEBUG("first file: " + firstVcfFile);
            outpFormat.readHeaderFrom(firstVcfFile, fs);
            
            boolean outputGVCF = HalvadeConf.getOutputGVCF(context.getConfiguration());
            recordWriter = outpFormat.getRecordWriter(context, new Path(output + (outputGVCF ? "HalvadeCombined.g.vcf" : "HalvadeCombined.vcf")));
        } catch (URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            throw new InterruptedException("URI for input directory is invalid.");
        }
    }
    
}
