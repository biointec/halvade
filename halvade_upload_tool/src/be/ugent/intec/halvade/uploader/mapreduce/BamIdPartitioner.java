/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author dries
 */
public class BamIdPartitioner extends Partitioner<PairedIdWritable, FastqRecord> { 
    
    @Override
    public int getPartition(PairedIdWritable key, FastqRecord value, int numReduceTasks) {
        return key.getIdHashCode() % numReduceTasks;
    }
    
}