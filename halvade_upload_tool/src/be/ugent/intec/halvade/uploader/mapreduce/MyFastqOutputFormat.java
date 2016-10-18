/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.uploader.mapreduce;

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.seqdoop.hadoop_bam.FormatConstants.BaseQualityEncoding;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

/**
 *
 * @author dries
 */
public class MyFastqOutputFormat extends TextOutputFormat<PairedIdWritable, FastqRecord> {

    public static final String CONF_BASE_QUALITY_ENCODING = "hbam.fastq-output.base-quality-encoding";
    public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "sanger";
    public static final Charset UTF8 = Charset.forName("UTF8");

    static final byte[] PLUS_LINE;

    static {
        try {
            PLUS_LINE = "\n+\n".getBytes("us-ascii");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException("us-ascii encoding not supported!");
        }
    }

    public static class FastqRecordWriter extends RecordWriter<PairedIdWritable, FastqRecord> {

        protected StringBuilder sBuilder = new StringBuilder(800);
        protected Text buffer = new Text();
        protected OutputStream out;
        protected BaseQualityEncoding baseQualityFormat;
        protected Text key = new Text();
        protected SequencedFragment seq = new SequencedFragment();
        protected Text sequence = new Text();
        protected Text qual = new Text();

        public FastqRecordWriter(Configuration conf, OutputStream out) {
            this.out = out;
            setConf(conf);
        }

        public void setConf(Configuration conf) {
            String setting = conf.get(CONF_BASE_QUALITY_ENCODING, CONF_BASE_QUALITY_ENCODING_DEFAULT);
            if ("illumina".equals(setting)) {
                baseQualityFormat = BaseQualityEncoding.Illumina;
            } else if ("sanger".equals(setting)) {
                baseQualityFormat = BaseQualityEncoding.Sanger;
            } else {
                throw new RuntimeException("Invalid property value '" + setting + "' for " + CONF_BASE_QUALITY_ENCODING + ".  Valid values are 'illumina' or 'sanger'");
            }
        }

        public void write(PairedIdWritable k, FastqRecord v) throws IOException {
            key.set(k.getId());
            sequence.set(v.getRead());
            qual.set(v.getQual());
            seq.setSequence(sequence);
            seq.setQuality(qual);
//            System.err.println("writing: "+ v);
            
            
            // write the id line
            out.write('@');
            if (key != null) {
                out.write(key.getBytes(), 0, key.getLength());
            } else {
                throw new NullPointerException("key is null");
            }
            out.write('\n');

            // write the sequence and separator
            out.write(seq.getSequence().getBytes(), 0, seq.getSequence().getLength());
            out.write(PLUS_LINE);

            // now the quality
            if (baseQualityFormat == BaseQualityEncoding.Sanger) {
                out.write(seq.getQuality().getBytes(), 0, seq.getQuality().getLength());
            } else if (baseQualityFormat == BaseQualityEncoding.Illumina) {
                buffer.set(seq.getQuality());
                SequencedFragment.convertQuality(buffer, BaseQualityEncoding.Sanger, baseQualityFormat);
                out.write(buffer.getBytes(), 0, buffer.getLength());
            } else {
                throw new RuntimeException("FastqOutputFormat: unknown base quality format " + baseQualityFormat);
            }

            // and the final newline
            out.write('\n');
        }

        public void close(TaskAttemptContext task) throws IOException {
            out.close();
        }
    }

    public RecordWriter<PairedIdWritable, FastqRecord> getRecordWriter(TaskAttemptContext task)
            throws IOException {
        Configuration conf = task.getConfiguration();
        boolean isCompressed = getCompressOutput(task);

        CompressionCodec codec = null;
        String extension = "";

        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(task, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }

        Path file = getDefaultWorkFile(task, extension);
        FileSystem fs = file.getFileSystem(conf);

        OutputStream output;

        if (isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            output = new DataOutputStream(codec.createOutputStream(fileOut));
        } else {
            output = fs.create(file, false);
        }

        return new FastqRecordWriter(conf, output);
    }
}
