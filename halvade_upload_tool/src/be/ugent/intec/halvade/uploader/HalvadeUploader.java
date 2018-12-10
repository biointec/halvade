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

package be.ugent.intec.halvade.uploader;

import be.ugent.intec.halvade.uploader.input.FileReaderFactory;
import be.ugent.intec.halvade.uploader.mapreduce.BamIdGroupingComparator;
import be.ugent.intec.halvade.uploader.mapreduce.BamIdPartitioner;
import be.ugent.intec.halvade.uploader.mapreduce.BamIdSortComparator;
import be.ugent.intec.halvade.uploader.mapreduce.FastqRecord;
import be.ugent.intec.halvade.uploader.mapreduce.MyFastqOutputFormat;
import be.ugent.intec.halvade.uploader.mapreduce.PairedIdWritable;
import com.amazonaws.auth.AWSCredentials;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

/**
 *
 * @author ddecap
 */
public class HalvadeUploader  extends Configured implements Tool {
    protected Options options = new Options();
    private int mthreads = 1;
    private long refSize = 3088286401L;
    private int readSize = 151;
    private int maxCov = -1;
    private long maxNumLines = -1;
    private boolean isInterleaved = false;
    private CompressionCodec codec;
    private String manifest;
    private String file1;
    private String file2;
    private boolean BAMInput;
    private String outputDir;
    private int bestFileSize = 60000000; // <64MB
    private boolean SSE = false;
    private String profile = "default";
    private boolean fromHDFS = false;
    private int red = 1;
    private static double advisdedHeapSize = 16;
    
    
    private AWSCredentials credentials;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // check memory:
        double heapMaxSize = Runtime.getRuntime().maxMemory()/(1024.0*1024*1024);
        if(heapMaxSize < advisdedHeapSize)
            System.err.println("WARNING: Heap size is set to " + heapMaxSize + " GB.\nWARNING: To increase performance increase Heap size to at least "+advisdedHeapSize+" GB.");
        Configuration c = new Configuration();
        HalvadeUploader hau = new HalvadeUploader();
        int res = ToolRunner.run(c, hau, args);
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        try {
            parseArguments(strings);  
            if(BAMInput){ 
                // run mapreduce job to preprocess
                runMapReduceJob(file1, outputDir, red);
            } else {
                processFiles();
            }
        } catch (ParseException e) {
            // automatically generate the help statement
            System.err.println("Error parsing: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "hadoop jar HalvadeUploaderWithLibs.jar -1 <MANIFEST> -O <OUT> [options]", options );
        } catch (Throwable ex) {
            Logger.THROWABLE(ex);
        }
        return 0;
    }
    
    private int runMapReduceJob(String in, String out, int reducers) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "bam-prep");
        job.setJarByClass(be.ugent.intec.halvade.uploader.mapreduce.BamRecordMapper.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setMapperClass(be.ugent.intec.halvade.uploader.mapreduce.BamRecordMapper.class);
        job.setReducerClass(be.ugent.intec.halvade.uploader.mapreduce.FastqWriterReducer.class);
        
        
        job.setOutputKeyClass(PairedIdWritable.class);
        job.setOutputValueClass(FastqRecord.class);
        job.setOutputFormatClass(MyFastqOutputFormat.class);
        FastqOutputFormat.setCompressOutput(job, true);
//        FastqOutputFormat.setOutputCompressorClass(job, Lz4Codec.class); // or GzipCodec.class
        
        job.setMapOutputKeyClass(PairedIdWritable.class);
        job.setMapOutputValueClass(FastqRecord.class);
        job.setInputFormatClass(BAMInputFormat.class);
        
        job.setPartitionerClass(BamIdPartitioner.class);
        job.setSortComparatorClass(BamIdSortComparator.class);
        job.setGroupingComparatorClass(BamIdGroupingComparator.class);

        job.setNumReduceTasks(reducers);
        
        
        Logger.DEBUG("Started Halvade Preprocessing");
        Timer timer = new Timer();
        timer.start();
        int ret = job.waitForCompletion(true) ? 0 : 1;
        timer.stop();
        Logger.DEBUG("Finished Halvade Preprocessing [runtime: " + timer.getFormattedElapsedTime() + "]");
        
        return ret;
    }
    
    private int processFiles() throws IOException, InterruptedException, URISyntaxException, Throwable {    
        Timer timer = new Timer();
        timer.start();
        
        AWSUploader upl = null;
        FileSystem fs = null;
        // write to s3?
        boolean useAWS = false;
        if(outputDir.startsWith("s3")) {
            useAWS = true;
            String existingBucketName = outputDir.replace("s3://","").split("/")[0];
            outputDir = outputDir.replace("s3://" + existingBucketName + "/", "");
            upl = new AWSUploader(existingBucketName, SSE, profile);
        } else {
            Configuration conf = getConf();
            fs = FileSystem.get(new URI(outputDir), conf);
            Path outpath = new Path(outputDir);
            if (fs.exists(outpath) && !fs.getFileStatus(outpath).isDirectory()) {
                Logger.DEBUG("please provide an output directory");
                return 1;
            }
        }
        
        FileReaderFactory factory = FileReaderFactory.getInstance(mthreads, fromHDFS);
        if(manifest != null) {
            Logger.DEBUG("reading input files from " + manifest);
            // read from file
            BufferedReader br = new BufferedReader(new FileReader(manifest)); 
            String line;
            while ((line = br.readLine()) != null) {
                String[] files = line.split("\t");
                if(files.length == 2) {
                    factory.addReader(files[0], files[1], false);
                } else if(files.length == 1) {
                    factory.addReader(files[0], null, isInterleaved);
                }
            }
        } else if (file1 != null && file2 != null) {
            Logger.DEBUG("Paired-end read input in 2 files.");    
            factory.addReader(file1, file2, false);            
        } else if (file1 != null) {
            if(isInterleaved) 
                Logger.DEBUG("Single-end read input in 1 files.");   
            else 
                Logger.DEBUG("Paired-end read input in 1 files.");     
            factory.addReader(file1, null, isInterleaved);   
        } else {
            Logger.DEBUG("Incorrect input, use either a manifest file or give both file1 and file2 as input.");     
        }
        
        // start reading
        (new Thread(factory)).start();
                
        int bestThreads = mthreads;
        long maxFileSize = getBestFileSize(); 
        if(useAWS) {
            AWSInterleaveFiles[] fileThreads = new AWSInterleaveFiles[bestThreads];
            // start interleaveFile threads
            for(int t = 0; t < bestThreads; t++) {
                fileThreads[t] = new AWSInterleaveFiles(
                        outputDir + "halvade_" + t + "_", 
                        maxFileSize, 
                        upl, t, codec, fromHDFS, maxNumLines);
                fileThreads[t].start();
            }
            for(int t = 0; t < bestThreads; t++)
                fileThreads[t].join();
            if(upl != null)
                upl.shutDownNow(); 
        } else {
            
            HDFSInterleaveFiles[] fileThreads = new HDFSInterleaveFiles[bestThreads];
            // start interleaveFile threads
            for(int t = 0; t < bestThreads; t++) {
                fileThreads[t] = new HDFSInterleaveFiles(
                        outputDir + "halvade_" + t + "_", 
                        maxFileSize, 
                        fs, t, codec, fromHDFS, maxNumLines);
                fileThreads[t].start();
            }
            for(int t = 0; t < bestThreads; t++)
                fileThreads[t].join();
        }
        factory.finalize();
        timer.stop();
        Logger.DEBUG("Time to process data: " + timer.getFormattedCurrentTime());     
        return 0;
    }
    
    private long getBestFileSize() {
        return bestFileSize;
    }
    
    
    public void createOptions() {
        Option optOut = OptionBuilder.withArgName( "output" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "Output directory on s3 (s3://bucketname/folder/) or HDFS (/dir/on/hdfs/)." )
                                .create( "O" );
        Option optFile1 = OptionBuilder.withArgName( "manifest/input1" )
                                .hasArg()
                                .isRequired(true)
                                .withDescription(  "The filename containing the input files to be put on S3/HDFS, must be .manifest. " + 
                                        "Or the first input file itself (fastq), '-' reads from stdin." )
                                .create( "1" );
        Option optFile2 = OptionBuilder.withArgName( "fastq2" )
                                .hasArg()
                                .withDescription(  "The second fastq file." )
                                .create( "2" );
        Option optSize = OptionBuilder.withArgName( "size" )
                                .hasArg()
                                .withDescription(  "Sets the maximum filesize of each split in MB." )
                                .withLongOpt("size")
                                .create( "s");
        Option optCov = OptionBuilder.withArgName( "maximumcoverage" )
                                .hasArg()
                                .withDescription(  "Sets the maximum coverage for this sample, if the number of reads is more than the maximum coverage, will downsample (by removing the last reads that are too much). Doesn't work with BAM inputs yet." )
                                .create( "cov" );
        Option optReadSize = OptionBuilder.withArgName( "readsize" )
                                .hasArg()
                                .withDescription(  "Size of the reads to calculate coverage [151]." )
                                .create( "readsize" );
        Option optRefSize = OptionBuilder.withArgName( "refsize" )
                                .hasArg()
                                .withDescription(  "Size of the reference to calculate coverage [3088286401]." )
                                .create( "refsize" );
        Option optThreads = OptionBuilder.withArgName( "threads" )
                                .hasArg()
                                .withDescription(  "Sets the available threads [1]." )
                                .create( "t" );
        Option optProfile = OptionBuilder.withArgName( "profilename" )
                                .hasArg()
                                .withDescription(  "Sets the profile name to be used when looking for AWS credentials in the credentials file (~/.aws/credentials). [default]" )
                                .withLongOpt("profile")
                                .create( "p" );
        Option optInter = OptionBuilder.withArgName( "" )
                                .withDescription(  "The single file input files contain interleaved paired-end reads." )
                                .create( "i" );
        Option optSnappy = OptionBuilder.withArgName( "" )
                                .withDescription(  "Compress the output files with snappy (faster) instead of gzip. The snappy library needs to be installed in Hadoop." )
                                .withLongOpt("snappy")
                                .create();
        Option optLz4 = OptionBuilder.withArgName( "" )
                                .withDescription(  "Compress the output files with lz4 (faster) instead of gzip. The lz4 library needs to be installed in Hadoop." )
                                .withLongOpt("lz4")
                                .create();
        Option optSSE = OptionBuilder.withArgName( "" )
                                .withDescription(  "Enables Server Side Encryption to transfer the files to amazon S3." )
                                .withLongOpt("sse")
                                .create();
        Option optHDFS = OptionBuilder.withArgName( "" )
                                .withDescription(  "The input data is stored on a dfs (HDFS, S3, etc) and needs to be accessed by hadoop API." )
                                .withLongOpt("dfs")
                                .create();
        Option optBam = OptionBuilder.withArgName( "" )
                                .withDescription(  "The input file given in the '-1' option is an (un)mapped bam file that needs to be remapped." )
                                .withLongOpt("bam")
                                .create();
        Option optRed = OptionBuilder.withArgName( "" )
                                .withDescription(  "Gives the number of splits the bam input file should be split in." )
                                .hasArg()
                                .withLongOpt("red")
                                .create();
        
        options.addOption(optOut);
        options.addOption(optFile1);
        options.addOption(optFile2);
        options.addOption(optThreads);
        options.addOption(optProfile);
        options.addOption(optSize);
        options.addOption(optInter);
        options.addOption(optSnappy);
        options.addOption(optLz4);
        options.addOption(optSSE);
        options.addOption(optHDFS);
        options.addOption(optBam);
        options.addOption(optRed);
        options.addOption(optReadSize);
        options.addOption(optCov);
        options.addOption(optRefSize);
    }
    
    public void parseArguments(String[] args) throws ParseException {
        createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(options, args);
        if(line.hasOption("bam"))
            BAMInput = true;
        manifest = line.getOptionValue("1");
        if(!manifest.endsWith(".manifest")) {
            file1 = manifest;
            manifest = null;
        }
        outputDir = line.getOptionValue("O");
        if(!outputDir.endsWith("/")) outputDir += "/";
        
        if (line.hasOption("2"))
            file2 = line.getOptionValue("2");   
        if (line.hasOption("profile"))
            profile = line.getOptionValue("profile");    
        if(line.hasOption("t"))
            mthreads = Integer.parseInt(line.getOptionValue("t"));
        if(line.hasOption("readsize"))
            readSize = Integer.parseInt(line.getOptionValue("readsize"));
        if(line.hasOption("refsize"))
            refSize = Integer.parseInt(line.getOptionValue("refsize"));
        if(line.hasOption("cov")) {
            maxCov = Integer.parseInt(line.getOptionValue("cov"));
            maxNumLines = (4*refSize*maxCov)/(readSize*mthreads);
        }
        if(line.hasOption("red"))
            red = Integer.parseInt(line.getOptionValue("red"));
        else if (BAMInput) {
            throw new ParseException("Option 'bam' requires the 'red' option.");
        }
        if(line.hasOption("i"))
            isInterleaved = true;
        if(line.hasOption("sse"))
            SSE = true;
        if(line.hasOption("dfs"))
            fromHDFS = true;
        if(line.hasOption("snappy")) {       
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(getConf());
            codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.SnappyCodec");
        }
        if(line.hasOption("lz4")) {       
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(getConf());
            codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.Lz4Codec");
        }
        if(codec != null)
            Logger.DEBUG("Hadoop encryption: " + codec.getDefaultExtension().substring(1));
        if(line.hasOption("size"))
            bestFileSize = Integer.parseInt(line.getOptionValue("size")) * 1024 * 1024;
    }

}
