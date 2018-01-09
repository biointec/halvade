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

package be.ugent.intec.halvade.utils;

import be.ugent.intec.halvade.hadoop.mapreduce.HalvadeCounters;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 * @author ddecap
 */
public class HalvadeFileUtils {
    protected static final int RETRIES = 3;

    public static String Unzip(String inFilePath) throws IOException  {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inFilePath));

        String outFilePath = inFilePath.replace(".gz", "");
        OutputStream out = new FileOutputStream(outFilePath);

        byte[] buf = new byte[64*1024];
        int len;
        while ((len = gzipInputStream.read(buf)) > 0)
            out.write(buf, 0, len);

        gzipInputStream.close();
        out.close();

        new File(inFilePath).delete();

        return outFilePath;
    }
        
    protected static boolean checkCrc(FileSystem fs, String _file) throws IOException {
        Path file = new Path(_file);
        Path crc = new Path(file.getParent() + "/." + file.getName() + ".crc");
        return checkCrc(fs, file, crc);
    }
    
    protected static boolean checkCrc(FileSystem fs, Path file, Path crc) throws IOException {
        long startTime = System.currentTimeMillis();
        byte[] buf = new byte[512*1024*1024];
        long len = fs.getFileStatus(file).getLen();
        long pos = 0;
        Boolean gotException = false;
        FSDataInputStream in = fs.open(file); 
        try { 
            int read = in.read(pos, buf, 0, buf.length);
            pos += read;
            while( pos < len ) {
                read = in.read(pos, buf, 0, buf.length);
                pos += read;
            }
//          IOUtils.readFully(in, buf, 0, buf.length);  // cant process more than 2gb files...
        } catch (ChecksumException e) {
            gotException = true; 
        }   
        Logger.DEBUG("checksum of " + file + " is " + (gotException ? "incorrect, needs to be redownloaded" : "correct"));
        in.close(); 
        long estimatedTime = System.currentTimeMillis() - startTime;
        Logger.DEBUG("crc check time: " + estimatedTime +"ms");
        return !gotException;
        // real check is 52m 16s (chr1)
        // just return true here is  (crh1)
    }
        
    public static int downloadFileWithLock(FileSystem fs, HalvadeFileLock lock, String from, String to, Configuration conf) throws IOException, InterruptedException, URISyntaxException {
        Logger.DEBUG("downloading from " + from + " to " + to);
        try {
            Path toPath = new Path(to);
            Path fromPath = new Path(from);     
            File f = new File(to);
            lock.getLock(); 
            if(!f.exists()) {
                fs.copyToLocalFile(new Path(from), toPath);
            } else {
                // check if crc file exists?
                FileSystem lfs = FileSystem.get(new URI("file:///"), conf);
                if(!checkCrc(lfs, to)) {
                    f.delete();
                    Logger.DEBUG("redownloading...");
                    fs.copyToLocalFile(fromPath, toPath);
                } else {
                }
            }
            return 0;
        } catch (IOException | URISyntaxException ex) {
            Logger.EXCEPTION(ex);
            lock.removeAndReleaseLock();
            throw ex;
        } finally {
            lock.removeAndReleaseLock();
        }
    }
    
    public static int downloadFileFromHDFS(FileSystem fs, String from, String to, Configuration conf) throws IOException, URISyntaxException {
        Logger.DEBUG("downloading from " + from + " to " + to);
        Path toPath = new Path(to);
        Path fromPath = new Path(from);     
        File f = new File(to);
        if(!f.exists()) {
            fs.copyToLocalFile(new Path(from), toPath);
        } else {
            // check if crc file exists?
            FileSystem lfs = FileSystem.get(new URI("file:///"), conf);
            if(!checkCrc(lfs, to)) {
                f.delete();
                Logger.DEBUG("redownloading...");
                fs.copyToLocalFile(fromPath, toPath);
            } else {
            }
        }
        return 0;
    }
        
    public static int uploadFileToHDFS(FileSystem fs, String from, String to) throws IOException {  
        Logger.DEBUG("uploading from " + from + " to " + to);
        Path toPath = new Path(to);
        Path fromPath = new Path(from);     
        File f = new File(to);
        if(!f.exists()) {
            fs.copyFromLocalFile(new Path(from), toPath);
        } else {
            // check if crc file exists?
            if(!checkCrc(fs, to)) {
                f.delete();
                Logger.DEBUG("reuploading...");
                fs.copyFromLocalFile(fromPath, toPath);
            } else {
            }
        }
        return 0;
    }
    
    public static Boolean checkReferenceFilesAvailable(Configuration conf) throws IOException, URISyntaxException {
        String ref = HalvadeConf.getRef(conf);
        Boolean rnaPipeline = HalvadeConf.getIsRNA(conf);
        int aln = HalvadeConf.getAligner(conf);
        FileSystem fs = FileSystem.get(new URI(ref), conf);
        String suffix = ref.endsWith(HalvadeFileConstants.FASTA_SUFFIX) ? HalvadeFileConstants.FASTA_SUFFIX : HalvadeFileConstants.FA_SUFFIX;
        String missing_files = "";
        String[] stepOneRequiredFiles = (rnaPipeline ? HalvadeFileConstants.STAR_REF_FILES : HalvadeFileConstants.DNA_ALN_REF_FILES[aln]); 
        String[] stepTwoRequiredFiles = HalvadeFileConstants.GATK_REF_FILES;
        for (int i = 0; i < stepOneRequiredFiles.length; i++) {
            String newsuffix = stepOneRequiredFiles[i].replace(HalvadeFileConstants.FASTA_SUFFIX, suffix);
            String newfile = ref.replace(suffix, newsuffix);
            Boolean exists = fs.isFile(new Path(newfile));
            if (!exists) {
                Logger.DEBUG("gatk ref file [" + i + "] " + newfile + " is missing"); 
                missing_files += "<ref_name>" + newsuffix + " ";
            } // check should be done at a later stage, like if copy from hdfs to local scratch, else we assume its correct!
//            else {
//                Boolean check = HalvadeFileUtils.checkChecksum(new FileInputStream(newfile), fs.open(new Path(newfile)));
//                Logger.DEBUG("gatk ref file [" + i + "] " + newfile + " is found and checksum is " + (check ? "correct" : "incorrect"));
//            }
        }
        for (int i = 0; i < stepTwoRequiredFiles.length; i++) {
            String newsuffix = stepTwoRequiredFiles[i].replace(HalvadeFileConstants.FASTA_SUFFIX, suffix);
            String newfile = ref.replace(suffix, newsuffix);
            Boolean exists = fs.isFile(new Path(newfile)); 
            if (!exists) {
                Logger.DEBUG("gatk ref file [" + i + "] " + newfile + " is missing"); 
                missing_files += "<ref_name>" + newsuffix + " ";
            } 
//            else {
//                Boolean check = HalvadeFileUtils.checkChecksum(new FileInputStream(newfile), fs.open(new Path(newfile)));
//                Logger.DEBUG("gatk ref file [" + i + "] " + newfile + " is found and checksum is " + (check ? "correct" : "incorrect"));
//            }
        }
        if(!missing_files.equals("")) {
            throw new IOException("missing files: " + missing_files);
        } else {
            Logger.DEBUG("All reference files are available"); 
        }
        return true; // returns true or exception...
    }
    
    public static String downloadGFF(TaskInputOutputContext context) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String gff = HalvadeConf.getGff(context.getConfiguration());  
        if(gff == null) 
            return null;
        Boolean refIsLocal = HalvadeConf.getRefIsLocal(context.getConfiguration()); 
        if(refIsLocal) 
            return gff;
        String refDir = HalvadeConf.getScratchTempDir(conf);  
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        String gffSuffix = null;
        int si = gff.lastIndexOf('.');
        if (si > 0)
            gffSuffix = gff.substring(si);
        else 
            throw new InterruptedException("Illegal filename for gff file: " + gff);
        Logger.DEBUG("suffix: " + gffSuffix);
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.GFF_LOCK);
        String filebase = gff.substring(gff.lastIndexOf("/")+1).replace(gffSuffix, "");
        
        
        FileSystem fs = FileSystem.get(new URI(gff), conf);
        downloadFileWithLock(fs, lock, gff, refDir + filebase + gffSuffix, context.getConfiguration()); 
        return refDir + filebase + gffSuffix;
    }

    public static String downloadBWAIndex(TaskInputOutputContext context) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, HalvadeFileConstants.BWA_REF_FILES);
    }   
    public static String downloadBowtie2Index(TaskInputOutputContext context) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, HalvadeFileConstants.BOWTIE2_REF_FILES);
    }   
    public static String downloadCushaw2Index(TaskInputOutputContext context) throws IOException, URISyntaxException {
        return downloadAlignerIndex(context, HalvadeFileConstants.CUSHAW2_REF_FILES);
    }
    
    // function for local testing!!! do not use!
    public static String downloadAlignerIndex(Configuration conf, String[] refFiles) throws IOException, URISyntaxException {
        Boolean refIsLocal = false;  
        String ref = HalvadeConf.getRef(conf);
        if(refIsLocal) 
            return ref;
        String HDFSRef = ref;
        String refDir = HalvadeConf.getScratchTempDir(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.REF_LOCK);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        String suffix = HDFSRef.endsWith(HalvadeFileConstants.FASTA_SUFFIX) ? HalvadeFileConstants.FASTA_SUFFIX : HalvadeFileConstants.FA_SUFFIX;
        String filebase = HDFSRef.substring(HDFSRef.lastIndexOf("/")+1).replace(suffix, "");
        
        try {
            for (String filesuffix : refFiles) { //  refFiles = BWA_REF_FILES  
                String newsuffix = filesuffix.replace(HalvadeFileConstants.FASTA_SUFFIX, suffix);
                String newfile = HDFSRef.replace(suffix, newsuffix);
                downloadFileWithLock(fs, lock, newfile, refDir + filebase + newsuffix, conf);               
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.removeAndReleaseLock();
        }
        Logger.DEBUG("local fasta reference: " + refDir + filebase + suffix);
        return refDir + filebase + suffix; 
    } 
    
    protected static String downloadAlignerIndex(TaskInputOutputContext context, String[] refFiles) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        Boolean refIsLocal = HalvadeConf.getRefIsLocal(context.getConfiguration()); 
        String ref = HalvadeConf.getRef(conf);
        if(refIsLocal) 
            return ref;
        String HDFSRef = ref;
        String refDir = HalvadeConf.getScratchTempDir(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.REF_LOCK);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        String suffix = HDFSRef.endsWith(HalvadeFileConstants.FASTA_SUFFIX) ? HalvadeFileConstants.FASTA_SUFFIX : HalvadeFileConstants.FA_SUFFIX;
        String filebase = HDFSRef.substring(HDFSRef.lastIndexOf("/")+1).replace(suffix, "");
        try {
            for (String filesuffix : refFiles) { 
                String newsuffix = filesuffix.replace(HalvadeFileConstants.FASTA_SUFFIX, suffix);
                String newfile = HDFSRef.replace(suffix, newsuffix);
                downloadFileWithLock(fs, lock, newfile, refDir + filebase + newsuffix, context.getConfiguration());          
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.removeAndReleaseLock();
        }
        Logger.DEBUG("local fasta reference: " + refDir + filebase + suffix);
        return refDir + filebase + suffix; 
    }
    
    public static String downloadGATKIndex(TaskInputOutputContext context) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        Boolean refIsLocal = HalvadeConf.getRefIsLocal(context.getConfiguration()); 
        String ref = HalvadeConf.getRef(conf);
        if(refIsLocal) 
            return ref;
        String HDFSRef = ref;
        String refDir = HalvadeConf.getScratchTempDir(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/";
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.REF_LOCK);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        String suffix = HDFSRef.endsWith(HalvadeFileConstants.FASTA_SUFFIX) ? HalvadeFileConstants.FASTA_SUFFIX : HalvadeFileConstants.FA_SUFFIX;
        String filebase = HDFSRef.substring(HDFSRef.lastIndexOf("/")+1).replace(suffix, "");
        try {
            for (String filesuffix : HalvadeFileConstants.GATK_REF_FILES) {
                String newsuffix = filesuffix.replace(HalvadeFileConstants.FASTA_SUFFIX, suffix);
                String newfile = HDFSRef.replace(suffix, newsuffix);
                downloadFileWithLock(fs, lock, newfile, refDir + filebase + newsuffix, context.getConfiguration());       
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.removeAndReleaseLock();
        }
        Logger.DEBUG("local fasta reference: " + refDir + filebase + suffix);
        return refDir + filebase + suffix;
    }
    
    public static String downloadSTARIndex(TaskInputOutputContext context, String id, boolean usePass2Genome) throws IOException, URISyntaxException {
        Configuration conf = context.getConfiguration();
        Boolean refIsLocal = HalvadeConf.getRefIsLocal(context.getConfiguration()); 
        String ref = usePass2Genome ? HalvadeConf.getStarDirPass2HDFS(conf) : HalvadeConf.getStarDirOnHDFS(conf); // TODO check if this is ok??
        if(refIsLocal) // for both pass 1 and pass 2!
            return ref;
        String HDFSRef = ref;
        String refDir = HalvadeConf.getScratchTempDir(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/"; // should already be with a / ...
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.REF_LOCK);
        FileSystem fs = FileSystem.get(new URI(HDFSRef), conf);
        String filebase = HalvadeConf.getPass2UID(conf);
        
        // download all files in folder and make sure its 
        try {
            for (String file : HalvadeFileConstants.STAR_REF_FILES) {
                String newfile = HDFSRef + file;
                downloadFileWithLock(fs, lock, newfile, refDir + filebase + newfile, context.getConfiguration());                
            }
            for (String file : HalvadeFileConstants.STAR_REF_OPTIONAL_FILES) {
                String newfile = HDFSRef + file;
                if(fs.exists(new Path(newfile))) 
                    downloadFileWithLock(fs, lock, newfile, refDir + filebase + newfile, context.getConfiguration());
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.removeAndReleaseLock();
        }
        Logger.DEBUG("local star reference: " + refDir + filebase);
        return refDir + filebase;
    }
    
    public static String[] downloadSites(TaskInputOutputContext context, String id) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Boolean refIsLocal = HalvadeConf.getRefIsLocal(context.getConfiguration()); 
        String sites[] = HalvadeConf.getKnownSitesOnHDFS(conf);
        if(refIsLocal || sites == null || sites.length == 0)
            return sites;
        String HDFSsites[] = sites;
        String localSites[] = new String[sites.length];
        String refDir = HalvadeConf.getScratchTempDir(conf);
        if(!refDir.endsWith("/")) refDir = refDir + "/"; 
        HalvadeFileLock lock = new HalvadeFileLock(refDir, HalvadeFileConstants.REF_LOCK);
        FileSystem fs = FileSystem.get(new URI(sites[0]), conf);
        
        try {
            for (int i= 0; i < HDFSsites.length; i++) {
                String hdfssite = HDFSsites[i];
                String name = hdfssite.substring(hdfssite.lastIndexOf('/') + 1);
                downloadFileWithLock(fs, lock, hdfssite, refDir + name, context.getConfiguration());
                localSites[i] = refDir + name;
                // attempt to download .idx file
                if(fs.exists(new Path(hdfssite + ".idx")))
                    downloadFileWithLock(fs, lock, hdfssite + ".idx", refDir + name + ".idx", context.getConfiguration());
            }
            
        } catch (InterruptedException ex) {
            Logger.EXCEPTION(ex);
        } finally {
            lock.removeAndReleaseLock();
        }
        Logger.DEBUG("local sires:");
        for (String site: localSites) {
            Logger.DEBUG(site);
        }
        return localSites;
    }
    
    
    public static boolean removeLocalFile(String filename) {
        return removeLocalFile(false, filename);
    }
    public static boolean removeLocalFile(boolean keep, String filename) {
        if(keep) return false;
        File f = new File(filename);
        long size = f.length();
        boolean res = f.exists() && f.delete();
        Logger.DEBUG((res ? "successfully deleted ": "failed to delete ") + "\"" + filename + "\" [" + (size / 1024 / 1024)+ "Mb]");
        return res;
    }
    
    public static boolean removeLocalFile(String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        return removeLocalFile(false, filename, context, counter);
    }
    
    public static boolean removeLocalFile(boolean keep, String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        if(keep) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
        long size = f.length();
        boolean res = f.exists() && f.delete();
        Logger.DEBUG((res ? "successfully deleted ": "failed to delete ") + "\"" + filename + "\" [" + (size / 1024 / 1024)+ "Mb]");
        return res;
    }
    
    public static boolean removeLocalDir(String filename) {
        return removeLocalDir(false, filename);
    }
    public static boolean removeLocalDir(boolean keep, String filename) {
        if(keep) return false;
        File f = new File(filename);
//        long size = getFolderSize(f);
        boolean res = f.exists() && deleteDir(f);
        Logger.DEBUG((res ? "successfully deleted ": "failed to delete ") + "\"" + filename + "\""); // [" + (size / 1024 / 1024)+ "Mb]");
        return res; 
    }
    public static  boolean removeLocalDir(String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        return removeLocalDir(false, filename, context, counter);
    } 
    public static  boolean removeLocalDir(boolean keep, String filename, TaskInputOutputContext context, HalvadeCounters counter) {
        if(keep) return false;
        File f = new File(filename);
        if(f.exists()) context.getCounter(counter).increment(f.length());
//        long size = getFolderSize(f);
        boolean res = f.exists() && deleteDir(f);
        Logger.DEBUG((res ? "successfully deleted ": "failed to delete ") + "\"" + filename + "\"");// [" + (size / 1024 / 1024)+ "Mb]");
        return res; 
    } 
    	
    public static long getFolderSize(String dir) {
        File d = new File(dir);
        return getFolderSize(d);
    }
    protected static long getFolderSize(File dir) {
        long size = 0;
        if(!dir.isDirectory()) 
            size = dir.length();
        else {
            File[] list = dir.listFiles();
            if(list == null) return 0;
            for (File file : list) {
                if (file.isFile()) {
                    System.out.println(file.getName() + " " + file.length());
                    size += file.length();
                } else
                    size += getFolderSize(file);
            }
        }
        return size;
    }
    protected static  boolean deleteDir(File dir) {
        // check tmp dir filesize:
    	if(dir.exists()) {
    		File[] files = dir.listFiles();
    		if(files != null) {
    			for ( int i = 0; i < files.length; i++) {
    				if(files[i].isDirectory()) 
    					deleteDir(files[i]);
    				else
    					files[i].delete();
    			}
    		}
    	}
    	return dir.delete();
    }
}
