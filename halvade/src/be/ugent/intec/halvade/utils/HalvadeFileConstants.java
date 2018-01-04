/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.utils;

/**
 *
 * @author ddecap
 */
public class HalvadeFileConstants {
    
    public static final String FASTA_SUFFIX = ".fasta";
    public static final String FA_SUFFIX = ".fa";
    public static final String DICT_SUFFIX = ".dict";
    public static final String DFS = "org.apache.hadoop.hdfs.DistributedFileSystem";
    public static final String LOCALFS = "org.apache.hadoop.fs.LocalFileSystem";
    
    public static final String REF_LOCK = "down_ref.lock";
    public static final String GFF_LOCK = "down_gff.lock";
    public static final String STARG_LOCK = "down_starg.lock";
    public static final String STARG2_LOCK = "down_starg2.lock";
    public static final String DBSNP_LOCK = "down_snpdb.lock";
    
    
    // should not be necessary anymore!!
//    public static final String HALVADE_BWA_SUFFIX = ".bwa_ref";
//    public static final String HALVADE_BOWTIE2_SUFFIX = ".bowtie2_ref";
//    public static final String HALVADE_CUSHAW2_SUFFIX = ".cushaw2_ref";
//    public static final String HALVADE_GATK_SUFFIX = ".gatk_ref";
//    public static final String HALVADE_STAR_SUFFIX_P1 = ".star_ref";
//    public static final String HALVADE_STAR_SUFFIX_P2 = ".star_ref_p2";
//    public static final String HALVADE_DBSNP_SUFFIX = ".dbsnp";
    
    public static final String[] BWA_REF_FILES = 
        {".fasta", ".fasta.amb", ".fasta.ann", ".fasta.bwt", ".fasta.pac", ".fasta.sa"}; 
    public static final String[] BOWTIE2_REF_FILES = 
        {".fasta", ".fasta.1.bt2",".fasta.2.bt2",".fasta.3.bt2",".fasta.4.bt2",".fasta.rev.1.bt2",".fasta.rev.2.bt2"}; 
    public static final String[] CUSHAW2_REF_FILES = 
        {".fasta", ".fasta.amb", ".fasta.ann", ".fasta.pac", ".fasta.rbwt", ".fasta.rpac", ".fasta.rsa" }; 
    public static final String[] GATK_REF_FILES =  {".fasta", ".fasta.fai", ".dict" }; 
    public static final String[] STAR_REF_FILES = 
        {"chrLength.txt", "chrNameLength.txt", "chrName.txt", "chrStart.txt", 
         "Genome", "genomeParameters.txt", "SA", "SAindex"};
    public static final String[] STAR_REF_OPTIONAL_FILES =  {"sjdbInfo.txt", "sjdbList.out.tab"};
    
    public static final String[][] DNA_ALN_REF_FILES = {BWA_REF_FILES, BWA_REF_FILES, BOWTIE2_REF_FILES, CUSHAW2_REF_FILES};
    
}
