/**
 * Operator Paired Mapper for mapping paired reads to a
 * reference genome (path supplied by properties.xml)
 *
 *  Pre-Requisites
 * -> Reference Genome need to be indexed using BWA. (BWA -index ${PATH_NAME})
 * -> Set Path of Reference Genome and the FASTQs in properties.xml
 *
 *  Tasks:
 * -> 1. Loads reference from path suppplied.
 * -> 2. Loads two FASTQs (Paired Reads).
 * -> 3. Begin Mapping to reference.
 *
 * Time duration LOGS available in dt.log
 * Logs and messages from native library can be found in std.err
 *
 * Java Bindings (JNI) for BWA by Dr. Pierre Lindenbaum
 * https://github.com/lindenb/jbwa
 */

package com.Paired_Fastq_Mapper;


import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.KSeq;
import com.github.lindenb.jbwa.jni.ShortRead;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PairedMapper extends BaseOperator implements InputOperator
{

    private BwaIndex index;
    private BwaMem mem;
    private KSeq fastq_1;
    private KSeq fastq_2;
    private ShortRead read_1;
    private ShortRead read_2;
    private String refName;
    private Boolean isValidRefPath = true;
    private Boolean isValidReadPath_1 = true;
    private Boolean isValidReadPath_2 = true;
    private Boolean readStartFlag = false;
    @NotNull
    private String refFilePath;
    @NotNull
    private String fastqPath_1 = "/home/plaban/Test/Input/Small_FASTQ/R1.fq";
    @NotNull
    private String fastqPath_2 = "/home/plaban/Test/Input/Small_FASTQ/R2.fq";

    private Date app_start;
    private Date app_stop;
    private Date ref_start;
    private Date ref_stop;
    private Date map_start;
    private Date map_stop;

    List<ShortRead> List_1 = new ArrayList<ShortRead>();
    List<ShortRead> List_2 = new ArrayList<ShortRead>();

    private static final Logger LOG = LoggerFactory.getLogger(PairedMapper.class);

    public final transient DefaultOutputPort<String> outputPort= new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
        try
        {
            read_1 = fastq_1.next();
            read_2 = fastq_2.next();

            if (read_1 == null || read_2 == null || List_1.size() > 100)
            {
                if ((read_1 == null || read_2 == null) && readStartFlag)
                    teardown();

                if (!List_1.isEmpty())
                {
                    readStartFlag = true;

                    for (String sam : mem.align(List_1, List_2))
                        outputPort.emit(sam);
                }

                List_1.clear();
                List_2.clear();
            }

            List_1.add(read_1);
            List_2.add(read_2);
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
        super.setup(context);

        //JNI for BWA
        System.load("/home/plaban/Downloads/jbwa-master/src/main/native/libbwajni.so");

        app_start = new Date();

        try
        {
            LOG.info("Loading Reference Genome and its Indices......");

            ref_start = new Date();
            File refFile = new File(getRefFilePath());
            setRefName(refFile.getName());

            if (refFile.isFile())
            {
                index = new BwaIndex(refFile);
                mem = new BwaMem(index);

                ref_stop = new Date();

                LOG.info("Time elapsed in loading genome: " + (ref_stop.getTime() - ref_start.getTime()) + "ms");
            }

            else
            {
                isValidRefPath = false;
                LOG.error("Error!! Invalid Path: " + getRefFilePath());
            }

            LOG.info("Loading 1st FASTQ File....");
            File fastqFile_1 = new File(getFastqPath_1());

            if (fastqFile_1.isFile())
            {
                fastq_1 = new KSeq(fastqFile_1);
                LOG.info("Loaded 1st FASTQ File.....");
            }

            else
                isValidReadPath_1 = false;

            LOG.info("Loading 2nd FASTQ File....");
            File fastqFile_2 = new File(getFastqPath_2());

            if (fastqFile_2.isFile())
            {
                fastq_2 = new KSeq(fastqFile_2);
                LOG.info("Loaded 2nd FASTQ File.....");
            }

            else
                isValidReadPath_2 = false;

            LOG.info("Beginning map to reference genome... " + getRefName());

            /*BufferedReader reader = new BufferedReader(new FileReader(refFile));
            refName = reader.readLine();

            if (refName.charAt(0) == '>')
                refName = refName.substring( 1, refName.length());*/
        }

        catch (IOException e)
        {
            e.printStackTrace();
        }

        map_start = new Date();
    }

    public String getRefFilePath()
    {
        return refFilePath;
    }

    public void setRefFilePath(String path_ref)
    {
        this.refFilePath = path_ref;
    }

    public String getRefName()
    {
        return refName;
    }

    public void setRefName(String refName)
    {
        this.refName = refName;
    }

    public String getFastqPath_1()
    {
        return fastqPath_1;
    }

    public void setFastq_1(String path_1)
    {
        this.fastqPath_1 = path_1;
    }

    public String getFastqPath_2()
    {
        return fastqPath_2;
    }

    public void setFastqPath_2(String path_2)
    {
        this.fastqPath_2 = path_2;
    }

    @Override
    public void teardown()
    {
        super.teardown();

        app_stop = new Date();
        map_stop = new Date();

        if (isValidRefPath)
            LOG.info("Teardown of Mapping operation......... Started");

        fastq_1.dispose();
        fastq_2.dispose();
        index.close();
        mem.dispose();

        if (isValidRefPath && isValidReadPath_1 && isValidReadPath_2)
        {
            LOG.info("Teardown of Mapping operation......... Finished");
            LOG.info("**********************SUMMARY**********************");
            LOG.info("Time elapsed in loading genome: " + (ref_stop.getTime() - ref_start.getTime()) + "ms");
            LOG.info("Time elapsed in mapping reads to genome: " + (map_stop.getTime() - map_start.getTime()) + "ms");
            LOG.info("Total time taken by the Operator: " + (app_stop.getTime() - app_start.getTime()) + "ms");
        }

        else
        {
            if (!isValidRefPath)
                LOG.info("Mapping operation unsuccessful........ File not found at " + getRefFilePath());

            else if (!isValidReadPath_1)
                LOG.info("Read unsuccessful for FASTQ file no. 1...... File not found at " + getFastqPath_1());

            else
                LOG.info("Read unsuccessful for FASTQ file no. 2...... File not found at " + getFastqPath_2());
        }
    }
}
//End_Of_Code