/**
 * DAG logic for Paired FASTQ_Mapper
 */
package com.Paired_Fastq_Mapper;

import com.sun.tools.javah.Gen;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="Paired_FASTQ_Mapper")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    //Operators
    //Reads two Paired-End FASTQ files (input path to be set in properties.xml)
    //Loads a Reference Genome file(FASTA) and maps FASTQ reads to the reference Genome
    PairedMapper pairedMapper = dag.addOperator("Paired_FASTQ_Mapper", PairedMapper.class);

    //Writes the mapped data as SAM file (output directory path and and file name tobe set in properties.xml)
    GenericFileOutputOperator.StringFileOutputOperator samWriter = dag.addOperator("SAM_Writer", GenericFileOutputOperator.StringFileOutputOperator.class);

    //Streams
    dag.addStream("to_SAM_Writer", pairedMapper.outputPort, samWriter.input);
  }
}
