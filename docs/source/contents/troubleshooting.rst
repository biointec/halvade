Troubleshooting
================

Find the logs to solve Halvade errors
-------------------------------------

If Halvade doesn't finish due to an error, the error itself is printed in the output of the Hadoop command. However, more information can be found in the individual task stderr logs of the MapReduce job. The location of these log files is set in the MapReduce settings. Typically these are stored at `${yarn.log.dir}/userlogs` or if the `YARN_LOG_DIR` environment is set under `$YARN_LOG_DIR/userlogs`. It's highly likely that all reduce tasks give a similar result, so look at the stderr log of any reduce task. 
This log will show where Halvade is running into problems. If it isn't clear from the log, try to run the last command, with the error, manually. The exact command should be printed in the log as an array of strings, run this command with the shown option.


Halvade with BAM input seems stuck at the MarkDuplicates step
-------------------------------------------------------------

If the stderr log of a reduce task shows it started the MarkDuplicates file but didn't finish in a considerable time. Then it is highly likely it is finding incorrect aligned reads and giving error messages that slow the process to the point where it seems stuck. If this is the case, look at the header file of the BAM file and the fasta dictionary file, if the contig order is different then this is the source of the problem. Halvade assigns the contig by index in the sequence dictionary when reading the bam files and this causes wrong matching between steps. To fix this, the sequence dictionary of the bam files needs to be reordered the same as the dict file. This can be done with the ReorderSam tool by Picard like this:

.. code-block:: bash
	:linenos:

	java -jar picard.jar ReorderSam I=input.bam O=output.bam R=reference.fasta

Use the new file as input and Halvade should run without any problems now.
