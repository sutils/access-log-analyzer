package io.snows.acl;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PvAnalyzerTest {
    Configuration conf;

    @Before
    public void setUp() throws Exception {
        this.conf = new Configuration();
        this.conf.addResource(new Path("/tmp/hadoop.xml"));
    }

    @Test
    public void testPvAnalyzer() throws Exception {
        Job job;
        String pwd = System.getProperty("user.dir");
        //
        //
//        String input = "file://" + pwd + "/data/access.log";
//        String output = "file://" + pwd + "/out/xxx";
//        FileUtils.deleteDirectory(new File(pwd + "/out/xxx"));
//        job = Job.getInstance(conf);
//        job.setJobName("Access");
////        FileInputFormat.setInputDirRecursive(job, true);
//        FileInputFormat.addInputPath(job, new Path(input));
//        FileOutputFormat.setOutputPath(job, new Path(output));
//        PvAnalyzer.setupAccessJob(job);
//        job.waitForCompletion(true);
        //
        //
        String input2 = "file://" + pwd + "/out/xxx/IpDate-r-00000";
        String output2 = "file://" + pwd + "/out/xxx2";
        FileUtils.deleteDirectory(new File(pwd + "/out/xxx2"));
        job = Job.getInstance(conf);
        job.setJobName("Pv");
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output2));
        PvAnalyzer.setupPvJob(job);
        job.waitForCompletion(true);
    }
}
