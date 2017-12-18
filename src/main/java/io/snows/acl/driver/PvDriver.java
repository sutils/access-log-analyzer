package io.snows.acl.driver;

import com.google.common.base.Strings;
import io.snows.acl.PvAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PvDriver extends Configured implements Tool {
    private boolean verbose = true;

    public PvDriver(boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job;
        Configuration conf = getConf();
        //
        job = Job.getInstance(conf);
        job.setJobName("Access");
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        PvAnalyzer.setupAccessJob(job);
        if (!job.waitForCompletion(verbose)) {
            return 1;
        }
        //
        job = Job.getInstance(conf);
        job.setJobName("Pv");
        FileInputFormat.addInputPath(job, new Path(args[1] + "/IpDate-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        PvAnalyzer.setupPvJob(job);
        if (!job.waitForCompletion(verbose)) {
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        String ips = System.getProperty("IP_LOC_SRV");
        if (!Strings.isNullOrEmpty(ips)) {
            PvAnalyzer.IpLocServer = ips;
        }
        int exitCode = ToolRunner.run(new PvDriver(false), args);
        System.exit(exitCode);
    }
}
