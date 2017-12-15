package io.snows.acl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PvAnalyzer {

    public static class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static Logger log = LoggerFactory.getLogger(PvMapper.class);
        static Pattern MSIE = Pattern.compile("MSIE\\s*[0-9]*\\.[0-9]*");
        static Pattern WINDOWS = Pattern.compile("Windows\\s*NT\\s*[0-9]*\\.[0-9]*");
        static Pattern ANDROID = Pattern.compile("Android\\s*[0-9]*\\.[0-9]*");
        //
        SimpleDateFormat fullFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss z");
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy");
        SimpleDateFormat monthFormat = new SimpleDateFormat("MMM/yyyy");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" "$http_x_forwarded_for"
            String line = value.toString();
            try {
                String[] parts;
                //get the remote address
                parts = line.split("-", 2);
                if (parts.length < 2) {
                    return;
                }
                String remoteAddr = parts[0].trim();
                //get the time
                parts = parts[1].split("[\\[\\]]", 3);
                if (parts.length < 3) {
                    return;
                }

                Date time = fullFormat.parse(parts[1]);
                //get the path
                parts = parts[2].split("[\" ]", 4);
                if (parts.length < 4) {
                    return;
                }
                String path = parts[3].split("[# \\?]", 2)[0];
                if (!path.matches("/usr/.*") && !path.matches("/pub/.*")) {
                    path = "/other";
                }
                //get user agent
                parts = parts[3].split("\"", 5);
                if (parts.length < 5) {
                    return;
                }
                String agent = parts[4];
                //
                //write IP-Date
                context.write(new Text("IpvDate\t" + remoteAddr + "\t" + dateFormat.format(time)), new IntWritable(1));
                //write Path-Date
                context.write(new Text("ApiDate\t" + path + "\t" + dateFormat.format(time)), new IntWritable(1));
                //write IP-Month
                context.write(new Text("IpvMon\t" + remoteAddr + "\t" + monthFormat.format(time)), new IntWritable(1));
                //write Path-Month
                context.write(new Text("ApiMon\t" + path + "\t" + monthFormat.format(time)), new IntWritable(1));
                //write browser
                if (agent.contains("Opera")) {
                    context.write(new Text("Browser\tOpera"), new IntWritable(1));
                } else if (agent.contains("Firefox")) {
                    context.write(new Text("Browser\tFirefox"), new IntWritable(1));
                } else if (agent.contains("Chrome")) {
                    context.write(new Text("Browser\tChrome"), new IntWritable(1));
                } else if (agent.contains("Safari")) {
                    context.write(new Text("Browser\tSafari"), new IntWritable(1));
                } else if (agent.contains("MSIE")) {
                    Matcher m = MSIE.matcher(agent);
                    if (m.find()) {
                        String ie = m.group().trim();
                        context.write(new Text("Browser\t" + ie), new IntWritable(1));
                    } else {
                        log.warn("process ie agent fail by line:{}", line);
                    }
                } else if (agent.contains("SVN")) {
                    context.write(new Text("Browser\tSVN"), new IntWritable(1));
                } else {
                    context.write(new Text("Browser\tUnknown"), new IntWritable(1));
                }
                //write system
                if (agent.contains("Macintosh")) {
                    context.write(new Text("System\tMacintosh"), new IntWritable(1));
                } else if (agent.contains("iPhone")) {
                    context.write(new Text("System\tiPhone"), new IntWritable(1));
                } else if (agent.contains("iPad")) {
                    context.write(new Text("System\tiPad"), new IntWritable(1));
                } else if (agent.contains("Windows")) {
                    Matcher m = WINDOWS.matcher(agent);
                    if (m.find()) {
                        String window = m.group().trim();
                        context.write(new Text("System\t" + window), new IntWritable(1));
                    } else {
                        log.warn("process window agent fail by line:{}", line);
                    }
                } else if (agent.contains("Android")) {
                    Matcher m = ANDROID.matcher(agent);
                    if (m.find()) {
                        String android = m.group().trim();
                        context.write(new Text("System\t" + android), new IntWritable(1));
                    } else {
                        log.warn("process android agent fail by line:{}", line);
                    }
                }else if (agent.contains("SVN")) {
                    context.write(new Text("System\tSVN"), new IntWritable(1));
                } else {
                    context.write(new Text("System\tUnknown"), new IntWritable(1));
                }
            } catch (Exception e) {
                log.error("process fail by line:{}", line, e);
            }
        }
    }


    public static class PvReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        MultipleOutputs<Text, LongWritable> outs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            String parts[] = key.toString().split("\t", 2);
            outs.write(parts[0], new Text(parts[1]), new LongWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outs.close();
        }
    }

    public static void setup(Job job) {
        MultipleOutputs.addNamedOutput(job, "IpvDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "IpvMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "ApiDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "ApiMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "System", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "Browser", TextOutputFormat.class, Text.class, LongWritable.class);
        job.setJarByClass(PvMapper.class);
        job.setMapperClass(PvMapper.class);
        job.setReducerClass(PvReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }
}
