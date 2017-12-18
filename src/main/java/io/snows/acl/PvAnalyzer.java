package io.snows.acl;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PvAnalyzer {
    private static Logger log = LoggerFactory.getLogger(PvAnalyzer.class);
    static SimpleDateFormat fullFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss z");
    static SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy");
    static SimpleDateFormat monthFormat = new SimpleDateFormat("MMM/yyyy");

    public static String IpLocServer = "http://localhost:2774/getIpInfo?ip=%s";

    public static String getLocation(String ip) {
        HttpGet httpGet = new HttpGet(String.format(Locale.US, IpLocServer, ip));
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        HttpEntity entity = null;
        String responseContent = null;
        try {
            httpClient = HttpClients.createDefault();
            response = httpClient.execute(httpGet);
            entity = response.getEntity();
            responseContent = EntityUtils.toString(entity, "UTF-8");
            responseContent = responseContent.split("\"")[3];
        } catch (Exception e) {
            log.error("get ip location fail with {}", e);
        } finally {
            try {
                if (response != null)
                    response.close();
                if (httpClient != null)
                    httpClient.close();
            } catch (IOException e) {
            }
        }
        return responseContent;
    }

    public static class AccessMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static Logger log = LoggerFactory.getLogger(AccessMapper.class);
        static Pattern MSIE = Pattern.compile("MSIE\\s*[0-9]*\\.[0-9]*");
        static Pattern WINDOWS = Pattern.compile("Windows\\s*NT\\s*[0-9]*\\.[0-9]*");
        static Pattern ANDROID = Pattern.compile("Android[\\s/]*[0-9]*\\.[0-9]*");
        //

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
                context.write(new Text("IpDate\t" + remoteAddr + "\t" + dateFormat.format(time)), new LongWritable(1));
                //write Path-Date
                context.write(new Text("ApiDate\t" + path + "\t" + dateFormat.format(time)), new LongWritable(1));
                //write IP-Month
                context.write(new Text("IpMon\t" + remoteAddr + "\t" + monthFormat.format(time)), new LongWritable(1));
                //write Path-Month
                context.write(new Text("ApiMon\t" + path + "\t" + monthFormat.format(time)), new LongWritable(1));
                //write browser
                if (agent.contains("Opera")) {
                    context.write(new Text("Browser\tOpera"), new LongWritable(1));
                } else if (agent.contains("Firefox")) {
                    context.write(new Text("Browser\tFirefox"), new LongWritable(1));
                } else if (agent.contains("Chrome")) {
                    context.write(new Text("Browser\tChrome"), new LongWritable(1));
                } else if (agent.contains("Safari")) {
                    context.write(new Text("Browser\tSafari"), new LongWritable(1));
                } else if (agent.contains("MSIE")) {
                    Matcher m = MSIE.matcher(agent);
                    if (m.find()) {
                        String ie = m.group().trim();
                        context.write(new Text("Browser\t" + ie), new LongWritable(1));
                    } else {
                        log.warn("process ie agent fail by line:{}", line);
                    }
                } else if (agent.contains("SVN")) {
                    context.write(new Text("Browser\tSVN"), new LongWritable(1));
                } else {
                    context.write(new Text("Browser\tUnknown"), new LongWritable(1));
                }
                //write system
                if (agent.contains("Macintosh")) {
                    context.write(new Text("System\tMacintosh"), new LongWritable(1));
                } else if (agent.contains("iPhone")) {
                    context.write(new Text("System\tiPhone"), new LongWritable(1));
                } else if (agent.contains("iPad")) {
                    context.write(new Text("System\tiPad"), new LongWritable(1));
                } else if (agent.contains("Windows")) {
                    Matcher m = WINDOWS.matcher(agent);
                    if (m.find()) {
                        String window = m.group().trim();
                        context.write(new Text("System\t" + window), new LongWritable(1));
                    } else {
                        log.warn("process window agent fail by line:{}", line);
                    }
                } else if (agent.contains("Android")) {
                    Matcher m = ANDROID.matcher(agent);
                    if (m.find()) {
                        String android = m.group().trim();
                        context.write(new Text("System\t" + android), new LongWritable(1));
                    } else {
                        log.warn("process android agent fail by line:{}", line);
                    }
                } else if (agent.contains("SVN")) {
                    context.write(new Text("System\tSVN"), new LongWritable(1));
                } else {
                    context.write(new Text("System\tUnknown"), new LongWritable(1));
                }
            } catch (Exception e) {
                log.error("process fail by line:{}", line, e);
            }
        }
    }

    public static class PvMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static Logger log = LoggerFactory.getLogger(PvMapper.class);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //ip    date    count
            String line = value.toString();
            try {
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) {
                    return;
                }
                Date time = dateFormat.parse(parts[1]);
                String location = getLocation(parts[0]);
                if (Strings.isNullOrEmpty(location)) {
                    location = "unknown";
                }
                LongWritable count = new LongWritable(Long.parseLong(parts[2]));
                //write ipv
                context.write(new Text("IpvDate\t" + parts[1]), new LongWritable(1));
                context.write(new Text("IpvMon\t" + monthFormat.format(time)), new LongWritable(1));
                //write pv
                context.write(new Text("PvDate\t" + parts[1]), count);
                context.write(new Text("PvMon\t" + monthFormat.format(time)), count);
                //write location
                context.write(new Text("LocDate\t" + parts[1] + "\t" + location), count);
                context.write(new Text("LocMon\t" + monthFormat.format(time) + "\t" + location), count);
            } catch (Exception e) {
                log.error("PvMapper process fail by line:{}", line, e);
            }
        }
    }


    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        MultipleOutputs<Text, LongWritable> outs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outs = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) {
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

    public static void setupAccessJob(Job job) {
        MultipleOutputs.addNamedOutput(job, "IpDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "IpMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "ApiDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "ApiMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "System", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "Browser", TextOutputFormat.class, Text.class, LongWritable.class);
        job.setJarByClass(PvAnalyzer.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }

    public static void setupPvJob(Job job) {
        MultipleOutputs.addNamedOutput(job, "IpvDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "IpvMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "PvDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "PvMon", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "LocDate", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "LocMon", TextOutputFormat.class, Text.class, LongWritable.class);
        job.setJarByClass(PvAnalyzer.class);
        job.setMapperClass(PvMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }
}
