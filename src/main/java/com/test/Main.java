package com.test;
import com.test.combiner.TailNumByCarrierCombiner;
import com.test.mapper.TailNumTopNByCarrierMapper;
import com.test.reduce.TailNumTopNByCarrierReducer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length == 0) {
            logger.error("输入输出路径不能为空");
        }
        if (args.length > 2) {
            if (StringUtils.isNotBlank(args[2])) {
                try {
                    conf.setInt("topN", Integer.parseInt(args[2]));
                } catch (Exception e) {
                    logger.error("topN属性不能转化为数字,输入为{}", args[2]);
                }
            }
            Job job = new Job(conf, "FlightsByCarrier");
            job.setJarByClass(Main.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(TailNumTopNByCarrierMapper.class);
            job.setReducerClass(TailNumTopNByCarrierReducer.class);
            job.setCombinerClass(TailNumByCarrierCombiner.class);

            TextOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            if (args.length > 2) {
                if (StringUtils.isNotBlank(args[args.length - 1])) {
                    try {
                        job.setNumReduceTasks(Integer.parseInt(args[args.length - 1]));
                    } catch (Exception e) {
                        logger.error("ReduceTasks属性不能转化为数字,输入为{}", args[2]);
                    }
                }
            }
            //job.setOutputValueClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
        }
    }
}
