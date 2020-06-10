package com.test.reduce;

import com.test.mapper.FlightsByCarrierMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Classname FlightsByCarrierReducer
 * @Description 飞机每个月起飞数量
 * @Date 2020/6/8 11:22
 * @Created by xcp
 */
public class FlightsByCarrierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(FlightsByCarrierReducer.class);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
