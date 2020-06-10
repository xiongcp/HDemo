package com.test.mapper;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Classname FlightsByCarrierMapper
 * @Description 飞机每个月起飞数量
 * @Date 2020/6/8 11:21
 * @Created by xcp
 */
public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(FlightsByCarrierMapper.class);

    private CSVParser csvParser;

    public FlightsByCarrierMapper() {
        csvParser = new CSVParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (StringUtils.isNotBlank(value.toString())) {
            String[] line = csvParser.parseLine(value.toString());
            if (line.length < 30) {
                return;
            }
            String month = line[2];
            context.write(new Text(month), new IntWritable(1));
        }
    }
}
