'''
Author: Zhan
Date: 2021-06-15 23:46:43
LastEditTime: 2022-01-01 20:23:51
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /common_crawl/education_extraction.py
'''
# 要添加一个新单元，输入 '# %%'
# 要添加一个新的标记单元，输入 '# %% [markdown]'
import argparse
import logging
import os
import re

from collections import Counter
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selectolax.parser import HTMLParser


from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import functions as F
import tempfile

regex = "((?<=[^a-zA-Z0-9])(?:https?\:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*)"


LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

#  input parameters

name = 'CCSparkJob'
description = "edu_trackers"
arg_parser = argparse.ArgumentParser(prog=name, description=description,
                                             conflict_handler='resolve')

arg_parser.add_argument("--input", help= "input dir")
arg_parser.add_argument("--output", help="output_dir",default="s3://zhan-commoncrawl/tag_count_output")

arg_parser.add_argument("--num_input_partitions", type=int,
                        default=400,
                        help="Number of input splits/partitions, "
                        "number of parallel tasks to process WARC "
                        "files/records")
arg_parser.add_argument("--num_output_partitions", type=int,
                        default=10,
                        help="Number of output partitions")
arg_parser.add_argument("--output_format", default="parquet",
                        help="Output format: parquet (default),"
                        " orc, json, csv")
arg_parser.add_argument("--output_compression", default="gzip",
                        help="Output compression codec: None,"
                        " gzip/zlib (default), snappy, lzo, etc.")
arg_parser.add_argument("--output_option", action='append', default=[],
                        help="Additional output option pair"
                        " to set (format-specific) output options, e.g.,"
                        " `header=true` to add a header line to CSV files."
                        " Option name and value are split at `=` and"
                        " multiple options can be set by passing"
                        " `--output_option <name>=<value>` multiple times")

arg_parser.add_argument("--local_temp_dir", default=None,
                        help="Local temporary directory, used to"
                        " buffer content from S3")

arg_parser.add_argument("--log_level", default="INFO",
                        help="Logging level")
arg_parser.add_argument("--spark-profiler", action='store_true',
                        help="Enable PySpark profiler and log"
                        " profiling metrics if job has finished,"
                        " cf. spark.python.profile")

arg_parser.add_argument("--edu_file_path", 
default = 's3://zhan-commoncrawl/Unsaved/Unsaved/2021/06/29/tables/db3772b6-4171-4247-947e-aded8ff28ed8/',
help = "edu file list stored in s3")

arg_parser.add_argument("--num_of_list",default=100,
help = "small list of files, debug")

arg_parser.add_argument("--debug",default = 0,
help = "whether debug or not")


args_all = arg_parser.parse_args()
#  get the edu_file_list

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.parquet(args_all.edu_file_path)
df.createOrReplaceTempView("education_file_list")

if args_all.debug == 1:
    sqlDF = spark.sql('SELECT * from education_file_list limit {}'.format(args_all.num_of_list))
elif args_all.debug == 0:
    sqlDF = spark.sql('SELECT crawl,warc_filename,warc_record_offset,warc_record_length from education_file_list')
else:
    print("exit")
    exit(0)

warc_filename_list = sqlDF.collect()
print("num of filelist:{}".format(len(warc_filename_list)))
print(warc_filename_list[:10])
# url_host_name_list = sqlDF.select('url_host_name').rdd.flatMap(lambda x:x).collect()
# print(url_host_name_list[:10])


    
class JupyterCCSparkJob(object):
    """
    A simple Spark job definition to process Common Crawl data
    """
    warc_parse_http_header = True

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)
    
    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        args = args_all
        
        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

    def get_output_options(self):
        return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                        self.args.output_option)}

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager.getLogger(self.name)

    def run(self):
        self.args = args_all
        
        conf = SparkConf()
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        sc = SparkContext.getOrCreate(conf=conf)
        self.init_accumulators(sc)

        self.run_job(sc)

        if self.args.spark_profiler:
            sc.show_profiles()

# 		sc.stop()

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b
    

    def run_job(self, sc):
        
        input_data = sc.parallelize(warc_filename_list)
           
        output = input_data.mapPartitionsWithIndex(self.process_warcs)\
        .reduceByKey(lambda x,y:x)

        # print(output.count())
        print(output.take(10))
        columns = ['url','trackers']
        df = output.toDF(columns)
        df.write.format("csv").mode('overwrite').option("header", "true").save(self.args.output)
        
        self.log_aggregators(sc)
    
    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = "/user/"

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)
       
        for item in iterator:
            warc_filename = item['warc_filename']
            offset = item['warc_record_offset']
            length = item['warc_record_length']
            
            crawl_time = item['crawl']
            self.warc_input_processed.add(1)
           
            self.get_logger().info('Reading from S3 {}'.format(warc_filename))
            
            offset_end = int(offset) + int(length) - 1
            byte_range = 'bytes={offset}-{end}'.format(offset=offset, end=offset_end)

            warctemp = None
            try:
                warctemp = s3client.get_object(Bucket='commoncrawl', Key=warc_filename, Range=byte_range)['Body']
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to access {}: {}'.format(warc_filename, exception))
                self.warc_input_failed.add(1)
                continue
            stream = warctemp

            no_parse = (not self.warc_parse_http_header)
           
            try:
                archive_iterator = ArchiveIterator(stream,
                                                   no_record_parse=no_parse, arc2warc = True)
                
                for res in self.iterate_records(crawl_time, archive_iterator):

                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(warc_filename, exception))
            finally:
                stream.close()

    def process_record(self,record,crawl_time = None):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, crawl_time, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
    
        for record in archive_iterator:
            rec_type = record.rec_type
            url = record.rec_headers.get_header('WARC-Target-URI')
            for res in self.process_record(record,crawl_time):   
                yield res
         
            self.records_processed.add(1)
            # WARC record offset and length should be read after the record
            # has been processed, otherwise the record content is consumed
            # while offset and length are determined:
            #  warc_record_offset = archive_iterator.get_record_offset()
            #  warc_record_length = archive_iterator.get_record_length()

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain')

    @staticmethod
    def is_wat_json_record(record):
        """Return true if WARC record is a WAT record"""
        return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
            (record.rec_headers['WARC-Identified-Payload-Type'] in
             html_types)):
            return True
        for html_type in html_types:
            if html_type in record.content_type:
                return True
        return False

def get_text_selectolax(html):
    trackers = []
    try:
        tree = HTMLParser(html)
        if tree.body is None:
            return None
        
        for node in tree.tags('style'):
            node.decompose()
            
        #         找到a
        for node in tree.css('a,link,script,iframe,img'):
            text = node.text()
            if ("google-analytics" in text):
                trackers.append("google-analytics.com")
            if 'href' in node.attributes:
                url = node.attributes['href']
                domain = str(urlparse(url).netloc)
#                 domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain) # 这里我们认为所有的third-party request 都是trackers
            if 'src' in node.attributes:             
                url = node.attributes['src']
                domain = str(urlparse(url).netloc)
#                 domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain)
                    
            if "type" in node.attributes and node.attributes['type'] == 'text/javascript':
                
                result = re.findall(regex,text)
                for url in result:
                    domain = str(urlparse(url).netloc)
#                     domain = '.'.join(domain.split('.')[-2:])
                    if len(domain) >= 2:
                        trackers.append(domain)
                        
    except Exception as e:
        print(e)
    
    return ','.join(list(set(trackers)))

class Trackers_extraction_job(JupyterCCSparkJob):
    """ Count HTML tag names in Common Crawl WARC files"""

    name = "TagCount"

    # match HTML tags (element names) on binary HTML data
    html_tag_pattern = re.compile(b'<([a-z0-9]+)')

    def process_record(self, record, crawl_time):
        
        if record.rec_type != 'response':
            return
        url = record.rec_headers.get_header('WARC-Target-URI')
        text = record.content_stream().read()
        trackers = get_text_selectolax(text)
        if url and url.strip() != '': 
            yield "#".join([url,crawl_time]), trackers
            
job = Trackers_extraction_job()
job.run()