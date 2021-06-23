'''
Author: Zhan
Date: 2021-06-15 23:46:43
LastEditTime: 2021-06-23 12:21:02
LastEditors: Please set LastEditors
Description: In User Settings Edit
FilePath: /common_crawl/education_extraction.py
'''
# 要添加一个新单元，输入 '# %%'
# 要添加一个新的标记单元，输入 '# %% [markdown]'
# %%
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

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

input_bucket = 's3://zhan-commoncrawl/Unsaved/Unsaved/2021/06/18/tables/22e228f2-56b2-461e-b144-b054d7b1cbb4/'
df = spark.read.parquet(input_bucket)
df.createOrReplaceTempView("education_file_list")
sqlDF = spark.sql('SELECT * from education_file_list limit 10000')


warc_fileanme_list = sqlDF.select('warc_filename').rdd.flatMap(lambda x:x).collect()
url_host_name_list = sqlDF.select('url_host_name').rdd.flatMap(lambda x:x).collect()
print(url_host_name_list[:10])
warc_fileanme_list = list(map(lambda x: "s3://commoncrawl/{}".format(x),warc_fileanme_list))
print(warc_fileanme_list[:10])

# %% [markdown]
# # 获得trakcer标志

# %%
tracker_bucket = 's3://aws-emr-resources-235671948910-us-east-1/labeled_third_party/'
df_tracker = spark.read.option("header",True).csv(tracker_bucket)
df_tracker.createOrReplaceTempView("tracker_list")
sqlDF_tracker = spark.sql("SELECT Domain, Category, Company from tracker_list")
print(sqlDF_tracker.show())


# %%
tracker_list = sqlDF_tracker.select('Domain').rdd.flatMap(lambda x:x).collect()
print(tracker_list[:10])






    
class JupyterCCSparkJob(object):
    """
    A simple Spark job definition to process Common Crawl data
    """

    name = 'CCSparkJob'


    # description of input and output shown in --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    warc_parse_http_header = True

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    num_input_partitions = 400
    num_output_partitions = 10

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        # args = Args()

        arg_parser = argparse.ArgumentParser(prog=self.name, description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("--input", help=self.input_descr)
        arg_parser.add_argument("--output", help=self.output_descr,default="s3://zhan-commoncrawl/tag_count_output")

        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions, "
                                "number of parallel tasks to process WARC "
                                "files/records")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
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

        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        arg_parser.add_argument("--spark-profiler", action='store_true',
                                help="Enable PySpark profiler and log"
                                " profiling metrics if job has finished,"
                                " cf. spark.python.profile")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()
        
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
        self.args = self.parse_arguments()
        
        conf = SparkConf()
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        sc = SparkContext.getOrCreate(conf=conf)
        self.init_accumulators(sc)

        self.run_job(sc)

        if self.args.spark_profiler:
            sc.show_profiles()

        sc.stop()

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
        
        input_data = sc.parallelize(warc_fileanme_list)

        # input_data = sc.textFile(','.join(warc_fileanme_list))
           
        output = input_data.mapPartitionsWithIndex(self.process_warcs).reduceByKey(lambda x,y:x,numPartitions = self.args.num_output_partitions)

        # print(output.count())
        # print(output.take(10))
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
       
        for uri in iterator:
            self.warc_input_processed.add(1)
            print("WARC/WAT/WET input files processed = {}".format(self.warc_input_processed))
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)
                warctemp = TemporaryFile(mode='w+b',
                                         dir=self.args.local_temp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, warctemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.warc_input_failed.add(1)
                    warctemp.close()
                    continue
                warctemp.seek(0)
                stream = warctemp

            no_parse = (not self.warc_parse_http_header)
           
            try:
                archive_iterator = ArchiveIterator(stream,
                                                   no_record_parse=no_parse, arc2warc = True)
                
                for res in self.iterate_records(uri, archive_iterator):

                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
    
        for record in archive_iterator:
            for res in self.process_record(record):   
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
        for node in tree.css('a'):
            if 'href' in node.attributes:
                url = node.attributes['href']
                domain = str(urlparse(url).netloc)
            
                domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain)
        
        for node in tree.css('script'):
            if 'src' in node.attributes:             
                url = node.attributes['src']
                domain = str(urlparse(url).netloc)
                domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain)
                    
        for node in tree.css('iframe'):
            if 'src' in node.attributes:             
                url = node.attributes['src']
                domain = str(urlparse(url).netloc)
                domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain)
                
        for node in tree.css('img'):
            if 'src' in node.attributes:             
                url = node.attributes['src']
                domain = str(urlparse(url).netloc)
                domain = '.'.join(domain.split('.')[-2:])
                if len(domain) >= 2:
                    trackers.append(domain)
            
            
    except Exception as e:
        print(e)
    
    return ','.join(list(set(trackers)))

def get_text_bs(html):
    
    try:
        tree = BeautifulSoup(html, 'lxml')
    
        trackers = []

        body = tree.body
        if body is None:
            return None

        for tag in body.select('style'):
            tag.decompose()

        text = body.get_text(separator='\n')
        a = body.find_all('a')
        for aa in a:
            url = aa.get('href')
            domain = str(urlparse(url).netloc)
#             如果domain的长度小于三，就不加入tracker

#             判断domain 是不是trackers

            domain = '.'.join(domain.split('.')[-2:])
            if len(domain) >= 2 and domain in tracker_list:
                trackers.append(domain)

        s = body.find_all('script')
        for ss in s:
            url = ss.get('src')
            domain = str(urlparse(url).netloc)
            domain = '.'.join(domain.split('.')[-2:])
            if len(domain) >= 2 and domain in tracker_list:
                trackers.append(domain)

    except Exception as e:
        print(e)
    
    return ','.join(list(set(trackers)))

class Trackers_extraction_job(JupyterCCSparkJob):
    """ Count HTML tag names in Common Crawl WARC files"""

    name = "TagCount"

    # match HTML tags (element names) on binary HTML data
    html_tag_pattern = re.compile(b'<([a-z0-9]+)')

    def process_record(self, record):
        
        if record.rec_type != 'response':
            return
        url = record.rec_headers.get_header('WARC-Target-URI')
        domain = urlparse(url).netloc
#         判断domain 是否在教育网址中
        if domain not in url_host_name_list:
            return
        text = record.content_stream().read()
        trackers = get_text_selectolax(text)
        if url and url.strip() != '' and trackers:
            yield domain, trackers


            
job = Trackers_extraction_job()
job.run()