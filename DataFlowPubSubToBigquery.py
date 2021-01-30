## To run dataflow,Use this below Command 
## python dfPipeline.py --streaming --runner DataflowRunner --project yourprojectid --temp_location gs://mybuc1111/temp --staging_location    gs://mybuc1111/staging --job_name mydatapipeline --region us-central1

from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

import apache_beam as beam
import argparse

PROJECT_ID = 'your project id'
SUBSCRIPTION = 'projects/' + PROJECT_ID + '/subscriptions/mymsg-data'
SCHEMA = 'customer_id:Integer,first_name:String,state:String,car_make:String,Salary:Integer'

def parse_pubsub(data):
    import json
    return json.loads(data)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION).with_output_types(bytes)
       | 'Decode the Data' >> beam.Map(lambda x: x.decode('utf-8'))
       | 'PubSubDataToJSON' >> beam.Map(parse_pubsub)
       | 'WriteToBigQueryTable' >> beam.io.WriteToBigQuery(
           '{0}:mydataset.mytable'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
