import json
import csv
from google.cloud import pubsub_v1
import time

project_name = 'your project name'
topic_name = 'mymsg'
file = '/user/CustomersData.csv'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_name, topic_name)

def publish(publisher, topic,file):
    with open(file) as fh:
        rd = csv.DictReader(fh, delimiter=',')
        for row in rd:
            data = json.dumps(dict(row))
            publisher.publish(topic_path, data=data.encode('utf-8'))

if __name__ == '__main__':
    print("Hit CTRL-C to stop Tweeping!")
    while True:
        publish(publisher, topic_path, file)
        time.sleep(0.5)
