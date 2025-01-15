import base64
import json
import logging
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions, SetupOptions
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

# 设置日志级别为 INFO
logging.basicConfig(level=logging.INFO)

class WriteToCloudSQL(beam.DoFn):
    """将数据写入 Cloud SQL"""
    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string)

    def process(self, element):
        table_name, row = element
        try:
            insert_query = f"INSERT INTO {table_name} ({', '.join(row.keys())}) VALUES ({', '.join([':' + key for key in row.keys()])})"
            with self.engine.connect() as connection:
                connection.execute(insert_query, row)
                logging.info(f"Inserted row into {table_name}: {row}")
        except IntegrityError as e:
            logging.error(f"Integrity error: {e}")
        except Exception as e:
            logging.error(f"Error writing to Cloud SQL: {e}")

class DecodeAndProcessMessage(beam.DoFn):
    """解码和初步处理 Pub/Sub 消息"""
    def process(self, element):
        try:
            logging.debug(f"Received Pub/Sub message element: {element}")
            decoded_message = element.decode("utf-8")
            event_data = json.loads(decoded_message)
            logging.debug(f"Decoded Pub/Sub message: {event_data}")

            event_id = event_data.get('eventId')
            event_group = event_data.get('eventGroup')
            properties_str = event_data.get('properties', '{}')
            properties = json.loads(properties_str)

            if not event_id or not event_group:
                logging.error("Missing required fields: eventId or eventGroup.")
                return

            uuid = properties.get("uuid")
            if not uuid:
                return

            timestamp_id = properties.get("timestamp_id")
            if not timestamp_id:
                logging.error("timestamp_id is missing from properties.")
                return

            event_time = datetime.fromtimestamp(int(timestamp_id) / 1000, tz=timezone.utc).isoformat()

            row = {
                "eventid": event_id,
                "eventgroup": event_group,
                "event_time": event_time,
                "uuid": uuid
            }

            # 这里可以将其他属性添加到 row 中
            filtered_properties = {
                ('obus_' + k[1:] if k.startswith('$') else k): v
                for k, v in properties.items()
            }

            row.update(filtered_properties)

            transformed_row = {key.lower(): str(value) for key, value in row.items()}

            table_name = "table1"  # 替换为实际的表名
            yield (table_name, transformed_row)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

def run(argv=None):
    options = PipelineOptions(argv)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'qpon-1c174'  # 替换为您的项目ID
    google_cloud_options.region = 'us-central1'  # 替换为您的区域
    google_cloud_options.job_name = f'xhy-dataflow-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    google_cloud_options.staging_location = 'gs://xhy_bucket/templates/staging'  # 替换为您的 Cloud Storage 路径
    google_cloud_options.temp_location = 'gs://xhy_bucket/templates/temp'  # 替换为您的 Cloud Storage 路径

    worker_options = options.view_as(WorkerOptions)
    worker_options.max_num_workers = 2
    worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'

    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    db_connection_string = 'mysql+pymysql://xhy:12345@34.170.50.115:3306/xhy-test'  # 替换为实际的连接字符串
    with beam.Pipeline(options=options) as p:
        # 1. 读取并解码数据
        messages = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic='projects/qpon-1c174/topics/QponPagesTopic')  # 替换为您的 Pub/Sub 主题
            | "Decode and Process Message" >> beam.ParDo(DecodeAndProcessMessage())
        )

        # 2. 写入 Cloud SQL
        messages | "Write to Cloud SQL" >> beam.ParDo(WriteToCloudSQL(db_connection_string))

if __name__ == '__main__':
    run()