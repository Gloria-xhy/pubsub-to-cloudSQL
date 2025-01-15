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

# 全局变量，用于记录已创建的表和 schema 信息
SCHEMA_DICT = {}
CREATED_TABLES = set()

class SchemaManager:
    """管理 Cloud SQL schema 的类，负责生成和更新 schema"""
    def __init__(self, db_connection_string):
        self.engine = create_engine(db_connection_string)

    def load_existing_tables(self):
        """加载数据库中的所有表名"""
        global CREATED_TABLES
        try:
            with self.engine.connect() as connection:
                result = connection.execute("SHOW TABLES;")
                CREATED_TABLES = {row[0] for row in result}
                logging.info(f"Loaded existing tables: {CREATED_TABLES}")
        except Exception as e:
            logging.error(f"Error loading existing tables: {e}")

    def create_table(self, table_name, schema):
        """检查并在必要时创建 Cloud SQL 表"""
        global CREATED_TABLES
        if table_name in CREATED_TABLES:
            return

        create_statement = f"CREATE TABLE {table_name} ({', '.join(f'{field.name} {field.type_}' for field in schema)});"
        try:
            with self.engine.connect() as connection:
                connection.execute(create_statement)
                logging.info(f"Created table {table_name} with schema: {schema}")
                CREATED_TABLES.add(table_name)
        except Exception as e:
            logging.error(f"Error creating table {table_name}: {e}")

    def determine_sql_type(self, value, key):
        """根据值的类型确定 SQL 字段类型"""
        if isinstance(value, str):
            return f"{key} VARCHAR(255)"
        elif isinstance(value, bool):
            return f"{key} BOOLEAN"
        elif isinstance(value, int):
            return f"{key} INT"
        elif isinstance(value, float):
            return f"{key} FLOAT"
        elif isinstance(value, datetime):
            return f"{key} TIMESTAMP"
        else:
            return f"{key} VARCHAR(255)"

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

            filtered_properties = {
                ('obus_' + k[1:] if k.startswith('$') else k): v
                for k, v in properties.items()
            }

            row.update(filtered_properties)

            transformed_row = {key.lower(): json.dumps(value) if isinstance(value, (dict, list)) else str(value) for key, value in row.items()}

            table_name = f"{event_group}_{event_id}_ProcessedMessage"
            yield (table_name, transformed_row)
        except Exception as e:
            logging.error(f"Error processing message: {e}")

class CollectSchemaUpdates(beam.DoFn):
    """收集需要更新的 schema 信息"""
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

    def process(self, element):
        table_name, row = element
        existing_schema = SCHEMA_DICT.get(table_name, [])
        current_fields = {field.name for field in existing_schema}
        new_fields = []

        for key, value in row.items():
            if key.lower() not in current_fields:
                new_field = self.schema_manager.determine_sql_type(value, key.lower())
                new_fields.append(new_field)
                current_fields.add(key.lower())

        yield (table_name, new_fields, row)

class UpdateSchemaOnce(beam.DoFn):
    """统一新建表和更新 schema"""
    def __init__(self, schema_manager):
        self.schema_manager = schema_manager

    def process(self, element):
        table_name, new_fields, row = element
        self.schema_manager.create_table(table_name, new_fields)

        yield (table_name, row)

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

def run(argv=None):
    options = PipelineOptions(argv)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'qpon-1c174'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = f'xhy-dataflow-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    google_cloud_options.staging_location = 'gs://xhy_bucket/templates/staging'
    google_cloud_options.temp_location = 'gs://xhy_bucket/templates/temp'

    worker_options = options.view_as(WorkerOptions)
    worker_options.sdk_container_image = 'gcr.io/qpon-1c174/xhy_image:latest'
    worker_options.max_num_workers = 2
    worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'

    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    db_connection_string = 'mysql+pymysql://xhy:12345@34.170.50.115:3306/xhy-test'  # 替换为实际的连接字符串
    schema_manager = SchemaManager(db_connection_string)
    schema_manager.load_existing_tables()  # 初始化时加载现有表名

    with beam.Pipeline(options=options) as p:
        # 1. 读取并解码数据
        messages = (
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic='projects/qpon-1c174/topics/QponPagesTopic')
            | "Decode and Process Message" >> beam.ParDo(DecodeAndProcessMessage())
        )
        
        # 2. 分窗口并按表名分组
        grouped_messages = (
            messages
            | "Window into Fixed Windows" >> beam.WindowInto(beam.window.FixedWindows(300))
            | "Group by Table Name" >> beam.GroupByKey()
        )
        
        # 3. 收集需要更新的 schema 信息
        schema_updates = (
            grouped_messages
            | "Collect Schema Updates" >> beam.ParDo(CollectSchemaUpdates(schema_manager))
        )
        
        # 4. 统一新建表和更新 schema
        updated_tables = (
            schema_updates
            | "Update Schema Once" >> beam.ParDo(UpdateSchemaOnce(schema_manager))
        )

        # 5. 写入 Cloud SQL
        updated_tables | "Write to Cloud SQL" >> beam.ParDo(WriteToCloudSQL(db_connection_string))

if __name__ == '__main__':
    run()