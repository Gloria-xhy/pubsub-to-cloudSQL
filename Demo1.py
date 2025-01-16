import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pymysql
import json
import logging

class QueryCloudSQL2(beam.DoFn):
    def __init__(self, db_config):
        self.db_config = db_config
        self.table_schemas = {}

    def get_table_schema(self, connection, schema_name, table_name):
        if (schema_name, table_name) in self.table_schemas:
            return self.table_schemas[(schema_name, table_name)]

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'")
            columns = cursor.fetchall()
            schema = {col[0]: col[1] for col in columns}
            self.table_schemas[(schema_name, table_name)] = schema
            return schema

    def apply_schema_changes(self, connection, schema_name, table_name, expected_schema, after_columns):
        current_schema = self.get_table_schema(connection, schema_name, table_name)
        missing_columns = set(expected_schema.keys()) - set(current_schema.keys())

        if missing_columns:
            logging.warning(f"Missing columns in table {schema_name}.{table_name}: {missing_columns}")
            for column in missing_columns:
                # Extract the correct column type from after_columns
                column_type = None
                for col_info in after_columns:
                    if col_info['columnName'] == column:
                        column_type = col_info['columnMysqlType']
                        break
                if column_type is None:
                    logging.error(f"Could not determine type for column {column}, skipping.")
                    continue
                logging.warning(f"Adding column {column} of type {column_type} to table {schema_name}.{table_name}")
                with connection.cursor() as cursor:
                    cursor.execute(f"ALTER TABLE `{schema_name}`.`{table_name}` ADD COLUMN `{column}` {column_type}")
                connection.commit()
                # Re-fetch the schema after adding the column
                current_schema = self.get_table_schema(connection, schema_name, table_name)
                if column not in current_schema:
                    logging.error(f"Failed to add column {column} to table {schema_name}.{table_name}, skipping.")
                    return False
            return True
        return True

    def process(self, element):
        if element is None:
            logging.info("Received None element, skipping.")
            return

        # 解析 JSON 数据
        try:
            data = json.loads(element)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}, skipping element: {element}")
            return

        schema_name = data.get('schemaName')
        table_name = data.get('tableName')
        event_type = data.get('eventType')
        after_data = data.get('after', {})
        before_data = data.get('before', {})
        after_columns = data.get('afterColumns', [])
        before_columns = data.get('beforeColumns', [])
        key_names = data.get('keyNames', [])

        if not schema_name or not table_name or not event_type:
            logging.error(f"Missing required fields in data: {data}, skipping.")
            return

        # 连接到 Cloud SQL
        connection = pymysql.connect(
            host=self.db_config['host'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=schema_name
        )
        try:
            expected_schema = after_data  # Assuming after_data contains the expected schema
            if not self.apply_schema_changes(connection, schema_name, table_name, expected_schema, after_columns):
                logging.error(f"Failed to apply schema changes for table {schema_name}.{table_name}, skipping.")
                return

            table_schema = self.get_table_schema(connection, schema_name, table_name)

            with connection.cursor() as cursor:
                if event_type == 'INSERT':
                    # 检查表结构
                    if not all(col in table_schema for col in after_data.keys()):
                        logging.error(f"Table schema mismatch for INSERT: {data}, skipping.")
                        return

                    # 构建插入 SQL 语句
                    columns = ', '.join(after_data.keys())
                    placeholders = ', '.join(['%s'] * len(after_data))
                    sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, list(after_data.values()))
                elif event_type == 'UPDATE':
                    # 检查 before_data 是否为空
                    if not before_data:
                        logging.error(f"before_data is empty, cannot perform UPDATE: {data}, skipping.")
                        return

                    # 检查 key_names 是否为空
                    if not key_names:
                        logging.error(f"keyNames is empty, cannot perform UPDATE: {data}, skipping.")
                        return

                    # 从 beforeColumns 中提取主键值
                    key_values = {}
                    for key_name in key_names:
                        key_value = None
                        for col in before_columns:
                            if col['columnName'] == key_name:
                                key_value = col['columnValue']
                                break
                        if key_value is None:
                            logging.error(f"Could not find '{key_name}' in beforeColumns, cannot perform UPDATE: {data}, skipping.")
                            return
                        key_values[key_name] = key_value

                    # 检查表结构
                    if not all(col in table_schema for col in after_data.keys()):
                        logging.error(f"Table schema mismatch for UPDATE: {data}, skipping.")
                        return

                    # 构建更新 SQL 语句
                    set_clause = ', '.join([f"{col} = %s" for col in after_data.keys()])
                    where_clause = ' AND '.join([f"{key} = %s" for key in key_values.keys()])
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
                    params = list(after_data.values()) + list(key_values.values())
                    logging.info(f"Executing SQL: {sql} with params: {params}")
                    cursor.execute(sql, params)
                    if cursor.rowcount == 0:
                        logging.warning(f"No rows updated for SQL: {sql} with params: {params}")
                elif event_type == 'DELETE':
                    # 检查 before_data 是否为空
                    if not before_data:
                        logging.error(f"before_data is empty, cannot perform DELETE: {data}, skipping.")
                        return

                    # 检查 key_names 是否为空
                    if not key_names:
                        logging.error(f"keyNames is empty, cannot perform DELETE: {data}, skipping.")
                        return

                    # 从 beforeColumns 中提取主键值
                    key_values = {}
                    for key_name in key_names:
                        key_value = None
                        for col in before_columns:
                            if col['columnName'] == key_name:
                                key_value = col['columnValue']
                                break
                        if key_value is None:
                            logging.error(f"Could not find '{key_name}' in beforeColumns, cannot perform DELETE: {data}, skipping.")
                            return
                        key_values[key_name] = key_value

                    # 检查表结构
                    if not all(col in table_schema for col in key_values.keys()):
                        logging.error(f"Table schema mismatch for DELETE: {data}, skipping.")
                        return

                    # 构建删除 SQL 语句
                    where_clause = ' AND '.join([f"{key} = %s" for key in key_values.keys()])
                    sql = f"DELETE FROM `{table_name}` WHERE {where_clause}"
                    params = list(key_values.values())
                    logging.info(f"Executing SQL: {sql} with params: {params}")
                    cursor.execute(sql, params)
                    if cursor.rowcount == 0:
                        logging.warning(f"No rows deleted for SQL: {sql} with params: {params}")
                # 提交事务
                connection.commit()
                logging.info(f"{event_type} successful")
        except Exception as e:
            logging.error(f"Error occurred while {event_type.lower()}ing into Cloud SQL: {e}")
        finally:
            connection.close()


# 设置流式处理模式
def run(argv=None):
    options = PipelineOptions(argv)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'oppo-gcp-prod-digfood-129869'
    google_cloud_options.region = 'asia-southeast2'
    google_cloud_options.job_name = f'qpon-dataflow-mysql-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    google_cloud_options.staging_location = 'gs://qpon-dataflow-mysql/staging'
    google_cloud_options.temp_location = 'gs://qpon-dataflow-mysql/temp'

    worker_options = options.view_as(WorkerOptions)
    worker_options.sdk_container_image = 'gcr.io/oppo-gcp-prod-digfood-129869/qpon-dataflow-mysql_image:latest'
    worker_options.max_num_workers = 2  # 设置最大 worker 数量
    worker_options.autoscaling_algorithm = 'THROUGHPUT_BASED'  # 启用基于吞吐量的自动缩放

    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    # 设置 save_main_session 选项
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    # Cloud SQL 配置
    db_config = {
        'host': '10.4.240.3',
        'user': 'root',
        'password': 'X&T]|f;+VxH~Zjn='
    }

    # 配置日志记录
    logging.basicConfig(level=logging.INFO)

    with beam.Pipeline(options=options) as pipeline:

        query_cloudsql = (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription='projects/oppo-gcp-prod-digfood-129869/subscriptions/qpon-digital-food-topic-sub')
            | 'WindowIntoFixedWindows' >> beam.WindowInto(beam.window.FixedWindows(5))  # 60秒的固定窗口
            | 'DistinctElements' >> beam.Distinct()  # 去重
            | 'QueryCloudSQL' >> beam.ParDo(QueryCloudSQL2(db_config))
            | 'PrintQueryResult' >> beam.Map(print)
        )

if __name__ == '__main__':
    run()