import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pymysql
import json
import logging

# 设置日志级别
logging.getLogger().setLevel(logging.INFO)

# 定义 Pipeline 选项
beam_options = PipelineOptions(
    runner='DataflowRunner',
    setup_file='/home/jupyter/setup.py',
    project='oppo-gcp-prod-digfood-129869',
    temp_location='gs://qpon-dataflow-mysql/temp',
    max_num_workers=2,
    region='asia-southeast2',
    streaming=True
)

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
            logging.info(f"Parsed JSON data: {data}")
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

        logging.info(f"Processing event_type: {event_type} for table {schema_name}.{table_name}")

        # 连接到 Cloud SQL
        import pymysql
        try:
            connection = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=schema_name
            )
            logging.info(f"Connected to Cloud SQL database: {schema_name}")
        except Exception as e:
            logging.error(f"Failed to connect to Cloud SQL: {e}, skipping.")
            return

        try:
            expected_schema = after_data  # Assuming after_data contains the expected schema
            if not self.apply_schema_changes(connection, schema_name, table_name, expected_schema, after_columns):
                logging.error(f"Failed to apply schema changes for table {schema_name}.{table_name}, skipping.")
                return

            table_schema = self.get_table_schema(connection, schema_name, table_name)
            logging.info(f"Retrieved table schema: {table_schema}")

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
                    logging.info(f"Executing INSERT SQL: {sql} with params: {list(after_data.values())}")
                    cursor.execute(sql, list(after_data.values()))
                    logging.info(f"INSERT successful for table {schema_name}.{table_name}")
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
                    logging.info(f"Executing UPDATE SQL: {sql} with params: {params}")
                    cursor.execute(sql, params)
                    if cursor.rowcount == 0:
                        logging.warning(f"No rows updated for SQL: {sql} with params: {params}")
                        # 尝试插入数据
                        logging.info(f"Attempting to insert data as no rows were updated.")
                        if not all(col in table_schema for col in after_data.keys()):
                            logging.error(f"Table schema mismatch for INSERT: {data}, skipping.")
                            return

                        # 构建插入 SQL 语句
                        columns = ', '.join(after_data.keys())
                        placeholders = ', '.join(['%s'] * len(after_data))
                        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                        logging.info(f"Executing INSERT SQL: {sql} with params: {list(after_data.values())}")
                        cursor.execute(sql, list(after_data.values()))
                        logging.info(f"INSERT successful for table {schema_name}.{table_name}")
                    else:
                        logging.info(f"UPDATE successful for table {schema_name}.{table_name}")
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
                    logging.info(f"Executing DELETE SQL: {sql} with params: {params}")
                    cursor.execute(sql, params)
                    if cursor.rowcount == 0:
                        logging.warning(f"No rows deleted for SQL: {sql} with params: {params}")
                    else:
                        logging.info(f"DELETE successful for table {schema_name}.{table_name}")
                # 提交事务
                connection.commit()
                logging.info(f"{event_type} successful for table {schema_name}.{table_name}")
        except Exception as e:
            logging.error(f"Error occurred while {event_type.lower()}ing into Cloud SQL: {e}")
        finally:
            connection.close()

# 设置流式处理模式
options = PipelineOptions(
        streaming=True,  # 设置为流式处理
    )

# Cloud SQL 配置
db_config = {
    'host': '10.4.240.3',
    'user': 'root',
    'password': 'X&T]|f;+VxH~Zjn='
}

def run():
    with beam.Pipeline(options=beam_options) as pipeline:
        query_cloudsql = (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
                subscription='projects/oppo-gcp-prod-digfood-129869/subscriptions/qpon-digital-food-topic-sub-3')
            | 'QueryCloudSQL' >> beam.ParDo(QueryCloudSQL2(db_config))
            | 'PrintQueryResult' >> beam.Map(lambda x: logging.info(f"Processed element: {x}"))
        )

if __name__ == "__main__":
    run()