import os
import jinja2
from datetime import datetime
from python.util.utilities import Util
from python.util.get_file_path import get_relative_file_path
from python.util.fdw_snowflake import FdwSnowflake
from python.data_processing.procedures.processing_config import table_names

LOG = Util.get_logger(__name__)
execution_date = datetime.now().strftime("%Y-%m-%d")
execution_timestamp = datetime.now()
db_name = str(os.getenv("TARGET_DB"))

CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS {} AS {}"
TRUNCATE_TABLE_SQL = "TRUNCATE TABLE {} "
INSERT_TABLE_FROM_QUERY_SQL = "INSERT INTO {} ({})"

DWH = FdwSnowflake(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse="vw_dev_ingest",
    account="udemy",
    role="jobs_etl",
    database=db_name,
    schema="schema_test",
)


def _read_file(file_path):
    '''
    read file contents from path input
    :param file_path: str
    :return: str
    '''
    file_path = get_relative_file_path(file_path=file_path, caller_file_path=__file__)
    with open(file_path) as f:
        output = f.read()
    return output


def _execute_table_inserts(keyword, table, raw_db, raw_schema, curate_db, curate_schema):
    '''
    insert into a target - curated from source - raw based on output of specifically named sql file
    :param keyword: str
    :param table: str
    :param raw_db: str
    :param raw_schema: str
    :param curate_db: str
    :param curate_schema: str
    :return: na
    '''
    select_query_path = "queries/" + keyword + '_insert.sql'
    select_query_to_run = jinja2.Template(_read_file(select_query_path)).render(db_raw=raw_db, schema_raw=raw_schema,
                                                                                db_curated=curate_db,
                                                                                schema_curated=curate_schema)
    table_nm = curate_db + "." + curate_schema + "." + table
    insert_query_to_run = INSERT_TABLE_FROM_QUERY_SQL.format(table_nm, select_query_to_run)
    LOG.info("executing insert query ... \n" + insert_query_to_run)
    DWH.execute(insert_query_to_run)
    LOG.info("inserts done... " + table_nm)


def _audit_raw_data_table(keyword, raw_db, raw_schema, monitor_db, monitor_schema):
    '''
    insert into raw-data-load-tracker from based on raw data inserts
    :param keyword: str
    :param raw_db: str
    :param raw_schema: str
    :param monitor_db: str
    :param monitor_schema: str
    :return: na
    '''
    a_rawdata_query_path = "queries/" + keyword + '_auditraw.sql'
    a_rawdata_query_to_run = jinja2.Template(_read_file(a_rawdata_query_path)).render(db_raw=raw_db,
                                                                                      schema_raw=raw_schema)
    s3_transfer_log_table = monitor_db + "." + monitor_schema + "." + "S3_FILE_LOAD_AUDIT"
    audit_insert_raw_query_to_run = INSERT_TABLE_FROM_QUERY_SQL.format(s3_transfer_log_table, a_rawdata_query_to_run)
    LOG.info("audit raw data query ... \n" + audit_insert_raw_query_to_run)
    DWH.execute(audit_insert_raw_query_to_run)
    LOG.info("raw-data entries logged... " + s3_transfer_log_table)


def _audit_curated_table(keyword, raw_db, raw_schema, monitor_db, monitor_schema):
    '''
    insert into curated-data-load-tracker from based on data inserts
    :param keyword: str
    :param raw_db: str
    :param raw_schema: str
    :param monitor_db: str
    :param monitor_schema: str
    :return: na
    '''
    a_curated_query_path = "queries/" + keyword + '_auditcur.sql'
    a_curated_query_to_run = jinja2.Template(_read_file(a_curated_query_path)).render(db_raw=raw_db,
                                                                                      schema_raw=raw_schema)
    raw_to_curated_log_table = monitor_db + "." + monitor_schema + "." + "RAW_TO_CURATED_LOG_AUDIT"
    audit_curation_log_query_to_run = INSERT_TABLE_FROM_QUERY_SQL.format(raw_to_curated_log_table,
                                                                         a_curated_query_to_run)
    LOG.info("curation audit query ... \n" + audit_curation_log_query_to_run)
    DWH.execute(audit_curation_log_query_to_run)
    LOG.info("curated data logged... " + raw_to_curated_log_table)


def _truncate_table(table, db, schema):
    table_nm = db + "." + schema + "." + table
    # truncate_query = TRUNCATE_TABLE_SQL.format(table_nm)
    # DWH.execute(truncate_query) TO:DO -- intentionally commented, when Production remove comment
    LOG.info("truncated table... " + table_nm)


def main():
    for dataset in table_names:

        # values from config
        data = dataset['dataset']
        tbl_nm = dataset['table'].upper()
        raw_db = dataset['raw_database'].upper()
        raw_schema = dataset['raw_schema'].upper()
        curate_db = dataset['curated_database'].upper()
        curate_schema = dataset['curated_schema'].upper()
        monitor_db = dataset['monitor_database'].upper()
        monitor_schema = dataset['monitor_schema'].upper()

        # incremental rows insert from raw
        LOG.info("procedure run for... " + data + " | env : " + str(db_name) + " | " + str(execution_timestamp))
        _execute_table_inserts(keyword=data, table=tbl_nm, raw_db=raw_db, raw_schema=raw_schema, curate_db=curate_db,
                               curate_schema=curate_schema)
        LOG.info("inserts complete... " + data)

        # log audit entries raw + curated
        _audit_raw_data_table(keyword=data, raw_db=raw_db, raw_schema=raw_schema, monitor_db=monitor_db,
                              monitor_schema=monitor_schema)
        LOG.info("raw - audit entries done... " + data)
        _audit_curated_table(keyword=data, raw_db=raw_db, raw_schema=raw_schema, monitor_db=monitor_db,
                             monitor_schema=monitor_schema)
        LOG.info("curation - audit entries done... " + data)

        # truncate raw table
        _truncate_table(table=tbl_nm, db=raw_db, schema=raw_schema)
        LOG.info("Done... " + data + " | " + str(execution_timestamp))


if __name__ == "__main__":
    main()
    LOG.info("All Done ;-) ")