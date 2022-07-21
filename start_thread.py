from read_queue import read_queue_and_make_parquet
from concurrent import futures




with futures.ThreadPoolExecutor(max_workers=3) as executor:
    read_queue_and_make_parquet()
    queue_name = "dev-cdata2-datatime.fifo",
    from_bucket_name = "storelink-prod-fstore-src",
    from_bucket_prefix = "cData_day2",
    to_bucket_name = "storelink-data-etl-dev",
    to_bucket_prefix = "cdata_day2_etl"
    _ = [executor.submit(read_queue_and_make_parquet,
                                queue_name,
                                from_bucket_name,
                                from_bucket_prefix,
                                to_bucket_name,
                                to_bucket_prefix) for date in date_list]


        # raw_bucket_name = 'storelink-prod-fstore-src'
        # raw_bucket_prefix = string.Template("$prefix/$year/$month/$day")

        # output_bucket_name = 'storelink-data-etl-dev'
        # output_bucket_prefix = string.Template("$prefix/$year/$month/$day")
        # bucket_prefix = RAW_BUCKET_PREFIX.substitute(prefix = 'cData_day2',
        #                                             year = year,
        #                                             month = month,
        #                                             day = day)
        # output_bucket_prefix = OUTPUT_BUCKET_PREFIX.substitute(prefix = 'cData_day2_etl',
        #                                                         year = year,
        #                                                         month = month,
        #                                                         day = day)

        # output_dir_path = f"s3://{OUTPUT_BUCKET_NAME}/{output_bucket_prefix}"
