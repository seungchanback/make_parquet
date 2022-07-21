from read_queue import read_queue_and_make_parquet
from concurrent import futures



def main():
    with futures.ProcessPoolExecutor(max_workers=3) as executor:
        
        queue_param_dict = {'queue_name' : "dev-cdata2-datatime.fifo",
                            'from_bucket_name' : "storelink-prod-fstore-src",
                            'from_bucket_prefix' : "cData_day2",
                            'to_bucket_name' : "storelink-data-etl-dev",
                            'to_bucket_prefix' : "cdata_day2_etl"}

        process1 = executor.submit(read_queue_and_make_parquet,
                                    queue_param_dict)
        process2 = executor.submit(read_queue_and_make_parquet,
                                    queue_param_dict)
        process3 = executor.submit(read_queue_and_make_parquet,
                                    queue_param_dict)
    
    from send_discord import send_discord_msg
    send_discord_msg("""
            Parquet 변경 끝 !!
    """)

if __name__=='__main__':
    main()