from read_queue import read_queue_and_make_parquet
from concurrent import futures
from multiprocessing import Process



def main():
    
    queue_param_dict = {'queue_name' : "dev-cdata2-datatime.fifo",
                            'from_bucket_name' : "storelink-prod-fstore-src",
                            'from_bucket_prefix' : "cData_day2",
                            'to_bucket_name' : "storelink-data-etl-dev",
                            'to_bucket_prefix' : "cdata_day2_etl"}
    
    procs = []
    for num in range(1,3):
        proc = Process(target=read_queue_and_make_parquet,args=(queue_param_dict,),daemon=True)
        procs.append(proc)
    
    for num in range(1,3):
        procs[num-1].start()
    
    for proc in procs:
        proc.join()
    # with futures.ProcessPoolExecutor(max_workers=3) as executor:
        
    #     queue_param_dict = {'queue_name' : "dev-cdata2-datatime.fifo",
    #                         'from_bucket_name' : "storelink-prod-fstore-src",
    #                         'from_bucket_prefix' : "cData_day2",
    #                         'to_bucket_name' : "storelink-data-etl-dev",
    #                         'to_bucket_prefix' : "cdata_day2_etl"}

    #     process1 = executor.submit(read_queue_and_make_parquet,
    #                                 queue_param_dict)
    #     process2 = executor.submit(read_queue_and_make_parquet,
    #                                 queue_param_dict)
    #     process3 = executor.submit(read_queue_and_make_parquet,
    #                                 queue_param_dict)
    
    

if __name__=='__main__':
    main()
    from send_discord import send_discord_msg
    send_discord_msg("""
            Parquet 변경 끝 !!
    """)