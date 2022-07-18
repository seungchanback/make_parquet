from make_parquet_2 import make_parquet
from concurrent import futures

date_list = [
    ["2022","04","02"],
    ["2022","04","03"],
    ["2022","04","04"],
    ["2022","04","06"],
    ["2022","04","07"],
    ["2022","04","08"],
    ["2022","04","09"],
    ["2022","04","10"],
    ["2022","04","11"]
]

with futures.ThreadPoolExecutor(max_workers=3) as executor:

    _ = [executor.submit(make_parquet,date[0],date[1],date[2]) for date in date_list]
