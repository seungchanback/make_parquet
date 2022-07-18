from make_parquet import make_parquet
from concurrent import futures

date_list = [
    ["2022","04","07"],
    ["2022","04","09"],
    ["2022","04","10"]
]

with futures.ThreadPoolExecutor(max_workers=3) as executor:

    _ = [executor.submit(make_parquet,date[0],date[1],date[2]) for date in date_list]
