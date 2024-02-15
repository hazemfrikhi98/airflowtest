from pyspark.sql import SparkSession
import os
from urllib.parse import urljoin
import requests

EDH_USER = "nslt4939_muxmuxc"
EDH_PASSWORD = os.getenv("DOCKER_PW")
print(os.environ)

base_url = "https://edhu-portal.nor.fr.gin.intraorange:443/gateway/default/webhdfs/v1/"
hdfs_path = ("UAT/use_case/MUXC/WATCH_METRICS/year=2023/month=05/day=01/hour=00/minute=00/"
             "part-00000-9f65511a-0813-4a01-91ea-10ffd7b40629-c000.snappy.orc")
operation = "?op=OPEN"
url_file = urljoin(base_url, hdfs_path + operation)

local_filename = hdfs_path.split('/')[-1]
with requests.get(url_file, stream=True, auth=(EDH_USER, EDH_PASSWORD), verify=False) as r:
    r.raise_for_status()
    print(f'reading: {url_file}')
    print(f'writing to: {local_filename}')
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

spark = SparkSession.builder.getOrCreate()
df = spark.read.orc(local_filename)
df.show()

print(df.show())
print(df.head())
