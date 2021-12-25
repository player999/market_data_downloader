from lxml import etree
import urllib.request
import zipfile
import os
import time
import sys

BINANCE_URL_PREFIX="https://data.binance.vision/"
FILE_LIST_URL_TEMPLATE="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/monthly/klines/%s/15m/"

def get_pair_url(pair: str)->str:
    return FILE_LIST_URL_TEMPLATE%(pair)

def extract_pair_urls(data: str)->list[str]:
    data = etree.fromstring(data.encode("utf-8"))
    res = map(lambda x: BINANCE_URL_PREFIX + x.text, data.xpath("//*[name() = 'Key']"))
    res = list(filter(lambda x: "CHECKSUM" not in x, res))
    return res


def get_list_of_pair_files(pair: str)->list[str]:
    url = get_pair_url(pair)
    response = urllib.request.urlopen(url).read().decode()
    return extract_pair_urls(response)

def extract_data_from_zips(dir: str):
    files = os.listdir(dir)
    resulting_data = []
    for fle in files:
        if ".zip" not in fle:
            continue
        zf = zipfile.ZipFile(dir+fle)
        dat = zf.read(fle[:-3] + "csv").decode("utf-8")
        resulting_data.extend(dat.split("\n"))
    def extract_stamp(line: str)->int:
        return int(line.split(",")[0])
    resulting_data = list(filter(lambda x: x != "", resulting_data))
    resulting_data.sort(key=extract_stamp)
    text = "\n".join(resulting_data)
    with open(dir + "total.csv", "w") as f:
        f.write(text)

def download_zip_files(zipfiles: list[str])->str:
    dir_path = "downloads/" + str(int(time.time())) + "/"
    os.makedirs(dir_path, mode=511, exist_ok=True)
    for url in zipfiles:
        fname = url.split("/")[-1]
        response = urllib.request.urlopen(url).read()
        with open(dir_path + fname, "wb") as f:
            f.write(response)
    return dir_path

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Failure!")
    files = get_list_of_pair_files(sys.argv[1])
    down_dir = download_zip_files(files)
    extract_data_from_zips(down_dir)
