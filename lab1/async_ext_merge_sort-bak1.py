# -*- coding: utf-8 -*-

import sys
import os
import logging

import asyncio
import time

# confiure log
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True


#configure path
root_dir = os.path.dirname(os.path.abspath('.'))
input_dir = root_dir + '/lab1/input/'
output_dir = root_dir + '/lab1/output/'

#define data to read and sort
data = []


#read single file
async def readSingleFile(fineName: str):
    inputFile = open(fineName, "r")
    if inputFile.mode == 'r':
        while True:
            num = inputFile.readline()
            if not num: #end of the file
                break
            num = num.strip()
            data.append(int(num))
    else:
        logger.error(f"Can't read the file:{fileName}! Please check.")
    inputFile.close()

    pass


#read-data controller
async def readAllData():
    res = await asyncio.gather(
        *(readSingleFile(f"{input_dir}unsorted_{str(i+1)}.txt") for i in range(10)))
    return res
 

#write data to the output file
def writeSortedData(fileName:str):
 
    outputFile = open(fileName, "w")
    if outputFile.mode == 'w':
        for i in range(len(data)):
            outputFile.write(f"{str(data[i])}\n")
    else:
        logger.error(f"Can't write the file:{fileName}! Please check.")
    outputFile.close()

    pass


def sort():
    logger.debug(f"check root_dir:{root_dir}")
    logger.debug(f"check input_dir:{input_dir}")
    logger.debug(f"check output_dir:{output_dir}")

    #read unsorted data
    asyncio.run(readAllData())

    #sort data
    data.sort()
    logger.info(data)
    logger.info(f"there are totally {len(data)} numbers.")

    #write sorted data
    if os.path.exists(output_dir) == False:
        os.makedirs(output_dir)
    fileName = f"{output_dir}sorted.txt"
    writeSortedData(fileName)

    pass


if __name__ == "__main__":
    s = time.perf_counter()
    sort()
    elapsed = time.perf_counter() - s
    logger.info(f"{__file__} executed in {elapsed:0.4f} seconds.")
