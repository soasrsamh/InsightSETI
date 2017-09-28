import random
import sys
import time
import six
#from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
        #price_field = random.randint(800,1400)
        msg_cnt = 0
        start = 50
        for i in range(10): #for observation groups 13 through 13+range
            #time.sleep(10) #waits between observation groups
            for x in range(30): #1500 means about 1000 per obs because there are 4 producers
                time.sleep(4) # 0.2 waits this many seconds before producing another message about 1000 each obs each 5  min
                observationgroup_field = random.randint(start+i,start+i);
                observationorder_field = random.randint(1,6) 
                frequency_field = random.random()*10000
                snr_field = random.random()*100
                driftrate_field = random.random()-random.random()
                uncorrectedfrequency_field = random.random()-random.random()+frequency_field
                str_fmt = "{};{};{};{};{};{};{}"
                message_info = str_fmt.format(source_symbol,
                                              observationgroup_field,
                                              observationorder_field,
                                              frequency_field,
                                              snr_field,
                                              driftrate_field,
                                              uncorrectedfrequency_field)
                print message_info
                self.producer.send_messages('gbthits', source_symbol, message_info)
                msg_cnt += 1
                

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 
