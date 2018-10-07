import pymongo
import time

class Logger():
    def __init__(self, connection_string):
        # self.connection_string = 'mongodb://juta_8:kloppolk_2018@ds119110.mlab.com:19110/wq'
        # self.connection_string = 'mongodb://dalisa1212:kloppolk_2017@wqserver-shard-00-00-ftqza.mongodb.net:27017,wqserver-shard-00-01-ftqza.mongodb.net:27017,wqserver-shard-00-02-ftqza.mongodb.net:27017/wq?ssl=true&replicaSet=WQServer-shard-0&authSource=admin&retryWrites=true'

        self.connection_string = connection_string
        self.mongo = pymongo.MongoClient(mongo_connection_string).wq
        self.mongo_log = 'log'

    def log_print(self, msg, function_name, log_level = 'Error', print_msg = True):
        try:
            log_info = {}
            log_info['LogTime'] = str(datetime.utcfromtimestamp(time.time()))
            log_info['FunctionName'] = function_name
            log_info['LogInfo'] = msg
            log_info['LogLevel'] = log_level
            self.mongo[self.mongo_log].insert(log_info)
            if print_msg:
                print(msg)
        except Exception as e:
            print('Exception occured while printing log to MongoDB {e}'.format(e))



