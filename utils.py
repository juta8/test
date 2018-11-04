import requests
import alpha_parser
import alpha_client
import alpha_requestor
import logger
import pymongo
import time
import json
import pandas as pd
from bson.objectid import ObjectId

class Utils:
    def __init__(self, user_name):
        self.user_name = user_name
        config_fn = "config.json"
        with open(config_fn, 'r') as f:
            config = json.load(f)
        self.mongo_connection_string = config['mongodb_connection_string']
        origin = config['origin']
        site_address = 'https://www.worldquantvrc.com'
        info = [x for x in config['users'] if x['name'] == user_name]
        self.collection_purgatory = 'alphas_purgatory'
        self.collection_trash = 'alphas_trash'
        self.collection_simulate = 'alphas_simulate'
        self.collection_prod = 'alphas_prod'
        login = info[0]['login']
        password = info[0]['password']
        xsrf = info[0]['xsrf']
        proxies = info[0]['proxies']

        self.client = alpha_client.alpha_client(login=login,
                                                password=password,
                                                collection_prod=self.collection_prod,
                                                collection_simulate=self.collection_simulate,
                                                collection_trash=self.collection_trash,
                                                collection_purgatory=self.collection_purgatory,
                                                mongo_connection_string=self.mongo_connection_string,
                                                origin=origin,
                                                xsrf=xsrf,
                                                site_address=site_address,
                                                proxies=proxies)

        self.requestor = alpha_requestor.Request(login=login,
                                                 password=password,
                                                 origin=origin,
                                                 xsrf=xsrf,
                                                 site_address=site_address,
                                                 proxies=proxies)
        self.mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        self.alpha_parser = alpha_parser.AlphaParser()


    def random_trash(self, alpha, iteration, pack_number=12):
        df = pd.DataFrame(
            list(self.mongo['alphas_prod'].aggregate([{"$match": {"Executor": self.user_name, "Region": alpha['Region']}},
                                                 {"$sample": {"size": pack_number}}])))
        alphas = list(df['Code'])
        new_alphas_code = ['indneutralize(rank({}), 5 * rank({}) + 0.5)'.format(alpha['Code'], x) for x in alphas]
        new_alphas = [self.alpha_parser.build_alpha(x, univid=alpha['Universe'],
                                               region=alpha['Region'],
                                               opneut=alpha['Neutralization'],
                                               decay="12") for x in new_alphas_code]
        alphas = []
        for i in range(len(new_alphas)):
            if (iteration == 1):
                alpha_logic=alpha['Code']
            else:
                alpha_logic=alpha['LogicName']
            alpha_info = self.alpha_parser.report_alpha(alpha=new_alphas[i], alpha_code=new_alphas_code[i],
                                                   alpha_executor=self.user_name,
                                                   alpha_type=alpha['Type'],
                                                   logic_name=alpha_logic,
                                                   region=alpha['Region'],
                                                   universe=alpha['Universe'],
                                                   neutr=alpha['Neutralization'],
                                                   status="InTuch",
                                                   iteration=iteration)

            alphas.append(alpha_info)
        return alphas

