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

class AlphaMode:
    def __init__(self, user_name):
        self.user_name = user_name
        config_fn = "config.json"
        with open(config_fn, 'r') as f:
            config = json.load(f)
        self.mongo_connection_string = config['mongodb_connection_string']
        self.origin = config['origin']
        self.site_address = 'https://www.worldquantvrc.com'
        self.info = [x for x in config['users'] if x['name'] == user_name]
        self.collection_purgatory = 'alphas_purgatory'
        self.collection_trash = 'alphas_trash'
        self.collection_simulate = 'alphas_simulate'
        self.collection_prod = 'alphas_prod'
        self.login = self.info[0]['login']
        self.password = self.info[0]['password']
        self.xsrf = self.info[0]['xsrf']
        self.proxies = self.info[0]['proxies']

        self.client = alpha_client.alpha_client(login=self.login,
           password=self.password,
           collection_prod=self.collection_prod,
           collection_simulate=self.collection_simulate,
           collection_trash=self.collection_trash,
           collection_purgatory=self.collection_purgatory,
           mongo_connection_string=self.mongo_connection_string,
           origin=self.origin,
           xsrf=self.xsrf,
           site_address=self.site_address,
           proxies=self.proxies)

        self.requestor = alpha_requestor.Request(login=self.login,
            password=self.password,
            origin=self.origin,
            xsrf=self.xsrf,
            site_address=self.site_address,
            proxies=self.proxies)


    def simulate_base_pack(self, pack_number=500):
        cookie = self.requestor.log_in().cookies
        # ALPHAS PACK SIMULATION
        try:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(
                list(mongo['alphas_simulate'].find({'Status': 'InProgress', 'Executor': self.user_name}).limit(pack_number)))
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        except:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(
                list(mongo['alphas_simulate'].find({'Status': 'InProgress', 'Executor': self.user_name}).limit(pack_number)))
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        self.client.simulate_alphas(cookie, ids)

        # ALPHAS PACK PARSING
        try:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(
                list(mongo['alphas_purgatory'].find({'Status': 'InProgress', 'Executor': self.user_name}).limit(pack_number)))
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        except:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(
                list(mongo['alphas_purgatory'].find({'Status': 'InProgress', 'Executor': self.user_name}).limit(pack_number)))
            ids = list(alphas['_id'])
        if (len(alphas) != 0):
            ids = list(alphas['_id'])
        else:
            ids = []

        # APLHAS PACK SUBMISSION
        try:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(list(mongo['alphas_prod'].find({'Status': 'InProgress',
                                                                  'Executor': 'da',
                                                                  'SubmissionId':
                                                                      {"$gte": 0}}).limit(pack_number)))
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        except:
            mongo = pymongo.MongoClient(self.mongo_connection_string).wq
            alphas = pd.DataFrame(list(mongo['alphas_prod'].find(
                {'Status': 'InProgress', 'Executor': self.user_name, "SubmissionId": {"$gte": 0}}).limit(pack_number)))
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        self.client.parse_submissions(cookie, ids)

        self.requestor.log_out(cookie)

    def simulate(self, alpha, cookie, is_login=False, is_logout=False):
        alpha_success_submit=False
        if is_login:
            cookie = self.requestor.log_in().cookies

        simulate_response = json.loads(self.requestor.simulate_alpha(cookie=cookie, alpha=alpha).content)

        if simulate_response['error'] == None:
            alpha_index = simulate_response['result'][0]

        simulate_attempts = 0
        simulate_result = False
        while simulate_attempts < 30:
            time.sleep(5)
            progress =  self.requestor.progress_alpha(cookie, alpha_index).content.decode('utf8')
            print(progress, simulate_attempts)
            if progress == '"DONE"':
                simulate_result = True
                break
            simulate_attempts += 1

        if simulate_result:
            alpha_id = json.loads(self.requestor.get_alphaid(cookie= cookie, index=alpha_index).content)['result'][
                'clientAlphaId']
            submission_id = json.loads(self.requestor.get_submissionid(cookie=cookie, alphaid=alpha_id).content)['result']['RequestId']
            submit_attempts = 0
            while submit_attempts < 20:
                time.sleep(5)
                submission = self.requestor.get_submission_result(cookie=cookie, submissionid=submission_id)
                result = json.loads(submission.content)
                if result['result']==None:
                    alpha_success_submit = True
                submit_attempts += 1

        if is_logout:
            self.requestor.log_out(cookie)

        return alpha_success_submit
