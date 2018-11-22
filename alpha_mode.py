import requests
import alpha_parser
import alpha_client
import alpha_requestor
import utils
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
        self.utils = utils.Utils(user_name=self.user_name)


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
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []

        self.client.parse_alphas(cookie, ids)

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

    def parse_pack(self, pack_number=500):
        cookie = self.requestor.log_in().cookies
        # ALPHAS PACK SIMULATION
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
            if (len(alphas) != 0):
                ids = list(alphas['_id'])
            else:
                ids = []
        self.client.parse_alphas(cookie, ids)
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

    def touch_mode(self, iteration=1, pack_number=40, trash_iteration=12):
        cookie = self.requestor.log_in().cookies

        # ALPHAS PACK SIMULATION FOR UPDATE
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas = pd.DataFrame(list(mongo['alphas_prod'].find({'Sharpe': {'$gte': 0.8},
                                                              'Executor': self.user_name,
                                                              'IsTuch': True,
                                                              'Iteration': iteration-1}).limit(pack_number)))
        mongo[self.collection_prod].update({'_id': {'$in': [dict(alphas.iloc[i])['_id'] for i in range(alphas.shape[0])]}},
                                           {"$set": {'IsTuch': False}})

        # BUILD NEW ALPHAS
        upgrade_alphas = []
        for i in range(alphas.shape[0]):
            alpha = dict(alphas.iloc[i])
            upgrade_alphas += self.utils.random_trash(alpha=alpha, iteration=iteration, pack_number=trash_iteration)

        print(upgrade_alphas)
        # INSERT NEW ALPHAS
        mongo[self.collection_simulate].insert(upgrade_alphas)

        # SIMULATE NEW ALPHAS
        alphas_simulate = pd.DataFrame(
            list(
                mongo[self.collection_simulate].find({'Status': 'InTuch', 'Executor': self.user_name}).limit(pack_number * trash_iteration)))
        if (len(alphas_simulate) != 0):
            ids = list(alphas_simulate['_id'])
        else:
            ids = []
        self.client.simulate_alphas(cookie, ids)

        # PARSE NEW ALPHAS
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas_parse = pd.DataFrame(
            list(mongo['alphas_purgatory'].find({'Status': 'InTuch', 'Executor': self.user_name}).limit(
                pack_number * trash_iteration)))
        if (len(alphas_parse) != 0):
            ids = list(alphas_parse['_id'])
        else:
            ids = []
        self.client.parse_alphas(cookie, ids)

        # SUBMIT NEW ALPHAS
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas_submit = pd.DataFrame(list(mongo['alphas_prod'].find(
            {'Status': 'InTuch', 'Executor': self.user_name, "SubmissionId": {"$gte": 0}}).limit(
            pack_number * trash_iteration)))
        if (len(alphas_submit) != 0):
            ids = list(alphas_submit['_id'])
        else:
            ids = []

        self.client.parse_submissions(cookie, ids)

        # Build logic names
        if (iteration == 1):
            logic_names = [dict(alphas.iloc[i])['Code'] for i in range(alphas.shape[0])]
        else:
            logic_names = [dict(alphas.iloc[i])['LogicName'] for i in range(alphas.shape[0])]
        print(logic_names)

        # Remove everything in trash
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        mongo[self.collection_trash].remove({'Executor': self.user_name, 'Code': {"$in": [x['Code'] for x in upgrade_alphas]}}, multi=True)

        for logic_name in logic_names:
            print(logic_name)
            cur_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                'LogicName': logic_name,
                                                                'Iteration': iteration})))
            print(cur_res.shape)
            if (iteration == 1):
                old_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                     'Code': logic_name,
                                                                     'Iteration': iteration - 1})))
            else:
                old_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                     'LogicName': logic_name,
                                                                     'Iteration': iteration - 1})))
            print(old_res.shape)
            if (cur_res.shape[0] > 0):
                cur_max_sharpe=cur_res['Sharpe'].max()
                old_max_sharpe=old_res['Sharpe'].max()

                print('LogicName - {}, New iteration max sharpe - {}, Old Iteration max sharpe {}'.format(logic_name,
                                                                                                          cur_max_sharpe,
                                                                                                          old_max_sharpe))
                if (cur_max_sharpe < old_max_sharpe + 0.04):
                    mongo[self.collection_prod].remove({'Executor': self.user_name,
                                                        'LogicName':  logic_name,
                                                        'Iteration': iteration}, multi=True)
                else:
                    mongo[self.collection_prod].remove({'Executor': self.user_name,
                                                        'LogicName': logic_name,
                                                        'Iteration': iteration,
                                                        'Sharpe': {'$lt': cur_max_sharpe}}, multi=True)




        self.requestor.log_out(cookie)

    def mix_mode(self, iteration=1, pack_number=40, mix_iteration=12):
        cookie = self.requestor.log_in().cookies

        # ALPHAS PACK SIMULATION FOR UPDATE
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas = pd.DataFrame(list(mongo['alphas_prod'].find({'Sharpe': {'$gte': 0.7},
                                                              'Executor': self.user_name,
                                                              'ShortCount': {'$gte': 30},
                                                              'LongCount': {'$gte': 30},
                                                              'IsMix': True,
                                                              'Iteration': iteration-1}).limit(pack_number)))
        mongo[self.collection_prod].update({'_id': {'$in': [dict(alphas.iloc[i])['_id'] for i in range(alphas.shape[0])]}},
                                           {"$set": {'IsMix': False}})

        # BUILD NEW ALPHAS
        upgrade_alphas = []
        for i in range(alphas.shape[0]):
            alpha = dict(alphas.iloc[i])
            upgrade_alphas += self.utils.mix_trash(alpha=alpha, iteration=iteration, pack_number=mix_iteration)

        print(upgrade_alphas)
        # INSERT NEW ALPHAS
        mongo[self.collection_simulate].insert(upgrade_alphas)

        # SIMULATE NEW ALPHAS
        alphas_simulate = pd.DataFrame(
            list(
                mongo[self.collection_simulate].find({'Status': 'InMix', 'Executor': self.user_name})
                    .limit(pack_number * mix_iteration)))
        if (len(alphas_simulate) != 0):
            ids = list(alphas_simulate['_id'])
        else:
            ids = []
        self.client.simulate_alphas(cookie, ids)

        # PARSE NEW ALPHAS
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas_parse = pd.DataFrame(
            list(mongo['alphas_purgatory'].find({'Status': 'InMix', 'Executor': self.user_name})
                .limit(pack_number * mix_iteration)))
        if (len(alphas_parse) != 0):
            ids = list(alphas_parse['_id'])
        else:
            ids = []
        self.client.parse_alphas(cookie, ids)

        # SUBMIT NEW ALPHAS
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        alphas_submit = pd.DataFrame(list(mongo['alphas_prod'].find(
            {'Status': 'InMix', 'Executor': self.user_name, "SubmissionId": {"$gte": 0}}).limit(
            pack_number * mix_iteration)))
        if (len(alphas_submit) != 0):
            ids = list(alphas_submit['_id'])
        else:
            ids = []

        self.client.parse_submissions(cookie, ids)

        # Build logic names
        if (iteration == 1):
            logic_names = [dict(alphas.iloc[i])['Code'] for i in range(alphas.shape[0])]
        else:
            logic_names = [dict(alphas.iloc[i])['LogicName'] for i in range(alphas.shape[0])]
        print(logic_names)

        # Remove everything in trash
        mongo = pymongo.MongoClient(self.mongo_connection_string).wq
        mongo[self.collection_trash].remove({'Executor': self.user_name, 'Code': {"$in": [x['Code'] for x in upgrade_alphas]}}, multi=True)

        for logic_name in logic_names:
            print(logic_name)
            cur_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                'LogicName': logic_name,
                                                                'Iteration': iteration})))
            print(cur_res.shape)
            if (iteration == 1):
                old_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                     'Code': logic_name,
                                                                     'Iteration': iteration - 1})))
            else:
                old_res = pd.DataFrame(list(mongo[self.collection_prod].find({'Executor': self.user_name,
                                                                     'LogicName': logic_name,
                                                                     'Iteration': iteration - 1})))
            print(old_res.shape)
            if (cur_res.shape[0] > 0):
                cur_max_sharpe=cur_res['Sharpe'].max()
                old_max_sharpe=old_res['Sharpe'].max()

                print('LogicName - {}, New iteration max sharpe - {}, Old Iteration max sharpe {}'.format(logic_name,
                                                                                                          cur_max_sharpe,
                                                                                                          old_max_sharpe))
                if (cur_max_sharpe < old_max_sharpe + 0.04):
                    mongo[self.collection_prod].remove({'Executor': self.user_name,
                                                        'LogicName':  logic_name,
                                                        'Iteration': iteration}, multi=True)
                else:
                    mongo[self.collection_prod].remove({'Executor': self.user_name,
                                                        'LogicName': logic_name,
                                                        'Iteration': iteration,
                                                        'Sharpe': {'$lt': cur_max_sharpe}}, multi=True)




        self.requestor.log_out(cookie)