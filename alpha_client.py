import requests
import alpha_parser
import alpha_requestor
import logger
import pymongo
import time
import json
import pandas as pd
from bson.objectid import ObjectId

class alpha_client():
    def __init__(self, login, password, collection_simulate,
                 collection_prod, collection_trash, collection_purgatory,
                 mongo_connection_string, origin, xsrf,
                 site_address, proxies):
        self.login = login
        self.password = password
        self.mongo = pymongo.MongoClient(mongo_connection_string).wq
        self.origin = origin
        self.xsrf = xsrf
        self.collection_simulate = collection_simulate
        self.collection_prod = collection_prod
        self.collection_trash = collection_trash
        self.collection_purgatory = collection_purgatory

        self.client = requests.Session()
        self.site_address = site_address
        self.alpha_parser = alpha_parser.AlphaParser()
        self.requestor = alpha_requestor.Request(login=login,
                                                 password=password,
                                                 origin=origin,
                                                 xsrf=xsrf,
                                                 site_address=site_address,
                                                 proxies=proxies)
        self.logger = logger.Logger(mongo_connection_string)
        self.max_error_attempts = 5
        self.simulation_sleep = 0.5
        self.pause = 2

    # Usual usage: move alpha from purgatory to prod or trash
    def move_alpha_from_to(self, alpha, total_result, collection_old, collection_new, collection_simulate, inverse=False):
        self.mongo[collection_simulate].update({'Code': alpha['Code']}, {"$set": {'Status': 'Finished'}})
        alpha_curr = list(self.mongo[collection_old].find({'Index': int(alpha['Index'])}))

        if len(alpha_curr) == 0:
            msg = 'No such index'.format(alpha['index'])
            self.logger.log_print( info=msg, function_name='MoveAlpha')
            return

        alpha_curr = alpha_curr[0]
        alpha_curr = {k: v for k, v in alpha_curr.items() if k not in ['_id']}

        self.mongo[collection_new].insert(alpha_curr)
        self.mongo[collection_new].update({'Index': alpha['Index']}, {"$set": total_result})

        if inverse:
            self.mongo[collection_new].update({'Index': alpha['Index']}, {
                "$set": {'Code': '-{}'.format(alpha['Code'])}})

        self.mongo[collection_old].remove({'Index': alpha['Index']})
        self.mongo[collection_simulate].remove({'Code': alpha['Code']})

    # abs to all result's values
    def alpha_stats_abs(self, total_result):
        for key in total_result.keys():
            if ((type(total_result[key]) == type(1.0)) | (type(total_result[key]) == type(1))):
                total_result[key] = abs(total_result[key])
        return total_result

    def simulate_alphas(self, cookie, ids):
        print("Function simulating alphas started")
        i = 0
        error_attempts = 0
        run_attempts = 0

        alphas = list(self.mongo[self.collection_simulate].find({"_id": {"$in": ids}}))
        while i < len(alphas):
            if error_attempts < self.max_error_attempts:
                try:
                    alpha = alphas[i]['Alpha']
                    alpha_code = alphas[i]['Code']
                    print('Simulating alpha {}'.format(alpha_code))

                    response = json.loads(self.requestor.simulate_alpha(cookie=cookie,
                                                                        alpha=alpha).content)
                    # to do: delete
                    print(response)

                    if response['error'] == None:
                        alpha_index = response['result'][0]

                        def convert_key(key, old_key, new_key):
                            if key == old_key:
                                return new_key
                            else:
                                return key

                        data = {convert_key(key, "_id", "AlphaId"): value for key, value in alphas[i].items()}
                        data["Index"] = alpha_index

                        self.mongo[self.collection_purgatory].insert(data)
                        error_attempts = 0
                        run_attempts = 0
                        i += 1

                        time.sleep(self.simulation_sleep)

                    # To many alphas simulating error happen
                    elif response['error']['all'] == 'You have reached the limit of concurrent simulations. Please wait for the previous simulation(s) to finish.':
                        print('Too many alphas simulating')
                        time.sleep(self.pause * 6)

                    # Login error happen or site doesn't works
                    elif response['error'] == 'Expecting value: line 1 column 1 (char 0)':
                        msg = 'Login error happen or WorldQuant site doesnt work correctly'.format(response['error'])
                        self.logger.log_print(msg, function_name='AlphaSimulate')

                        error_attempts += 1
                        time.sleep(self.pause)

                    # Not proceeded error happen
                    else:
                        if (run_attempts == 0):
                            msg = 'Something went wrong, result of last request {}'.format(response)
                            self.logger.log_print(msg, function_name='AlphaSimulate')
                            error_attempts += 1
                            run_attempts += 1
                        else:
                            msg = 'Something went wrong, result of last request {}'.format(response)
                            self.logger.log_print(msg, function_name='AlphaSimulate')
                            i += 1
                            error_attempts += 1
                            run_attempts = 0

                except Exception as e:
                    msg = 'Exception occured while simulating alphas {}'.format(e)
                    self.logger.log_print(msg, function_name='AlphaSimulate')
                    error_attempts += 1
                    time.sleep(self.pause)
            else:
                print('Too many errors, function Simulate Alphas finished')
                break
        print("Function simulating alphas finished")
        print("")
        print("")

    def parse_alphas(self, cookie, ids, id_type=False):
        print("Function parsing alphas started")
        if id_type:
            alphas = list(self.mongo[self.collection_purgatory].find({"AlphaId": {"$in": ids}}))
        else:
            alphas = list(self.mongo[self.collection_purgatory].find({"_id": {"$in": ids}}))

        i = 0
        error_attempts = 0
        parse_attempts = 0
        sleep_attempts = 0

        while i < len(alphas):
            if error_attempts < self.max_error_attempts:
                try:
                    print('Parsing alpha {}'.format(alphas[i]['Code']))
                    stats = json.loads(self.requestor.stats_alpha(cookie=cookie,
                                                                  index=alphas[i]['Index']).content)
                    # to do: delete
                    print(stats)

                    try:
                        if (stats['error'] == '') & (stats['result'] == None) & (stats['status'] == False):
                            print("Deleting old alpha from purgatory {}".format(alphas[i]['Code']))
                            self.mongo[self.collection_purgatory].remove({'Code' : alphas[i]['Code']}, multi=True)
                            time.sleep(self.pause / 2)
                            i += 1

                        elif (stats['error'] == '') & (stats['status'] == True)& (len(stats['result']) > 0):
                            result = stats['result'][-1]
                            if abs(result['Fitness']) < 0.35:
                                self.mongo[self.collection_purgatory].remove({'Code': alphas[i]['Code']}, multi=True)
                                self.mongo[self.collection_simulate].remove({'Code': alphas[i]['Code']}, multi=True)
                                time.sleep(self.pause / 2)

                            elif abs(result['Fitness']) < 0.9:
                                inverse = result['Fitness'] < 0
                                result = self.alpha_stats_abs(total_result=result)
                                self.move_alpha_from_to(alpha=alphas[i],
                                                        total_result=result,
                                                        collection_old=self.collection_purgatory,
                                                        collection_new=self.collection_trash,
                                                        collection_simulate=self.collection_simulate,
                                                        inverse=inverse)
                            elif (abs(result['Fitness']) < 1.1) | (abs(result['Sharpe']) < 1.25):
                                inverse = result['Fitness'] < 0
                                result = self.alpha_stats_abs(total_result=result)
                                self.move_alpha_from_to(alpha=alphas[i],
                                                        total_result=result,
                                                        collection_old=self.collection_purgatory,
                                                        collection_new=self.collection_prod,
                                                        collection_simulate=self.collection_simulate,
                                                        inverse=inverse)
                            elif (result['Fitness'] > 1.1) & (result['Sharpe'] > 1.25):
                                inverse = False
                                self.move_alpha_from_to(alpha=alphas[i],
                                                        total_result=result,
                                                        collection_old=self.collection_purgatory,
                                                        collection_new=self.collection_prod,
                                                        collection_simulate=self.collection_simulate,
                                                        inverse=inverse)

                                alpha_id = json.loads(self.requestor.get_alphaid(cookie, alphas[i]['Index']).content)['result']['clientAlphaId']
                                print('AlphaId {}'.format(alpha_id))
                                submission_id = json.loads(self.requestor.get_submissionid(cookie, alpha_id).content)['result']['RequestId']
                                print('SubmissionId {}'.format(submission_id))
                                self.mongo[self.collection_prod].update({'Index': alphas[i]['Index']},
                                                                        {"$set": {"AlphaId": alpha_id,
                                                                                  "SubmissionId": submission_id,
                                                                          "SubmissionStatus": "InProgress"}})

                            elif (result['Fitness'] < - 1.1) & (result['Sharpe'] < -1.25):
                                self.mongo[self.collection_simulate].update({'Code': alphas[i]['Code']},
                                                                            {"$set": {"Code": "-{}".format(alphas[i]['Code']),
                                                                                      "Status": "Urgently"}})
                                self.mongo[self.collection_purgatory].remove({'Index': alphas[i]['Index']})
                            else:
                                msg = 'Not proceeded case with Fitness {} and Sharpe {}'.format(result['Fitness'],
                                                                                                result['Sharpe'])
                                self.logger.log_print(msg, function_name='AlphaParse')

                            i += 1
                            error_attempts = 0
                            sleep_attempts = 0
                            time.sleep(self.pause)
                            #  Case when alpha is not proceeded for a long period of time
                        elif ((stats['error'] == '') & (stats['status'] == True)):
                            msg = 'Alpha {} not finished simulation'.format(alphas[i]['Code'])
                            self.logger.log_print(msg, function_name='AlphaParse')

                            sleep_attempts += 1
                            time.sleep(3 * sleep_attempts)

                            if sleep_attempts > 2:
                                self.mongo[self.collection_purgatory].update({'Index': alphas[i]['Index']},
                                                                             {"$set": {"Comment": "Stats parsing timeout",
                                                                                      "Status": "Asleep"}})
                                sleep_attempts = 0
                                i += 1

                        else:
                            msg = 'Not proceeded error {}'.format(stats['error'])
                            self.logger.log_print(msg, function_name='AlphaParse')
                            error_attempts += 1
                            time.sleep(self.pause)

                    except Exception as e:
                        msg = 'Parse exception occured {}'.format(e)
                        self.logger.log_print(msg, function_name='AlphaParse')
                        parse_attempts += 1
                        if (parse_attempts > 3):
                            parse_attempts = 0
                            error_attempts += 1
                            i += 1
                        time.sleep(self.pause)

                except Exception as e:
                    msg = 'Local exception occured {}'.format(e)
                    self.logger.log_print(msg, function_name='AlphaParse')

                    error_attempts += 1
                    time.sleep(self.pause)

            else:
                msg = 'Too many errors, function Parse_Alphas finished'
                self.logger.log_print(msg, function_name='AlphaParse')
                print("Function parsing alphas finished")
                break

        print("Function parsing alphas finished")
        print("")
        print("")

    def parse_submissions(self, cookie, ids, ids_type = False):
        try:
            print("Function parse submissions started")
            if ids_type:
                alphas = list(self.mongo[self.collection_prod].find({"AlphaId": {"$in": ids}}))
            else:
                alphas = list(self.mongo[self.collection_prod].find({"_id": {"$in": ids}}))

            i = 0
            error_attempts = 0
            sleep_attempts = 0

            while i < len(alphas):
                if error_attempts < self.max_error_attempts:
                    print('Parsing submission of alpha with index {}'.format(alphas[i]['Index']))
                    try:
                        submission = self.requestor.get_submission_result(cookie, alphas[i]['SubmissionId'])
                        result = json.loads(submission.content)
                        print(result)
                        if result['status'] == False:
                            print('Alpha too old for submission')
                            self.mongo[self.collection_prod].update({'Index': alphas[i]['Index']},
                                                                    {"$set": {"SubmissionStatus": 'OldInProgress'}})
                            i += 1
                        elif result['result'] == None:
                            self.mongo[self.collection_prod].update({'Index': alphas[i]['Index']},
                                                                    {"$set": {"Submission": result['status'],
                                                                              "SubmissionInfo": result['error'],
                                                                              "SubmissionStatus": 'Finished'}})

                            sleep_attempts = 0
                            error_attempts = 0
                            i += 1
                        else:
                            if result['result']['InProgress'] == True:
                                sleep_attempts += 1
                                time.sleep(5 * sleep_attempts)
                                if (sleep_attempts > 5):
                                    self.mongo[self.collection_prod].update({'Index': alphas[i]['Index']},
                                                                    {"$set": {"Comment": "Submission timeout error",
                                                                              "SubmissionStatus": 'Finished'}})
                                    i += 1
                            else:
                                msg = 'Not proceeded error happen {}'.format(result)
                                self.logger.log_print(msg, function_name='AlphaParse')
                                error_attempts += 1

                    except Exception as e:
                        msg = 'Exception occured while submission{}'.format(e)
                        self.logger.log_print(msg, function_name='AlphaParse')
                        error_attempts += 1
                else:
                    msg = 'Too many errors, function Parse_Submission finished'
                    self.logger.log_print(msg, function_name='AlphaParse')
                    break
            print("Function parse submissions finished")
        except Exception as e:
            print("Function parse submissions finished with critical error {e}".format(e))