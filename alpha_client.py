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

    # abs to all result's values
    def alpha_stats_abs(self, total_result):
        for key in total_result.keys():
            if ((type(total_result[key]) == type(1.0)) | (type(total_result[key]) == type(1))):
                total_result[key] = abs(total_result[key])
        return total_result

    def simulate_alphas(self, cookie, ids):
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
                        time.sleep(self.pause)

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

    def parse_alphas(self, cookie, ids, id_type=False):
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

                    try:
                        if (stats['error'] == '') & (stats['status'] == True)& (len(stats['result']) > 0):
                            result = stats['result'][-1]
                            if abs(result['Fitness']) < 0.8:
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
                break

    def parse_submissions(self, cookie, ids, ids_type = False):
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
                    if result['result'] == None:
                        self.mongo[self.collection_prod].update({'Index': alphas[i]['Index']},
                                                                {"$set": {"Submission": res['status'],
                                                                          "SubmissionInfo": res['error'],
                                                                          "SubmissionStatus": 'Finished'}})

                        sleep_attempts = 0
                        error_attempts = 0
                        i += 1
                    else:
                        if res['result']['InProgress'] == True:
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


    # # simulate alphas
    # def simulate_alphas(self, cookie, alphas, alphas_iteration=None):
    #     # to do: sleeping time
    #     i = 0
    #     error_attempts = 0
    #     running_alphas = []
    #
    #
    #     while i < len(alphas):
    #         if (error_attempts < self.max_error_attempts):
    #             try:
    #                 print('Simulating alpha {}'.format(json.loads(alphas[i])[0]['code']))
    #                 build_response = json.loads(self.requestor.simulate_alpha(cookie, alphas[i]).content)
    #
    #                 # Add alpha to Purgatery and try to simulate next alpha
    #                 if (build_response['error'] == None):
    #                     alpha_info, mongo_alpha_info = self.alpha_parser.simulating_alpha(alphas[i], build_response, alphas_iteration)
    #                     running_alphas.append(alpha_info)
    #                     self.mongo[self.collection_purgatory].insert(mongo_alpha_info)
    #
    #                     error_attempts = 0
    #                     i += 1
    #
    #                     time.sleep(self.simulation_sleep)
    #
    #                 # Too many alphas simulating
    #                 elif (build_response['error']['all'] == 'You have reached the limit of concurrent simulations. Please wait for the previous simulation(s) to finish.'):
    #                     print('Too many alphas simulating')
    #                     time.sleep(5)
    #
    #                 # Some error happen
    #                 elif (build_response['error'] == 'Expecting value: line 1 column 1 (char 0)'):
    #                     print('Local error {}'.format(build_response['error']))
    #                     error_attempts += 1
    #                     time.sleep(self.pause)
    #
    #                 # Not proceeded error happen
    #                 else:
    #                     print('Something went wrong, result of last request {}'.format(build_response))
    #                     error_attempts += 1
    #
    #             # Exception occured
    #             except Exception as e:
    #                 print('Local error happen {}'.format(e))
    #                 error_attempts +=1
    #                 time.sleep(self.pause)
    #         else:
    #             print('Too many errors, function Simulate Alphas finished')
    #             break
    #     return running_alphas


    # def parse_alphas(self, cookie, running_alphas):
    #     i = 0
    #
    #     error_attempts = 0
    #     parse_attempts = 0
    #     sleep_attempts = 0
    #
    #     submission_ids = []
    #     reverse_alphas = []
    #
    #     while (i < len(running_alphas)):
    #         if (error_attempts < self.max_error_attempts):
    #             try:
    #                 print('Parsing alpha {}'.format(json.loads(running_alphas[i]['code'])[0]['code']))
    #                 stats = json.loads(self.requestor.stats_alpha(cookie, running_alphas[i]['index']).content)
    #
    #                 try:
    #                     if ((stats['error'] == '') & (stats['status'] == True) & (len(stats['result']) > 0)):
    #                         total_result = stats['result'][-1]
    #
    #                         # case trash alpha
    #                         if (abs(total_result['Fitness']) <= 1):
    #                             is_inverse = (total_result['Fitness'] < 0)
    #                             total_result = self.alpha_stats_abs(total_result=total_result)
    #                             self.move_alpha_from_to(alpha=running_alphas[i],
    #                                                     total_result=total_result,
    #                                                     collection_old=self.collection_purgatory,
    #                                                     collection_new=self.collection_trash,
    #                                                     collection_simulate=self.collection_simulate,
    #                                                     inverse=is_inverse)
    #
    #                         # case good alpha but not enought
    #                         elif ((abs(total_result['Fitness']) < 1.1) | (abs(total_result['Sharpe']) < 1.25)):
    #                             is_inverse = total_result['Fitness'] < 0
    #                             total_result = self.alpha_stats_abs(total_result=total_result)
    #                             self.move_alpha_from_to(alpha=running_alphas[i],
    #                                                     total_result=total_result,
    #                                                     collection_old=self.collection_purgatory,
    #                                                     collection_new=self.collection_prod,
    #                                                     collection_simulate=self.collection_simulate,
    #                                                     inverse=is_inverse)
    #
    #                         # case very good alpha
    #                         elif ((total_result['Fitness'] > 1.1) & (total_result['Sharpe'] > 1.25)):
    #                             self.move_alpha_from_to(alpha=running_alphas[i],
    #                                                     total_result=total_result,
    #                                                     collection_old=self.collection_purgatory,
    #                                                     collection_new=self.collection_prod,
    #                                                     collection_simulate=self.collection_simulate,
    #                                                     inverse=is_inverse)
    #                             alpha_id = json.loads(self.alpha_parser.get_alphaid(cookie, running_alphas[i]['index']).content)['result']['clientAlphaId']
    #                             submission_id = json.loads(self.get_submissionid(cookie, alpha_id).content)['result']['RequestId']
    #                             submission_info = {}
    #                             self.mongo[self.collection_prod].update({'Index': running_alphas[i]['index']},
    #                                                                        {"$set", {"AlphaId": alpha_id, "SubmissionId": submission_id}})
    #                             submission_info['id'] = submission_id
    #                             submission_info['index'] = running_alphas[i]['index']
    #
    #                             submission_ids.append(submission_info)
    #
    #                         # case very good reverse alpha
    #                         elif((total_result['Fitness'] <- 1.1) & (total_result['Sharpe'] < -1.25)):
    #                             print('11111')
    #                             print(running_alphas[i])
    #                             reverse_alpha = json.loads(running_alphas[i]['code'])
    #                             print(reverse_alpha)
    #                             reverse_alpha[0]['code'] = '-{}'.format(reverse_alpha[0]['code'])
    #                             print(reverse_alphas)
    #                             reverse_alphas.append(json.dumps(reverse_alpha))
    #
    #                         else:
    #                             print('Not proceeded case whith Fitness {} and Sharpe {}'.format(total_result['Fitness'], total_result['Sharpe']))
    #
    #                         i+= 1
    #                         error_attempts = 0
    #                         sleep_attempts = 0
    #                         time.sleep(self.pause)
    #                     #  Case when alpha is not proceeded for a long period of time
    #                     elif ((stats['error'] == '') & (stats['status'] == True)):
    #                         sleep_attempts += 1
    #                         print('Alphas not finished simulation')
    #                         time.sleep(5 * sleep_attempts)
    #                         if (sleep_attempts > 4):
    #                             print('Delete alpha {}'.format(running_alphas[i]['code']))
    #                             self.mongo[self.collection_purgatory].remove({'Index': running_alphas[i]['index']})
    #                             sleep_attempts = 0
    #                             i+= 1
    #
    #                     else:
    #                         print('Wrong error format {}'.format(stats['error']))
    #                         error_attempts += 1
    #                         time.sleep(self.pause)
    #
    #                     parse_attempts = 0
    #
    #                 except Exception as e:
    #                     print('Parse exception {e}', e)
    #                     parse_attempts += 1
    #                     if (parse_attempts > 4):
    #                         parse_attempts = 0
    #                         i+= 1
    #                     time.sleep(self.pause)
    #
    #             except Exception as e:
    #                 print('Local exception occured {}'.format(e))
    #                 error_attempts += 1
    #                 time.sleep(self.pause)
    #         else:
    #             print('Too many errors, function Parse_Alphas finished')
    #             break
    #     return reverse_alphas, submission_ids
    
    # def parse_submissions(self, cookie, submission_ids):
    #     i = 0
    #
    #     error_attempts = 0
    #     sleep_attempts = 0
    #
    #     while (i < len(submission_ids)):
    #         if (error_attempts < 1):
    #             print('Parsing submission of alpha with index {}'.format(submission_ids[i]['index']))
    #             try:
    #                 submission = self.requestor.get_submission_result(cookie, submission_ids[i]['id'])
    #                 res = json.loads(submission.content)
    #                 print(res)
    #                 if (res['result'] == None):
    #                     sleep_attempts = 0
    #                     error_attempts = 0
    #                     self.mongo[self.collection_prod].update({'Index': submission_ids[i]['index']}, {"$set": {"Submission": res['status'], 'SubmissionInfo': res['error']}})
    #                     i += 1
    #                 else:
    #                     if (res['result']['InProgress'] == True):
    #                         sleep_attempts += 1
    #                         print('Sleep for time {}'.format(5 * sleep_attempts))
    #                         print(res)
    #                         time.sleep(5 * sleep_attempts)
    #                         if (sleep_attempts > 10):
    #                             i+= 1
    #                     else:
    #                         print(res)
    #                         break
    #
    #
    #             except Exception as e:
    #                 print('Local exception occured {}'.format(e))
    #                 error_attempts += 1
    #         else:
    #             print('Too many errors, function Parse_Submission finished')
    #             break
                
    # def run_alphas(self, alphas, cookie, alphas_iteration=None):
    #     try:
    #         running_alphas = self.simulate_alphas(cookie, alphas, alphas_iteration)
    #         reverse_alphas, submission_ids = wq.parse_alphas(cookie, running_alphas)
    #         running_reverse_alphas = wq.simulate_alphas(cookie, reverse_alphas, alphas_iteration)
    #         reverse_reverse_alphas, reverse_submission_ids = wq.parse_alphas(cookie, running_reverse_alphas)
    #         wq.parse_submissions(cookie, submission_ids)
    #         wq.parse_submissions(cookie, reverse_submission_ids)
    #         return 'Success'
    #     except Exception as e:
    #         print('Exception occured {}'.format(e))
    #         return 'Failed'
    #
    #
    # def parse_ancient_alphas(self, cookie):
    #     try:
    #         df = pd.DataFrame(list(mongo[self.mongo_alphas_collection_name].find({'Fitness': None})))
    #         print(df.shape)
    #         df = df.iloc[:500, :]
    #         df = df.rename(index=str, columns={"Code": "code",
    #                                            "Neutralization" : "opneut",
    #                                            "Universe":"univid",
    #                                            "Region":"region",
    #                                            "Delay":"delay"})
    #         df['raw_alpha'] = df.apply(lambda x:
    #             wq.build_alpha(
    #                 code = x['code'],
    #                 univid= x['univid'],
    #                 delay = x['delay'],
    #                 region = x['region'],
    #                 opneut = x['opneut']), axis = 1)
    #
    #         alphas = list(df['raw_alpha'].values)
    #         print(len(alphas))
    #         print(alphas[0])
    #
    #         result = self.run_alphas(alphas, cookie)
    #         if (result == 'Success'):
    #
    #             ids = list(df['_id'].values)
    #             ids = [ObjectId(id_id) for id_id in ids]
    #             print('Deleting old alphas')
    #             deleting_result = self.mongo[self.collection_simulate].remove({'_id': {'$in' : ids}})
    #             print(deleting_result)
    #             return 'Success'
    #         else:
    #             return 'Failed'
    #     except Exception as e:
    #         print('Exception occured while executing function Parse Ancient Alphas {}'.format(e))
    #         return 'Failed'