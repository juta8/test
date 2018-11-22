import sys
import json
from string import Formatter
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import time
import itertools
import alpha_parser

config_fn = "config.json"
data_fn = "data.json" 
cache_fn = "cache.json"

with open(data_fn, 'r') as f:
    data = json.load(f)

with open(cache_fn, 'r') as f:
    cache = json.load(f)

with open(config_fn, 'r') as f:
    config = json.load(f)

# fundamental = data['data']['USA']['company_fundamental']
# estimation = data['data']['USA']['company_estimation']
# relationship = data['data']['USA']['company_relationship']

fundamental = data['data']['Europe']['company_fundamental']
estimation = data['data']['Europe']['company_estimation']
relationship = data['data']['Europe']['company_relationship']

client = pymongo.MongoClient(config['mongodb_connection_string'])
db = client.wq
alpha_parser = alpha_parser.AlphaParser()

class UnseenFormatter(Formatter):
    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            try:
                return kwds[key]
            except KeyError:
                return key
        else:
            return Formatter.get_value(key, args, kwds)

def findsubsets(lst, size):
    return list(itertools.combinations(lst, size))

if __name__ == '__main__':
    # to do: add logic database name to config
    try:
        alphas = []
        fmt = UnseenFormatter()
        logic_name = sys.argv[1]
        logic_info = pd.DataFrame(list(db['logic'].find({'Name': logic_name})))
        db['logic'].update({'Name': logic_name}, {"$set": {'Status': 'Finished'}})

        if (logic_info.shape[0] > 0):
            alpha_params = logic_info['Params'].iloc[0]
            alpha_logic = logic_info['Logic'].iloc[0]
            alpha_executor = logic_info['Executor'].iloc[0]

            fundamental_subsets = findsubsets(fundamental, len(alpha_params))
            estimation_subsets = findsubsets(estimation, len(alpha_params))
            relationship_subsets = findsubsets(relationship, len(alpha_params))

            # print(len(fundamental_subsets), len(estimation_subsets), len(relationship_subsets))

            for region in [data['region']['Europe']]:
                for universe in [data['universe']['TOP100']]:
                    for neutr in [data['neutralization']['market']]:
                        print('Creating fundamental alphas')
                        for subset in fundamental_subsets:
                            value_dict = dict(zip(alpha_params, subset))
                            alpha_code = fmt.format(alpha_logic, **value_dict)
                            alpha = alpha_parser.build_alpha(alpha_code, univid=universe, region=region, opneut=neutr, decay="12")
                            alpha_info = alpha_parser.report_alpha(alpha = alpha, alpha_code = alpha_code, alpha_executor = alpha_executor,
                                                      alpha_type='Fundamental', logic_name =logic_name, region = region,
                                                      universe=universe, neutr = neutr, params=sorted(list(subset)))

                            alphas.append(alpha_info)

                        # print('Creating estimation alphas')
                        # for subset in estimation_subsets:
                        #     value_dict = dict(zip(alpha_params, subset))
                        #     alpha_code = fmt.format(alpha_logic, **value_dict)
                        #     alpha = alpha_parser.build_alpha(alpha_code, univid=universe, region=region, opneut=neutr, decay="12")
                        #     alpha_info = alpha_parser.report_alpha(alpha=alpha, alpha_code=alpha_code, alpha_executor=alpha_executor,
                        #                               alpha_type='Estimation', logic_name=logic_name, region=region,
                        #                               universe=universe, neutr=neutr)
                        #
                        #     alphas.append(alpha_info)
                        #
                        # print('Creating relationship alphas')
                        # for subset in relationship_subsets:
                        #     value_dict = dict(zip(alpha_params, subset))
                        #     alpha_code = fmt.format(alpha_logic, **value_dict)
                        #     alpha = alpha_parser.build_alpha(alpha_code, univid=universe, region=region, opneut=neutr, decay="12")
                        #     alpha_info = alpha_parser.report_alpha(alpha=alpha, alpha_code=alpha_code, alpha_executor=alpha_executor,
                        #                               alpha_type='Relationship', logic_name=logic_name, region=region,
                        #                               universe=universe, neutr=neutr)
                        #
                        #     alphas.append(alpha_info)

            print('Created {} alphas'.format(len(alphas)))
            db['alphas_simulate'].insert(alphas)
        else:
            print('No logic with such name {}'.format(logic_name))
    except Exception as exc:
        print('Exception occured while adding alphas to database {}'.format(exc))






