import json
from datetime import datetime, timedelta
import time


class AlphaParser():
    def build_alpha(self, code, delay="1", univid="TOP3000", region="USA", opneut="subindustry", decay="5",
                    optrunc="0.1", opassetclass="EQUITY", opcodetype="EXPRESSION", unitcheck="off", tags="equity",
                    DataViz="0", backdays=512, simtime="Y10", nanhandling="on", pasteurize="on"):
        alpha_list = []
        alpha_dict = {}
        alpha_dict["nanhandling"] = "{}".format(nanhandling)
        alpha_dict["delay"] = "{}".format(delay)
        alpha_dict["unitcheck"] = "{}".format(unitcheck)
        alpha_dict["pasteurize"] = "{}".format(pasteurize)
        alpha_dict["univid"] = "{}".format(univid)
        alpha_dict["opcodetype"] = "{}".format(opcodetype)
        alpha_dict["opassetclass"] = "{}".format(opassetclass)
        alpha_dict["optrunc"] = "{}".format(optrunc)
        alpha_dict["code"] = "{}".format(code)
        alpha_dict["region"] = "{}".format(region)
        alpha_dict["opneut"] = "{}".format(opneut)
        alpha_dict["IntradayType"] = None
        alpha_dict["tags"] = "{}".format(tags)
        alpha_dict["decay"] = "{}".format(decay)
        alpha_dict["dataviz"] = "{}".format(DataViz)
        alpha_dict["backdays"] = "{}".format(backdays)
        alpha_dict["simtime"] = "{}".format(simtime)
        alpha_list.append(alpha_dict)
        return json.dumps(alpha_list)

    def report_alpha(self, alpha, alpha_code, alpha_executor, alpha_type, logic_name, region, universe, neutr, delay=1,
                     type='Base0'):
        alpha_dict = {}
        alpha_dict['Alpha'] = alpha
        alpha_dict['Code'] = alpha_code
        alpha_dict['Neutralization'] = neutr
        alpha_dict['Universe'] = universe
        alpha_dict['Delay'] = delay
        alpha_dict['Region'] = region
        alpha_dict['Time'] = str(datetime.utcfromtimestamp(time.time()))
        alpha_dict['Executor'] = alpha_executor
        alpha_dict['LogicName'] = logic_name
        alpha_dict['Comment'] = ''
        alpha_dict['Status'] = 'InProgress'
        alpha_dict['Type'] = type
        alpha_dict['DataType'] = alpha_type
        return alpha_dict

    def simulating_alpha(self, alpha, build_response, alphas_iteration):
        alpha_info = {}
        alpha_info['code'] = alpha
        alpha_info['index'] = build_response['result'][0]

        mongo_alpha_info = {}
        mongo_alpha_info['Alpha'] = alpha
        mongo_alpha_info['Code'] = json.loads(alpha)[0]['code']
        mongo_alpha_info['Neutralization'] = json.loads(alpha)[0]['opneut']
        mongo_alpha_info['Universe'] = json.loads(alpha)[0]['univid']
        mongo_alpha_info['Region'] = json.loads(alpha)[0]['region']
        mongo_alpha_info['Delay'] = json.loads(alpha)[0]['delay']
        mongo_alpha_info['Time'] = str(datetime.utcfromtimestamp(time.time()))
        mongo_alpha_info['Comment'] = ''
        mongo_alpha_info['Index'] = build_response['result'][0]
        mongo_alpha_info['Iteration'] = alphas_iteration
        return alpha_info, mongo_alpha_info