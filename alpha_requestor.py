import requests
import json
# to do:
# add origin and time sleeping
class Request():
    def __init__(self, login, password, origin,
                 xsrf, site_address, proxies):
        self.login = login
        self.password = password
        self.origin = origin
        self.xsrf = xsrf
        self.site_address = site_address
        self.proxies = proxies

        self.client = requests.Session()

    def log_in(self):
        login_url = '{}/login/process'.format(self.site_address)
        response = requests.post(
            login_url,
            data={
                'EmailAddress': self.login,
                'Password': self.password,
                '_xsrf': self.xsrf,
            },
            headers={
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Origin': '{}'.format(self.site_address),
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Referer': '{}/en/cms/wqc/websim/'.format(self.site_address),
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': 'django_language=en; {};'.format(self.xsrf),
            },
            proxies=self.proxies
        )
        return response

    def log_out(self, cookie, origin=None):
        if origin is None:
            origin = '{}'.format(self.site_address)
        logout_url = '{}/logout'.format(self.site_address)
        response = requests.get(
            logout_url,
            headers={
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'Referer': '{}/en/cms/wqc/websim/'.format(self.site_address),
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie),
            },
            proxies=self.proxies
        )
        return response

    def build_cookie(self, cookie):
        result = 'django_language={}; _xsrf={}; WSSID={}; UID={}; WQCookieConsent=accepted; _gat={}; _gat_wsdevTracker={}'.format('en', self.xsrf,
                                                                                                        cookie.get_dict()[
                                                                                                            'WSSID'],
                                                                                                        cookie.get_dict()[
                                                                                                            'UID'], 1,
                                                                                                        1)
        return result

    def simulate_alpha(self, cookie, alpha, referer=None, origin=None):
        if referer is None:
            referer = '{}/simulate'.format(self.site_address)
        if origin is None:
            origin='{}'.format(self.site_address)

        sumilate_url = '{}/simulate'.format(self.site_address)
        response = requests.post(
            sumilate_url,
            data={
                'args': alpha,
                '_xsrf': self.xsrf,
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response

    def progress_alpha(self, cookie, index, referer=None, origin=None):
        if referer is None:
            referer = '{}/simulate'.format(self.site_address)

        if origin is None:
            origin = '{}'.format(self.site_address)

        stats_url = '{}/job/progress/{}'.format(self.site_address, index)
        response = requests.post(
            stats_url,
            data={
                '_xsrf': self.xsrf,
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response

    def stats_alpha(self, cookie, index, referer=None, origin=None):
        if referer is None:
            referer = '{}/simulate'.format(self.site_address)

        if origin is None:
            origin = '{}'.format(self.site_address)

        stats_url = '{}/job/simsummary/{}'.format(self.site_address, index)
        response = requests.post(
            stats_url,
            data={
                '_xsrf': self.xsrf,
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response

    def get_alphaid(self, cookie, index, referer=None, origin=None):
        if referer is None:
            referer = '{}/simulate'.format(self.site_address)

        if origin is None:
            origin = '{}'.format(self.site_address)
        alphaid_url = '{}/job/details/{}'.format(self.site_address, index)
        response = requests.post(
            alphaid_url,
            data={
                '_xsrf': self.xsrf,
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response

    def get_submissionid(self, cookie, alphaid, referer=None, origin=None):
        if referer is None:
            referer = '{}/simulate'.format(self.site_address)

        if origin is None:
            origin = '{}'.format(self.site_address)

        submit_url = '{}/submission/start'.format(self.site_address)
        alpha_dict = {}
        alpha_dict['alpha_list'] = [alphaid]
        args = json.dumps(alpha_dict)
        response = requests.post(
            submit_url,
            data={
                '_xsrf': self.xsrf,
                'args': args
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response

    def get_submission_result(self, cookie, submissionid, referer=None, origin=None):
        if referer is None:
            referer ='{}/simulate'.format(self.site_address)

        if origin is None:
            origin = '{}'.format(self.site_address)

        submission_result_url = '{}/submission/result/{}'.format(self.site_address, submissionid)
        response = requests.post(
            submission_result_url,
            data={
                '_xsrf': self.xsrf
            },
            headers={
                'accept': 'application/json',
                'cache-control': 'max-age=0',
                'Origin': origin,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
                'content-type': 'application/x-www-form-urlencoded',
                'Referer': referer,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Cookie': self.build_cookie(cookie)
            },
            proxies=self.proxies
        )
        return response
