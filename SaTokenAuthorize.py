from starlette.datastructures import Headers, URL
from starlette.responses import Response, PlainTextResponse, RedirectResponse
from starlette.types import Scope, Receive, Send

import RedisConfig
from EdithCloudNacos import NacosClient


class SameTokenInvalidError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class SaTokenConfigureFromNacosConfig:
    def __init__(self, nacos_client: NacosClient, config_name='satoken'):
        self.config = nacos_client.get_config_from_data_id(config_name)
        self.token_name = self.config['sa-token']['token-name']
        self.same_token_timeout = self.config['sa-token']['same-token-timeout']

    def splicingTokenSaveKey(self):
        return f'{self.token_name}:var:same-token'

    def splicingPastTokenSaveKey(self):
        return f'{self.token_name}:var:past-same-token'


class SameTokenChecker:
    def __init__(self, config: SaTokenConfigureFromNacosConfig, redis_template: RedisConfig):
        self.config = config
        self.template = redis_template

    def check(self, token):
        if not (token and (token == self.getPastTokenNh() or token == self.getPastTokenNh())):
            raise SameTokenInvalidError(f'Invalid token {token}')

    def getPastTokenNh(self):
        return self.template[self.config.splicingTokenSaveKey()]

    def getPastPastTokenNh(self):
        return self.template[self.config.splicingPastTokenSaveKey()]


class SaTokenFilter:
    def __init__(self, checker: SameTokenChecker):
        self.checker = checker

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        headers = Headers(scope=scope)
        same_token = headers.get('SA-SAME-TOKEN')
        response: Response
        try:
            self.checker.check(same_token)
            url = URL(scope=scope)
            redirect_url = url.replace(netloc="www." + url.netloc)
            response = RedirectResponse(url=str(redirect_url))
        except SameTokenInvalidError as e:
            response = PlainTextResponse(e.msg, status_code=400)
        await response(scope, receive, send)
