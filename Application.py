import json
from datetime import datetime

import uvicorn as uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import StreamingResponse

from EdithCloudNacos import NacosClient
from RedisConfig import RedisConnectionConfigureFromNacosConfig, RedisTemplate
from SaTokenAuthorize import SaTokenConfigureFromNacosConfig, SameTokenChecker

app = FastAPI()
nacos_client = NacosClient()

# 创建RedisTemplate和SaTokenAuthorize
# saTokenConfigureFromNacosConfig = SaTokenConfigureFromNacosConfig(nacos_client)
# redisConnectionConfigureFromNacosConfig = RedisConnectionConfigureFromNacosConfig(nacos_client)
# redisTemplate = RedisTemplate(redisConnectionConfigureFromNacosConfig)
# sameTokenChecker = SameTokenChecker(saTokenConfigureFromNacosConfig, redisTemplate)


# 添加SaToken鉴权中间件
# app.add_middleware(
#     SaTokenFilter, checker=sameTokenChecker
# )

# 定义Dto数据类，用来接受post传参
class DtoExample(BaseModel):
    id: str


# 创建路由
@app.post('/stream')
async def streamPost(request: Request, dto: DtoExample):
    print(f'{dto.id}')
    return StreamingResponse(event_generator(request), media_type='text/event-stream')


# 以下是EventStream事件流推送的事例
async def event_generator(r: Request):
    for i in 'EventStreamExample':
        if await r.is_disconnected():
            break
        data = str(CResponse(message=i))
        yield data


# 统一响应类
class CResponse:
    def __init__(self, code=200, **kwargs):
        self.r = {'timestamp': datetime.now().timestamp(),
                  'code': 200,
                  'data': kwargs}

    def __str__(self):
        return json.dumps(self.r, separators=(',', ':'))


# 创建异步服务器
if __name__ == '__main__':
    uvicorn.run("Application:app", host="0.0.0.0", port=8080)
