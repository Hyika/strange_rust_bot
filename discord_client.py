# Python 내장 라이브러리
import asyncio

# 사용자 라이브러리 
from discord_gateway import Gateway
from discord_http import Http
from discord_openai import LLMProvider, OpenAI


class Client: 
    def __init__(self, token: str): 
        self.application_id: str | None = None
        self.token: str = token

        self.gateway: Gateway = None
        self.http: Http = None
        self.openai: OpenAI = None

        self._listeners: dict = {}

    def event(self, func): 
        event_name = func.__name__.upper()
        self._listeners[event_name] = func
        return func

    async def _start(self): 
            try: 
                self.http = Http(token=self.token)
                await self.http.start()
                gateway_url = await self.http.get_gateway_url()

                self.gateway = Gateway(token=self.token, url=gateway_url, client=self)
                await self.gateway.connect()
            except Exception as e: 
                print(f"Gateway connection error. : {e}")
            finally:
                if self.http: 
                    await self.http.close()
                if self.gateway: 
                    await self.gateway.close()
                print("Client disconnect. ")

    async def send(self, channel_id: int, message: str): 
        await self.http.send_message(channel_id=channel_id, message=message)
                             

    async def dispatch(self, data: dict, event_name: str | None): 
        match event_name: 
            case 'READY': 
                self.gateway.resume_url = data['resume_gateway_url']
                self.session_id = data['session_id']
                self.http.application_id = self.application_id = data['user']['id']
                print(data)
                await self.http.make_a_global_cmd()

                if func := self._listeners.get('ON_READY'):
                    await func()

            case "MESSAGE_CREATE": 
                guild_id = data['guild_id']
                channel_id = data['channel_id']
                if self.application_id != data['author']['id']:
                    print('message_create')
                    print(data)
    
                    if self.openai: 
                        print('OpenAI-compatible-api detected! ')
                        print(data['content'])
                        ai_response = await self.openai.send_message(prompt=data['content']) 
                        print('ai response: {ai_reponse}')
                        await self.send(
                            channel_id=channel_id, 
                            message=ai_response
                        )

                if func := self._listeners.get('ON_MESSAGE'):
                    await func()

            case "MESSAGE_DELETE": 
                guild_id = data['guild_id']
                channel_id = data['channel_id']

                await self.send(
                    channel_id=channel_id, 
                    message="메시지, 삭제했어..."
                )

            case "INTERACTION_CREATE": 
                print("Interaction create. ")
                
                self.openai = OpenAI(
                    http=self.http, 
                    provider=LLMProvider.LOCAL_MODEL, 
                    base_url=data['data']['options'][0]['options'][0]['value']
                )

                id = data['id']
                token = data['token']

                res = await self.http.interaction_callback(id=id, token=token, data={"content":"...@%@#$%!?"})
                print(res)
            case _: 
                print(f'{event_name}')


    def run(self):        
        try:
            asyncio.run(self._start())
        except KeyboardInterrupt:
            print("Disconnect by using keyboard. ")
