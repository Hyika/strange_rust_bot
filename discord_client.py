# Python 내장 라이브러리
import asyncio

# 사용자 라이브러리 
from discord_gateway import Gateway
from discord_http import Http


class Client: 
    def __init__(self, token: str): 
        self.application_id: str | None = None
        self.token: str = token

        self.gateway: Gateway = None
        self.http: Http = None

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

    async def dispatch(self, data: dict, event_name: str | None): 
        match event_name: 
            case 'READY': 
                self.gateway.resume_url = data['resume_gateway_url']
                self.session_id = data['session_id']
                self.http.application_id = self.application_id = data['user']['id']

                await self.http.make_a_global_cmd()

                if func := self._listeners.get('ON_READY'):
                    await func()

            case "MESSAGE_CREATE": 
                if func := self._listeners.get('ON_MESSAGE'):
                    await func()
            case "INTERACTION_CREATE": 
                print("Interaction create. ")
                print(data)
            case _: 
                print(f'{event_name}')


    def run(self):        
        try:
            asyncio.run(self._start())
        except KeyboardInterrupt:
            print("Disconnect by using keyboard. ")
