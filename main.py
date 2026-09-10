# Python 내장 라이브러리
import asyncio
from collections import defaultdict
from dataclasses import asdict, dataclass
from enum import IntEnum
import json
import os
from typing import Self
import urllib.request

# 외부 라이브러리
from dotenv import load_dotenv
import websockets

# 사용자 라이브러리 
from discord_http import Http


@dataclass
class IdentifyProperties: 
    os: str
    browser: str 
    device: str

@dataclass
class IdentifyData: 
    token: str
    intents: int
    properties: IdentifyProperties

@dataclass 
class ResumeData: 
    token: str 
    session_id: str 
    seq: int

class GatewayOpcode(IntEnum):
    DISPATCH = 0
    HEARTBEAT = 1
    IDENTIFY = 2
    PRESENCE_UPDATE = 3
    VOICE_STATE_UPDATE = 4
    RESUME = 6
    RECONNECT = 7
    REQUEST_GUILD_MEMBERS = 8
    INVALID_SESSION = 9
    HELLO = 10
    HEARTBEAT_ACK = 11
    REQUEST_SOUNDBOARD_SOUNDS = 31 

@dataclass
class GatewayPayload: 
    op: GatewayOpcode
    d: any
    s: int | None = None
    t: str | None = None
    
    def to_dict(self): 
        return asdict(self)

    @classmethod
    def from_dict(cls, data:dict): 
        return cls(
            op=GatewayOpcode(data['op']), 
            d=data.get('d'), 
            s=data.get('s'), 
            t=data.get('t')
        )

    @classmethod
    def identify(cls, identify_data: IdentifyData) -> Self:
         return cls(
             op=GatewayOpcode.IDENTIFY, 
             d=identify_data
         ) 

    @classmethod
    def heartbeat(cls, sequence_number: int | None) -> Self: 
        return cls(
            op=GatewayOpcode.HEARTBEAT, 
            d=sequence_number
        )

    @classmethod
    def resume(cls, resume_data: ResumeData) -> Self:
        return cls(
            op=GatewayOpcode.RESUME, 
            d=resume_data
        )


class Gateway: 
    def __init__(self, token: str, url: str, client: Client): 
        self.token = token
        self.websocket = None
        self.url = url
        self.resume_url: str | None = None
        self.session_id: str | None = None
        self.sequence_number: int | None = None

        self._heartbeat_task = None
        self.client = client


    async def send(self, payload: GatewayPayload): 
        message = json.dumps(payload.to_dict())
        await self.websocket.send(message)

    async def receive(self)-> GatewayPayload: 
        message = await self.websocket.recv()
        payload = GatewayPayload.from_dict(json.loads(message))
        return payload

    async def heartbeat_lifecycle(self, ms_interval):
        try:
            while True:
                await asyncio.sleep(ms_interval/1000)

                await self.send(GatewayPayload.heartbeat(sequence_number=self.sequence_number))
                print("Heartbeat sent")
        except asyncio.CancelledError: 
            print("Heartbeak lifecycle cancelled. ")
            raise

    async def close(self, code: int = None, reason: str = None): 
        if self.websocket:
            if self._heartbeat_task:
                self._heartbeat_task.cancel() 
                self._heartbeat_task = None

            await self.websocket.close(code=code, reason=reason)
            self.websocket = None
        else:
            print("Websocket is not found")
        
    async def connect(self): 
        if self.websocket:
            raise Exception("Websocket is already created.")
        else:
            self.websocket = await websockets.connect(self.resume_url or self.url)

        while True: 
            payload = await self.receive()

            match payload.op:
                case GatewayOpcode.DISPATCH: 
                    print(f'OP 0 | Dispatch received. ')
                    self.sequence_number = payload.s
                    await self.client.dispatch(data=payload.d, event_name=payload.t)
                    
                case GatewayOpcode.RECONNECT: 
                    await self.close(code=4000, reason="Reconnecting")
                    break

                case GatewayOpcode.HELLO: 
                    print(f'OP 10 | Hello received. ')

                    self._heartbeat_task = asyncio.create_task(self.heartbeat_lifecycle(payload.d['heartbeat_interval']))

                    if self.resume_url is None:
                        await self.send(
                            GatewayPayload.identify(
                                identify_data=IdentifyData(
                                    self.token, 
                                    intents=33281, 
                                    properties=IdentifyProperties(
                                        os='linux', 
                                        browser='titti', 
                                        device='titti'
                                    )
                                )
                            )
                        )
                    else: 
                        await self.send(
                            GatewayPayload.resume(
                                resume_data=ResumeData(
                                    token=self.token, 
                                    session_id=self.session_id, 
                                    seq=self.sequence_number
                                )
                            )
                        )
                case GatewayOpcode.HEARTBEAT_ACK: 
                    print(f'OP 11 | Heartbeat ACK received. ')

class Client: 
    def __init__(self, token: str): 
        self.bot_user_id: str | None = None
        self.token: str = token
        self.gateway: Gateway = None
        self.http: Http = None

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
                if self.http: await self.http.close()
                if self.gateway: await self.gateway.close()


    async def dispatch(self, data: dict, event_name: str | None): 
        match event_name: 
            case 'READY': 
                self.gateway.resume_url = data['resume_gateway_url']
                self.session_id = data['session_id']
                self.bot_user_id = data['user']['id']
            case "MESSAGE_CREATE": 
                print("message create. ")
            case _: 
                print(f'{event_name}')

    def run(self):        
        try:
            asyncio.run(self._start())
        except KeyboardInterrupt:
            print("Disconnect using keyboard. ")


if __name__ == '__main__': 
    load_dotenv()

    if TOKEN := os.getenv("TOKEN"):
        client = Client(TOKEN)
        client.run()
    else:
        print(".env 파일에서 TOKEN을 찾을 수 없습니다.")

