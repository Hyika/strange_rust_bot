# Python 내장 라이브러리
from aiohttp import ClientSession, WSMsgType
import asyncio
from dataclasses import asdict, dataclass
from enum import IntEnum
import json
import os
from typing import Self
import urllib.request

# 외부 라이브러리
from dotenv import load_dotenv
import websockets



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
    def __init__(self, token: str): 
        self.token: str = token
        self.url: str = f'{Gateway.get_gateway_url(token).get('url')}/?v=10&encoding=json'
        self.resume_url: str | None = None
        self.session_id: str | None = None
        self.sequence_number: int | None = None

    @staticmethod
    def get_gateway_url(token: str)-> dict:
        url = 'https://discord.com/api/v10/gateway/bot'
        headers = {
            'Authorization': f'Bot {token}',
            'User-Agent': 'DiscordBot (https://github.com, v1.0)'
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        try:
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                return data
        except urllib.error.HTTPError as e:
            print(f"HTTP 에러 발생: {e.code} - {e.reason}")
            raise
        except Exception as e:
            print(f"Gateway URL 수신 실패: {e}")
            raise

    @staticmethod
    async def send(ws, payload: GatewayPayload): 
        message = json.dumps(payload.to_dict())
        await ws.send(message)

    @staticmethod
    async def receive(ws)-> GatewayPayload: 
        message = await ws.recv()
        payload = GatewayPayload.from_dict(json.loads(message))
        return payload

    async def heartbeat(self, ws, ms_interval):
        while True:
            await asyncio.sleep(ms_interval/1000)

            await Gateway.send(
                ws=ws, 
                payload=GatewayPayload.heartbeat(sequence_number=self.sequence_number)
            )
            print("Heartbeat sent")
        
    async def connect(self): 
        while True:
            async with websockets.connect(self.resume_url or self.url) as ws: 
                async with asyncio.TaskGroup() as tg: 
                    while True: 
                        payload = await Gateway.receive(ws)

                        match payload.op:
                            case GatewayOpcode.DISPATCH: 
                                print(f'OP 0 | Dispatch received. ')
                                print(f'{payload.t}')
                                self.sequence_number = payload.s

                                match payload.t: 
                                    case 'READY': 
                                        self.resume_url = payload.d['resume_gateway_url']
                                        self.session_id = payload.d['session_id']
                                    case 'MESSAGE_CREATE':
                                        print(payload.d)
                                    case _: 
                                        pass
                                
                            case GatewayOpcode.HELLO: 
                                print(f'OP 10 | Hello received. ')

                                tg.create_task(self.heartbeat(ws, payload.d['heartbeat_interval']))

                                if self.resume_url is None:
                                    await Gateway.send(
                                        ws=ws, 
                                        payload=GatewayPayload.identify(
                                            identify_data=IdentifyData(
                                                self.token, 
                                                intents=513, 
                                                properties=IdentifyProperties(
                                                    os='linux', 
                                                    browser='titti', 
                                                    device='titti'
                                                )
                                            )
                                        )
                                    )
                                else: 
                                    await Gateway.send(
                                        ws=ws, 
                                        payload=GatewayPayload.resume(
                                            resume_data=ResumeData(
                                                token=self.token, 
                                                session_id=self.session_id, 
                                                seq=self.sequence_number
                                            )
                                        )
                                    )
                            case GatewayOpcode.HEARTBEAT_ACK: 
                                print(f'OP 11 | Heartbeat ACK received. ')



class Discord: 
    def __init__(self, token: str): 
        self.token = token

    def run(self): 
        gateway = Gateway(token=self.token, url="wss://gateway.discord.gg/?v=10&encoding=json")
        asyncio.run(gateway.connect())



if __name__ == '__main__': 
    load_dotenv()

    if TOKEN := os.getenv("TOKEN"):
        client = Discord(TOKEN)
        client.run()
    else:
        print(".env 파일에서 TOKEN을 찾을 수 없습니다.")
