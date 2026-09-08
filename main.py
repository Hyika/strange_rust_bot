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
import httpx
import websockets


class Http: 
    DISCORD_BASE_URL = 'https://discord.com/api/v10' 
    OPENAI_BASE_URL = 'http://127.0.0.1:8080'

    def __init__(self, token: str, session: httpx.AsyncClient): 
        self.token = token
        self.session = session
        self.headers = {
            'Authorization': f'Bot {self.token}', 
            'User-Agent': 'DiscordBot (https://github.com, v1.0)', 
            'Content-Type': 'application/json'
        }

    async def get_gateway_url(self)-> dict:
        url = f'{self.DISCORD_BASE_URL}/gateway/bot'
        response = await self.session.get(url, headers=self.headers)

        response.raise_for_status()

        data = response.json()
        gateway_url = f"{data['url']}/?v=10&encoding=json"
        return gateway_url


    async def send_message(self, channel_id: str | int, content: str) -> dict:
        url = f'{self.DISCORD_BASE_URL}/channels/{channel_id}/messages'
        payload = {'content': content}

        response = await self.session.post(
            url, 
            headers=self.headers, 
            json=payload
        )
        response.raise_for_status()

        return response.json()

    async def chat_completion(
        self, 
        messages: list[dict], 
        temperature: float = 0.7, 
        max_tokens: int= 8000
    )-> str:
        url = f'{self.OPENAI_BASE_URL}/v1/chat/completions'
        payload = {
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens,
            'stream': False, 
        }

        response = await self.session.post(url, json=payload, timeout=60.0)
        response.raise_for_status()

        data = response.json()
        # OpenAI 응답 규격에서 모델의 텍스트 답변 추출
        return data['choices'][0]['message']['content']
    

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
        self.token: str = token
        self.websocket = None
        self.url = url
        self.resume_url: str | None = None
        self.session_id: str | None = None
        self.sequence_number: int | None = None

        self.client = client


    async def send(self, payload: GatewayPayload): 
        message = json.dumps(payload.to_dict())
        await self.websocket.send(message)

    async def receive(self)-> GatewayPayload: 
        message = await self.websocket.recv()
        payload = GatewayPayload.from_dict(json.loads(message))
        return payload

    async def heartbeat(self, ms_interval):
        while True:
            await asyncio.sleep(ms_interval/1000)

            await self.send(GatewayPayload.heartbeat(sequence_number=self.sequence_number))
            print("Heartbeat sent")
        
    async def connect(self): 
        while True: 
            async with websockets.connect(self.resume_url or self.url) as ws: 
                
                self.websocket = ws

                async with asyncio.TaskGroup() as tg: 
                    while True: 
                        payload = await self.receive()

                        match payload.op:
                            case GatewayOpcode.DISPATCH: 
                                print(f'OP 0 | Dispatch received. ')
                                self.sequence_number = payload.s

                                await self.client.dispatch(data=payload.d, event_name=payload.t)

                                # match payload.t: 
                                #     case 'READY': 
                                #         self.resume_url = payload.d['resume_gateway_url']
                                #         self.session_id = payload.d['session_id']
                                #     case 'MESSAGE_CREATE':
                                #         print(payload.d)
                                #     case _: 
                                #         pass
                                
                            case GatewayOpcode.RECONNECT: 
                                await ws.close(code=4000, reason="Reconnecting")
                                break

                            case GatewayOpcode.HELLO: 
                                print(f'OP 10 | Hello received. ')

                                tg.create_task(self.heartbeat(payload.d['heartbeat_interval']))

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

        # 채널별 대화 히스토리 저장소 (기본값: 빈 리스트)
        self.message_history: dict[str, list[dict]] = defaultdict(list)

    async def _start(self): 
        async with httpx.AsyncClient() as session: 
            self.http = Http(token=self.token, session=session)
            gateway_url = await self.http.get_gateway_url()

            self.gateway = Gateway(token=self.token, url=gateway_url, client=self)
            
            await self.gateway.connect()

    async def dispatch(self, data: dict, event_name: str | None): 
        match event_name: 
            case 'READY': 
                self.gateway.resume_url = data['resume_gateway_url']
                self.session_id = data['session_id']
                self.bot_user_id = data['user']['id']
            case "MESSAGE_CREATE": 
                await self.on_message(data)
            case _: 
                print(f'{event_name}')

    async def on_message(self, data: dict):
        author = data.get("author", {})
        author_id = author.get("id")
        content = data.get("content", "")
        channel_id = data.get("channel_id")

        # 봇 자신이 보낸 메시지거나 다른 봇의 메시지는 무시 (무한 루프 방지)
        if author_id == self.bot_user_id or author.get("bot", False):
            return

        print(f"[{author.get('username')}]: {content}")
        history = self.message_history[channel_id]

        messages = [
            {
                'role': 'system',
                'content': "you are a great assistant. "
            }
        ]

        
        messages.extend(history)
        messages.append({'role': 'user', 'content': content})

        try:
            reply = await self.http.chat_completion(messages)

            history.append({'role': 'user', 'content': content})
            history.append({'role': 'assistant', 'content': reply})

            await self.http.send_message(channel_id, reply)
        except Exception as e:
            print(f"LLM 통신 에러: {e}")
            await self.http.send_message(
                channel_id, "답변 생성 중 오류가 발생했습니다."
            )

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
