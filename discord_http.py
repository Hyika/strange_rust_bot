# 외부 라이브러리
import httpx


class Http: 
    DISCORD_BASE_URL = 'https://discord.com/api/v10' 
    OPENAI_BASE_URL = 'http://127.0.0.1:8080'

    def __init__(self, token: str): 
        self.token = token
        self.session = None
        self.headers = {
            'Authorization': f'Bot {self.token}', 
            'User-Agent': 'DiscordBot (https://github.com, v1.0)', 
            'Content-Type': 'application/json'
        }

    async def start(self): 
        if self.session:
            print("Session is already started")
        else: 
            self.session = httpx.AsyncClient(headers=self.headers)

    async def close(self): 
        if self.session: 
            await self.session.aclose()
            self.session = None
        else:
            print("Session is not found")

    async def get_gateway_url(self)-> dict:
        if not self.session: 
            raise Exception("Session Not Found")
        
        url = f'{self.DISCORD_BASE_URL}/gateway/bot'
        response = await self.session.get(url, headers=self.headers)

        response.raise_for_status()

        data = response.json()
        gateway_url = f"{data['url']}/?v=10&encoding=json"
        return gateway_url
    
