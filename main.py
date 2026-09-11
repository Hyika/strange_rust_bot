# Python 내장 라이브러리
import os

# 외부 라이브러리
from dotenv import load_dotenv

# 사용자 라이브러리 
from discord_client import Client


if __name__ == '__main__': 
    load_dotenv()

    if TOKEN := os.getenv("TOKEN"):
        client = Client(TOKEN)

        @client.event
        async def on_ready(): 
            print("I'm ready!")

        @client.event
        async def on_message(): 
            print("message sent!")

        client.run()
    else:
        print(".env 파일에서 TOKEN을 찾을 수 없습니다.")

    