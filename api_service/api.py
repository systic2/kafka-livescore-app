import asyncio
import json
import threading
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocket, WebSocketDisconnect

import api_service.kakfa_thread as kafka_thread
from api_service.helpers import DataManager, ConnectionManager, GameEncoder

app = FastAPI()  # FastAPI object 생성

app.add_middleware(
    CORSMiddleware,  # Cross-Origin Resource Sharing
    allow_origins=["*"],  # 허용되는 origin
    allow_credentials=True,  # 여러 origin에 대해서 쿠키가 허용되게 할 것인가
    allow_methods=["*"],  # 허용되는 HTTP method ['GET']
    allow_headers=["*"],  # 허용되는 HTTP request header
)  # FastAPI object에 값을 추가

data = DataManager()  # helpers.py의 DataManager 클래스 생성
sockets = ConnectionManager()  # helpers.py의 ConnectionManager 클래스 생성

# asyncio.run 으로 비동기식 프로그래밍을 진행하고 인수로 kafka_thread.py의 run 함수 반환 값들을 event_watcher 에 저장
event_watcher = threading.Thread(target=asyncio.run, args=(kafka_thread.run(sockets, data),))
event_watcher.start()  # event_watcher 스타트


@app.get("/games")  # '/games'로 get 요청이 들어오면 실행
def get_games():  # 게임 가져오기
    """
    Start processing a game
    :return:
    """
    if data.games:  # data 안에 값이 있으면
        return json.dumps(list(data.games.values()), cls=GameEncoder)  # json으로 인코딩
    return "[]"  # "[]" 반환


@app.websocket("/ws")  # '/ws'로 웹소켓 요청 수행
async def websocket_endpoint(websocket: WebSocket):  # 비동기식 프로그래밍
    await sockets.connect(websocket)  # ConnectionManager 연결
    try:
        while True:  # 무한루프
            _ = await websocket.receive_text()  # 웹소켓 receive_text 함수 대기
    except WebSocketDisconnect:  # 예외 처리
        sockets.disconnect(websocket)  # ConnectionManager 연결 해제


if __name__ == "__main__":  # 단독으로 실행될 경우
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")  # uvicorn.run으로 app 실행
