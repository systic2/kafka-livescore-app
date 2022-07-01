import datetime
import json
import traceback
from dataclasses import dataclass, asdict
from time import sleep
from typing import List, Dict, Any, Optional

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from starlette.websockets import WebSocket

KAFKA_URL = "kafka:9092"  # kafka 주소


def connect_consumer():  # 컨슈머 연결
    while True:
        try:
            consumer = KafkaConsumer(
                f"raw-events",
                max_poll_records=1,
                bootstrap_servers=KAFKA_URL,
            )  # kafka 컨슈머 생성
            break
        except NoBrokersAvailable:  # 예외 처리
            sleep(1)
    return consumer  # 컨슈머 반환


def parse(message):  # 메시지 구문 분석
    return json.loads(message.value.decode("utf-8"))  # json.loads 함수 호출


class Types:  # 이벤트 타입
    SHOT = 16  # 슛
    FOUL = 22  # 파울
    BAD_BEHAVIOUR = 24  # 비신사적 행동
    STARTING_XI = 35  # 스타팅 11
    END_HALF = 34  # 경기 종료


class Outcomes:  # 결과
    GOAL = 97  # 골
    FOUL_YELLOW = "Yellow Card"  # The docs are wrong here, so judging based on event name
    FOUL_SECOND_YELLOW = "Second Yellow Card"  # 파울로 인한 두 번째 옐로우 카드
    FOUL_RED = "Red Card"  # 파울로 인한 레드 카드
    BAD_BEHAVIOR_YELLOW = "Yellow Card"  # 비신사적 행동으로 인한 옐로우 카드
    BAD_BEHAVIOR_SECOND_YELLOW = "Second Yellow Card"  # 비신사적 행동으로 인한 두 번째 옐로우 카드
    BAD_BEHAVIOR_RED = "Red Card"  # 비신사적 행동으로 인한 레드 카드


@dataclass  # 데이터 정의
class Message(object):  # Message 데이터클래스
    game_id: str
    event: Dict[str, Any]


@dataclass  # 데이터 정의
class Player(object):  # Player 데이터클래스
    id: int
    name: str

    def encode(self):  # 메서드
        return {
            "id": self.id,
            "name": self.id
        }


@dataclass  # 데이터 정의
class YellowCard(object):  # YellowCard 데이터클래스
    player: Player
    minute: int
    team_id: int

    @classmethod  # 클래스 메서드
    def from_event(cls, event):
        return YellowCard(
            player=Player(**event["player"]),
            minute=event["minute"],
            team_id=event["team"]["id"],
        )


@dataclass  # 데이터 정의
class SecondYellow(object):  # SecondYellow 데이터클래스
    player: Player
    minute: int
    team_id: int

    @classmethod  # 클래스 메서드
    def from_event(cls, event):
        return SecondYellow(
            player=Player(**event["player"]),
            minute=event["minute"],
            team_id=event["team"]["id"],
        )


@dataclass  # 데이터 정의
class RedCard(object):  # RedCard 데이터클래스
    player: Player
    minute: int
    team_id: int

    @classmethod  # 클래스 메서드
    def from_event(cls, event):
        return RedCard(
            player=Player(**event["player"]),
            minute=event["minute"],
            team_id=event["team"]["id"],
        )


@dataclass  # 데이터 정의
class Goal(object):  # Goal 데이터클래스
    team_id: int
    player: Player
    minute: int

    @classmethod  # 클래스 메서드
    def from_event(cls, event):
        return Goal(
            team_id=event["team"]["id"],
            player=Player(**event["player"]),
            minute=event["minute"],
        )


@dataclass  # 데이터 정의
class Team(object):  # Team 데이터클래스
    id: int
    name: str


@dataclass  # 데이터 정의
class Game:  # Game 데이터클래스
    game_id: str
    yellow_cards: List[YellowCard]
    red_cards: List[RedCard]
    second_yellows: List[SecondYellow]
    goals: List[Goal]
    home_team: Optional[Team]
    away_team: Optional[Team]
    events: List[str]

    def __init__(self, game_id):  # 생성자
        self.game_id = game_id
        self.yellow_cards = []
        self.red_cards = []
        self.second_yellows = []
        self.goals = []
        self.home_team = None
        self.away_team = None
        self.events = []

    def dict(self):  # 딕셔너리 형태로 반환
        return {
            "yellow_cards": self.yellow_cards,
            "red_cards": self.red_cards,
            "second_yellow": self.yellow_cards,
            "goals": self.goals,
            "home_team": self.home_team,
            "away_team": self.away_team
        }

    def apply(self, event) -> bool:  # event를 받아서 bool 형태로 반환
        """
        Apply the event and return whether something happened
        :param event:
        :return:
        """
        if event["id"] in self.events:  # event['id'] 가 events 안에 있으면 False 반환
            return False
        try:
            self.events.append(event["id"])  # game 클래스 events 속성에 event['id'] 값을 추가
            if self.__is_starting_XI(event):  # 스타팅 11이면 실행
                print("Starting XI event")
                team = Team(**event["team"])  # Team 클래스에 event['team'] 딕셔너리를 받아서 team 변수에 저장
                if event["index"] == 1:  # event['index'] 가 1이면 team을 home_team에 저장
                    self.home_team = team
                elif event["index"] == 2:  # event['index'] 가 1이면 team을 away_team에 저장
                    self.away_team = team
            elif self.__is_goal(event):  # 골이면 실행
                print("Goal event")
                goal = Goal.from_event(event)  # event변수를 받아서 Goal 클래스 from_event 함수 호출
                self.goals.append(goal)  # Game 클래스 goals에 goal 추가
            elif self.__is_yellow(event):  # 옐로우면 실행
                print("Yellow card event")
                yellow = YellowCard.from_event(event)  # event변수를 받아서 YellowCard 클래스 from_event 함수 호출
                self.yellow_cards.append(yellow)  # Game 클래스 yellow_cards에 추가
            elif self.__is_red(event):  # 레드면 실행
                print("Red card event")
                red = RedCard.from_event(event)  # event 변수를 받아서 RedCard 클래스 from_event 함수 호출
                self.red_cards.append(red)  # Game 클래스 red_cards에 추가
            elif self.__is_second_yellow(event):  # 두 번째 옐로우면 실행
                print("Second yellow card event")
                sy = SecondYellow.from_event(event)  # SecondYellow 클래스 from_event 함수 호출
                self.second_yellows.append(sy)  # Game 클래스 second_yellows 에 추가
            else:   # 제외인 경우
                t = event["type"]["id"]  # t 변수에 event['type']['id']를 저장 후 False 반환
                return False
            return True  # True를 반환
        except KeyError as e:  # 예외 처리
            print("could not process event")
            print(event)
            print(traceback.format_exc())

    @staticmethod  #
    def __is_goal(event):
        return event["type"]["id"] == Types.SHOT \
               and event["shot"]["outcome"]["id"] == Outcomes.GOAL

    @staticmethod
    def __is_yellow(event):
        if "foul_committed" in event:
            return (
                    event["type"]["id"] == Types.FOUL and
                    event["foul_committed"].get("card", {}).get("name", "DUMMY") == Outcomes.FOUL_YELLOW
            )
        if "bad_behaviour" in event:
            return (
                    event["type"]["id"] == Types.BAD_BEHAVIOUR and
                    event["bad_behaviour"].get("card", {}).get("name", "DUMMY") == Outcomes.BAD_BEHAVIOR_YELLOW
            )
        return False

    @staticmethod
    def __is_second_yellow(event):
        if "foul_committed" in event:
            return (
                    event["type"]["id"] == Types.FOUL and
                    event["foul_committed"].get("card", {}).get("name", "DUMMY") == Outcomes.FOUL_SECOND_YELLOW
            )
        if "bad_behaviour" in event:
            return (
                    event["type"]["id"] == Types.BAD_BEHAVIOUR and
                    event["bad_behaviour"].get("card", {}).get("name", "DUMMY") == Outcomes.BAD_BEHAVIOR_SECOND_YELLOW
            )
        return False

    @staticmethod
    def __is_red(event):
        if "foul_committed" in event:
            return (
                    event["type"]["id"] == Types.FOUL and
                    event["foul_committed"].get("card", {}).get("name", "DUMMY") == Outcomes.FOUL_RED
            )
        if "bad_behaviour" in event:
            return (
                    event["type"]["id"] == Types.BAD_BEHAVIOUR and
                    event["bad_behaviour"].get("card", {}).get("name", "DUMMY") == Outcomes.BAD_BEHAVIOR_RED
            )
        return False

    @staticmethod
    def __is_starting_XI(event):
        return event["type"]["id"] == Types.STARTING_XI


class DataManager:  # DataManager 클래스
    games: Dict[str, Game] = {}  # games = { 'key': Game object }

    def process_message(self, message: Message) -> Game:  # message를 받아서 Game 클래스 형태로 반환
        if message.game_id not in self.games:  # games 안에 game_id가 없으면
            self.games[message.game_id] = Game(message.game_id)  # message.game_id와 일치하는 Game클래스를
            # games[message.game_id]로 저장

        game = self.games[message.game_id]  # game 안에 games 딕셔너리의 Game 클래스를 저장
        updated = game.apply(message.event)  # updated 변수에 game.apply(message.event)함수 호출 반환

        if self.is_end_match(message.event):  # is_end_match()가 참이면 games의 Game 클래스를 삭제
            del self.games[message.game_id]

        if updated:  # updated 가 참이면 game을 반환
            return game

    @staticmethod
    def is_end_match(event):  # event를 받아서 bool 값으로 반환
        return (
                event["type"]["id"] == Types.END_HALF and
                event["period"] == 2  # event['type']['id]가 Types.END_HALF(34) 이고 event['period']가 2이면
        )


class ConnectionManager:
    def __init__(self):  # 생성자
        self.active_connections: List[WebSocket] = []  # active_connections 에 WebSocket 리스트 저장

    async def connect(self, websocket: WebSocket):  # 비동기식 프로그래밍
        await websocket.accept()  # 웹소켓 accept 대기
        self.active_connections.append(websocket)  # active_connections 에 websocket 추가

    def disconnect(self, websocket: WebSocket):  # 연결 해제
        self.active_connections.remove(websocket)  # active_connections 에 websocket 제거

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):  # 비동기 프로그래밍
        for connection in self.active_connections:  # active_connections 에서 connection을 꺼내옴
            await connection.send_text(message)  # 메시지 전송 대기


class GameEncoder(json.JSONEncoder):  # JSONEncoder 확장
    def default(self, o):
        if type(o) in [Game, Team, Player, YellowCard, RedCard]:  # 해당 List 안에 입력한 object 타입이 있다면
            return asdict(o)  # 딕셔너리 형태로 반환
        return super().default(o)  # JSON default 함수로 반환
