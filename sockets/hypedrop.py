import json
import asyncio
from .base_socket import BaseSocket

class HypeDropSocket(BaseSocket):
    def __init__(self, queue):
        url = "wss://router.hypedrop.com/ws"
        super().__init__(url, queue, source_name="HypeDrop")
        self.connection_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://www.hypedrop.com",
        }

    async def on_open(self, websocket):
        init_msg = {"type": "connection_init", "payload": {}}
        await websocket.send(json.dumps(init_msg))

        subscriptions = [
             # 1. Wallet Updates
            {"id":"5d4c0732-a3bb-4e6c-84c0-006f1e073434","type":"subscribe","payload":{"variables":{},"extensions":{},"operationName":"OnUpdateWallet","query":"subscription OnUpdateWallet {\n  updateWallet {\n    wallet {\n      id\n      amount\n      name\n      __typename\n    }\n    walletChange {\n      id\n      type\n      externalId\n      valueChange\n      __typename\n    }\n    __typename\n  }\n}\n"}},
             # 2. Settings Updates
            {"id":"ab19ca8f-6f31-442f-9836-4c7d70a75eaf","type":"subscribe","payload":{"variables":{},"extensions":{},"operationName":"OnUpdateSetting","query":"subscription OnUpdateSetting {\n  updateSetting {\n    setting {\n      id\n      key\n      value\n      __typename\n    }\n    __typename\n  }\n}\n"}},
             # 3. Jackpot Updates
            {"id":"08555029-dadb-4c3c-aa4c-648e3b985972","type":"subscribe","payload":{"variables":{},"extensions":{},"operationName":"OnJackpotUpdate","query":"subscription OnJackpotUpdate($id: ID) {\n  updateJackpot(id: $id) {\n    jackpot {\n      ...ActiveJackpot\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment ActiveJackpot on Jackpot {\n  id\n  winnerCount\n  totalValue\n  currency\n  deletedAt\n  scheduledAt\n  totalTickets\n  ticketsInfo {\n    minId\n    maxId\n    __typename\n  }\n  __typename\n}\n"}},
             # 4. Box Openings
            {"id":"eec46ebf-bfbb-4cb9-81f9-7e35a2d04d67","type":"subscribe","payload":{"variables":{"minItemValue":10},"extensions":{},"operationName":"OnCreateBoxOpening","query":"subscription OnCreateBoxOpening($ancestorBoxId: ID, $boxId: ID, $boxSlug: String, $minItemValue: Float) {\n  createBoxOpening(\n    ancestorBoxId: $ancestorBoxId\n    boxId: $boxId\n    boxSlug: $boxSlug\n    minItemValue: $minItemValue\n  ) {\n    boxOpening {\n      ...StreamBoxOpening\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment StreamBoxOpening on BoxOpening {\n  id\n  boxItemId\n  createdAt\n  itemValue\n  box {\n    id\n    name\n    slug\n    iconUrl\n    price\n    currency\n    market {\n      id\n      slug\n      __typename\n    }\n    __typename\n  }\n  itemVariant {\n    id\n    name\n    brand\n    color\n    rarity\n    size\n    displayValue\n    currency\n    iconUrl\n    type\n    __typename\n  }\n  pvpGameId\n  user {\n    ...UserBadgeSimple\n    __typename\n  }\n  userItemId\n  roll {\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment UserBadgeSimple on User {\n  id\n  displayName\n  avatar\n  rank\n  authentic\n  teamId\n  level\n  __typename\n}\n"}},
             # 5. Create PvP Game (THE BIG ONE)
            {"id":"ffd5b621-f4b2-456f-b8c3-e4d669e191d6","type":"subscribe","payload":{"variables":{},"extensions":{},"operationName":"OnCreatePvpGame","query":"subscription OnCreatePvpGame {\n  createPvpGame {\n    pvpGame {\n      ...PvpGameThumbnail\n      __typename\n    }\n    autoJoinedByBots\n    __typename\n  }\n}\n\nfragment PvpGameThumbnail on PvpGame {\n  id\n  type\n  status\n  minPlayers\n  maxPlayers\n  currency\n  updatedAt\n  totalBet\n  fastMode\n  brandSpin\n  initialBet\n  sponsoredInitialBet\n  sponsorPercentage\n  meetsSponsorRules\n  activeRound {\n    number\n    status\n    round {\n      roundId\n      __typename\n    }\n    __typename\n  }\n  userId\n  initialWinItemVariant {\n    id\n    name\n    brand\n    iconUrl\n    rarity\n    __typename\n  }\n  players {\n    ...PvpGameThumbnailPlayerFragment\n    __typename\n  }\n  rounds {\n    edges {\n      node {\n        ...PvpGameThumbnailRound\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  createdAt\n  strategy\n  isPrivate\n  mode\n  maxNumberOfPlayersInTeam\n  maxNumberOfTeams\n  teams {\n    userId\n    teamSelection\n    __typename\n  }\n  totalPayout\n  __typename\n}\n\nfragment PvpGameThumbnailRound on PvpRound {\n  id\n  status\n  bet\n  roundId\n  box {\n    id\n    iconUrl\n    backgroundImageUrl\n    __typename\n  }\n  __typename\n}\n\nfragment PvpGameThumbnailPlayerFragment on PvpGamePlayer {\n  user {\n    ...UserBadgeSimple\n    microphoneEnabled\n    __typename\n  }\n  userId\n  isPvpBot\n  timesWon\n  totalBet\n  totalPayout\n  totalProfit\n  __typename\n}\n\nfragment UserBadgeSimple on User {\n  id\n  displayName\n  avatar\n  rank\n  authentic\n  teamId\n  level\n  __typename\n}\n"}},
             # 6. Update PvP Game
            {"id":"eb1b185a-9730-4bcd-baf3-068274845c0a","type":"subscribe","payload":{"variables":{},"extensions":{},"operationName":"OnUpdatePvpGameThumbnail","query":"subscription OnUpdatePvpGameThumbnail($id: ID, $userId: ID) {\n  updatePvpGame(id: $id, userId: $userId) {\n    pvpGame {\n      id\n      activeRound {\n        number\n        status\n        round {\n          roundId\n          __typename\n        }\n        __typename\n      }\n      players {\n        ...PvpGameThumbnailPlayerFragment\n        __typename\n      }\n      status\n      totalBet\n      totalPayout\n      updatedAt\n      teams {\n        userId\n        teamSelection\n        __typename\n      }\n      isPrivate\n      __typename\n    }\n    autoJoinedByBots\n    __typename\n  }\n}\n\nfragment PvpGameThumbnailPlayerFragment on PvpGamePlayer {\n  user {\n    ...UserBadgeSimple\n    microphoneEnabled\n    __typename\n  }\n  userId\n  isPvpBot\n  timesWon\n  totalBet\n  totalPayout\n  totalProfit\n  __typename\n}\n\nfragment UserBadgeSimple on User {\n  id\n  displayName\n  avatar\n  rank\n  authentic\n  teamId\n  level\n  __typename\n}\n"}}
        ]

        for sub in subscriptions:
            await websocket.send(json.dumps(sub))
            await asyncio.sleep(0.2)

    async def parse_message(self, message) -> dict:
        try:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type != "next":
                return None

            payload = data.get("payload", {}).get("data", {})
            
            if "updatePvpGame" not in payload:
                return None

            game = payload["updatePvpGame"]["pvpGame"]

            if game.get("status") != "FINISHED":
                return None

            real_players = []

            for player in game.get("players", []):
                if player.get("isPvpBot") is True:
                    continue
                
                user_info = player.get("user", {})
                
                player_data = {
                    "display_name": user_info.get("displayName"),
                    "user_id": player.get("userId"),
                    "avatar": user_info.get("avatar"),
                    "total_bet": player.get("totalBet"),
                    "total_profit": player.get("totalProfit"),
                    "payout": player.get("totalPayout")
                }
                real_players.append(player_data)

            if not real_players:
                return None

            return {
                "event": "game_finished",
                "players": real_players
            }

        except Exception as e:
            print(f"Error parsing: {e}") 
            return None