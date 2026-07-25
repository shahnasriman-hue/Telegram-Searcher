import os
import json
import sqlite3
import asyncio
import threading
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any

from telethon import TelegramClient, functions, types, errors
from telethon.tl.functions.messages import SearchGlobalRequest
from telethon.tl.functions.channels import SearchPostsRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.messages import GetCommonChatsRequest
from telethon.tl.types import InputMessagesFilterEmpty, InputPeerEmpty

CONFIG_FILE = "config.json"
SESSION_DIR = "sessions"
os.makedirs(SESSION_DIR, exist_ok=True)
DB_FILE = "telegram_database.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS chats (
        id INTEGER PRIMARY KEY,
        title TEXT,
        username TEXT,
        type TEXT,
        member_count INTEGER
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS members (
        user_id INTEGER,
        first_name TEXT,
        last_name TEXT,
        username TEXT,
        chat_id INTEGER,
        PRIMARY KEY (user_id, chat_id)
    )
    """)
    conn.commit()
    conn.close()

init_db()

# ----------------- Async Loop in Background Thread -----------------
loop = asyncio.new_event_loop()

def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

loop_thread = threading.Thread(target=start_background_loop, args=(loop,), daemon=True)
loop_thread.start()

# ----------------- Telegram Manager -----------------
class TelegramManager:
    def __init__(self):
        self.client: Optional[TelegramClient] = None
        self.api_id: Optional[str] = None
        self.api_hash: Optional[str] = None
        self.phone: Optional[str] = None
        self.phone_code_hash: Optional[str] = None
        self.crawling_status: Dict[str, Any] = {}
        self.load_config()

    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    data = json.load(f)
                    self.api_id = data.get("api_id")
                    self.api_hash = data.get("api_hash")
            except Exception as e:
                print(f"Error loading config: {e}")

    def save_config(self, api_id: str, api_hash: str):
        self.api_id = api_id
        self.api_hash = api_hash
        with open(CONFIG_FILE, "w") as f:
            json.dump({"api_id": api_id, "api_hash": api_hash}, f)

    def clear_config(self):
        self.api_id = None
        self.api_hash = None
        if os.path.exists(CONFIG_FILE):
            try:
                os.remove(CONFIG_FILE)
            except Exception:
                pass

    async def _init_client_async(self) -> bool:
        if not self.api_id or not self.api_hash:
            return False
        
        if self.client:
            if self.client.is_connected():
                return True
            try:
                await self.client.connect()
                return True
            except Exception as e:
                print(f"Error connecting client: {e}")
                self.client = None

        session_path = os.path.join(SESSION_DIR, "tg_search_session")
        self.client = TelegramClient(session_path, int(self.api_id), self.api_hash)
        await self.client.connect()
        return True

    def init_client(self) -> bool:
        future = asyncio.run_coroutine_threadsafe(self._init_client_async(), loop)
        return future.result()

    async def _get_status_async(self) -> Dict[str, Any]:
        if not self.api_id or not self.api_hash:
            return {"status": "unconfigured"}
        
        try:
            initialized = await self._init_client_async()
            if not initialized:
                return {"status": "unconfigured"}
            
            authorized = await self.client.is_user_authorized()
            if authorized:
                me = await self.client.get_me()
                me_data = {
                    "id": me.id,
                    "first_name": me.first_name,
                    "last_name": me.last_name,
                    "username": me.username,
                    "phone": me.phone
                }
                return {"status": "connected", "user": me_data}
            elif self.phone_code_hash:
                return {"status": "waiting_code", "phone": self.phone}
            else:
                return {"status": "disconnected"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_status(self) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._get_status_async(), loop)
        return future.result()

    async def _send_code_async(self, phone: str) -> Dict[str, Any]:
        await self._init_client_async()
        res = await self.client.send_code_request(phone)
        self.phone = phone
        self.phone_code_hash = res.phone_code_hash
        return {"status": "waiting_code", "phone": phone}

    def send_code(self, phone: str) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._send_code_async(phone), loop)
        return future.result()

    async def _login_code_async(self, code: str) -> Dict[str, Any]:
        if not self.client or not self.phone or not self.phone_code_hash:
            raise Exception("Login flow not started or expired")
        
        try:
            await self.client.sign_in(
                phone=self.phone,
                code=code,
                phone_code_hash=self.phone_code_hash
            )
            self.phone_code_hash = None
            return await self._get_status_async()
        except errors.SessionPasswordNeededError:
            return {"status": "waiting_password"}

    def login_code(self, code: str) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._login_code_async(code), loop)
        return future.result()

    async def _login_password_async(self, password: str) -> Dict[str, Any]:
        if not self.client:
            raise Exception("Telegram client not initialized")
        await self.client.sign_in(password=password)
        self.phone_code_hash = None
        return await self._get_status_async()

    def login_password(self, password: str) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._login_password_async(password), loop)
        return future.result()

    async def _logout_async(self):
        if self.client:
            try:
                if await self.client.is_user_authorized():
                    await self.client.log_out()
                else:
                    await self.client.disconnect()
            except Exception as e:
                print(f"Error during logout: {e}")
            self.client = None
        
        self.phone = None
        self.phone_code_hash = None
        
        # Clean up session files
        session_file = os.path.join(SESSION_DIR, "tg_search_session.session")
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except Exception:
                pass

    def logout(self):
        future = asyncio.run_coroutine_threadsafe(self._logout_async(), loop)
        future.result()

    async def _search_messages_async(
        self, 
        query: str, 
        limit: int, 
        offset_rate: int = 0, 
        offset_id: int = 0, 
        offset_peer_id: Optional[int] = None, 
        offset_peer_type: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("Telegram client is not authorized")

        offset_peer = InputPeerEmpty()
        if offset_peer_id:
            try:
                offset_peer = await self.client.get_input_entity(int(offset_peer_id))
            except Exception:
                pass

        result = await self.client(SearchPostsRequest(
            query=query,
            hashtag=None,
            offset_rate=offset_rate,
            offset_peer=offset_peer,
            offset_id=offset_id,
            limit=limit
        ))
        
        chats_dict = {}
        for c in result.chats:
            chats_dict[c.id] = c
        for u in result.users:
            chats_dict[u.id] = u

        messages_list = []
        for msg in result.messages:
            peer_name = "Unknown"
            peer_username = None
            peer_type = "unknown"
            chat_id = None
            
            if msg.peer_id:
                if isinstance(msg.peer_id, types.PeerChannel):
                    chat_id = msg.peer_id.channel_id
                elif isinstance(msg.peer_id, types.PeerChat):
                    chat_id = msg.peer_id.chat_id
                elif isinstance(msg.peer_id, types.PeerUser):
                    chat_id = msg.peer_id.user_id

                chat = chats_dict.get(chat_id)
                if chat:
                    if hasattr(chat, 'title'):
                        peer_name = chat.title
                        peer_type = "channel" if isinstance(chat, types.Channel) else "chat"
                    elif hasattr(chat, 'first_name'):
                        peer_name = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                        peer_type = "user"
                    
                    if hasattr(chat, 'username') and chat.username:
                        peer_username = chat.username
            
            message_link = None
            if peer_username:
                message_link = f"https://t.me/{peer_username}/{msg.id}"
            elif chat_id:
                message_link = f"https://t.me/c/{chat_id}/{msg.id}"

            if getattr(msg, 'message', None):
                messages_list.append({
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else None,
                    "text": msg.message,
                    "peer_name": peer_name,
                    "peer_username": peer_username,
                    "peer_type": peer_type,
                    "message_link": message_link,
                    "views": getattr(msg, 'views', None),
                    "forwards": getattr(msg, 'forwards', None)
                })

        next_rate = getattr(result, 'next_rate', 0)
        next_offset_id = 0
        next_offset_peer_id = None
        next_offset_peer_type = None
        
        if result.messages:
            last_msg = result.messages[-1]
            next_offset_id = last_msg.id
            if last_msg.peer_id:
                if isinstance(last_msg.peer_id, types.PeerChannel):
                    next_offset_peer_id = last_msg.peer_id.channel_id
                    next_offset_peer_type = "channel"
                elif isinstance(last_msg.peer_id, types.PeerChat):
                    next_offset_peer_id = last_msg.peer_id.chat_id
                    next_offset_peer_type = "chat"
                elif isinstance(last_msg.peer_id, types.PeerUser):
                    next_offset_peer_id = last_msg.peer_id.user_id
                    next_offset_peer_type = "user"

        return {
            "results": messages_list,
            "next_rate": next_rate,
            "offset_id": next_offset_id,
            "offset_peer_id": next_offset_peer_id,
            "offset_peer_type": next_offset_peer_type,
            "has_more": bool(next_rate or next_offset_id)
        }

    def search_messages(
        self, 
        query: str, 
        limit: int,
        offset_rate: int = 0, 
        offset_id: int = 0, 
        offset_peer_id: Optional[int] = None, 
        offset_peer_type: Optional[str] = None
    ) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(
            self._search_messages_async(query, limit, offset_rate, offset_id, offset_peer_id, offset_peer_type), 
            loop
        )
        return future.result()

    async def _search_chats_async(self, query: str, limit: int) -> Dict[str, Any]:
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("Telegram client is not authorized")

        result = await self.client(SearchRequest(q=query, limit=limit))
        
        chats_list = []
        for chat in result.chats:
            is_channel = isinstance(chat, types.Channel)
            is_group = is_channel and not getattr(chat, 'broadcast', False)
            is_broadcast = is_channel and getattr(chat, 'broadcast', False)
            chat_type = "group" if is_group else ("channel" if is_broadcast else "chat")
            
            chats_list.append({
                "id": chat.id,
                "title": chat.title,
                "username": getattr(chat, 'username', None),
                "type": chat_type,
                "participants_count": getattr(chat, 'participants_count', None),
                "verified": getattr(chat, 'verified', False),
                "scam": getattr(chat, 'scam', False),
                "fake": getattr(chat, 'fake', False),
                "link": f"https://t.me/{chat.username}" if getattr(chat, 'username', None) else None
            })

        return {"results": chats_list}

    def search_chats(self, query: str, limit: int) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._search_chats_async(query, limit), loop)
        return future.result()

    async def _search_hashtag_async(
        self, 
        hashtag: str, 
        limit: int,
        offset_rate: int = 0, 
        offset_id: int = 0, 
        offset_peer_id: Optional[int] = None, 
        offset_peer_type: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("Telegram client is not authorized")

        offset_peer = InputPeerEmpty()
        if offset_peer_id:
            try:
                offset_peer = await self.client.get_input_entity(int(offset_peer_id))
            except Exception:
                pass

        clean_tag = hashtag.lstrip('#')
        result = await self.client(SearchPostsRequest(
            hashtag=clean_tag,
            offset_rate=offset_rate,
            offset_peer=offset_peer,
            offset_id=offset_id,
            limit=limit
        ))
        
        chats_dict = {}
        for c in result.chats:
            chats_dict[c.id] = c
        for u in result.users:
            chats_dict[u.id] = u

        messages_list = []
        for msg in result.messages:
            peer_name = "Unknown"
            peer_username = None
            peer_type = "unknown"
            chat_id = None
            
            if msg.peer_id:
                if isinstance(msg.peer_id, types.PeerChannel):
                    chat_id = msg.peer_id.channel_id
                elif isinstance(msg.peer_id, types.PeerChat):
                    chat_id = msg.peer_id.chat_id
                elif isinstance(msg.peer_id, types.PeerUser):
                    chat_id = msg.peer_id.user_id

                chat = chats_dict.get(chat_id)
                if chat:
                    if hasattr(chat, 'title'):
                        peer_name = chat.title
                        peer_type = "channel" if isinstance(chat, types.Channel) else "chat"
                    elif hasattr(chat, 'first_name'):
                        peer_name = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                        peer_type = "user"
                    
                    if hasattr(chat, 'username') and chat.username:
                        peer_username = chat.username
            
            message_link = None
            if peer_username:
                message_link = f"https://t.me/{peer_username}/{msg.id}"
            elif chat_id:
                message_link = f"https://t.me/c/{chat_id}/{msg.id}"

            if getattr(msg, 'message', None):
                messages_list.append({
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else None,
                    "text": msg.message,
                    "peer_name": peer_name,
                    "peer_username": peer_username,
                    "peer_type": peer_type,
                    "message_link": message_link,
                    "views": getattr(msg, 'views', None),
                    "forwards": getattr(msg, 'forwards', None)
                })

        next_rate = getattr(result, 'next_rate', 0)
        next_offset_id = 0
        next_offset_peer_id = None
        next_offset_peer_type = None
        
        if result.messages:
            last_msg = result.messages[-1]
            next_offset_id = last_msg.id
            if last_msg.peer_id:
                if isinstance(last_msg.peer_id, types.PeerChannel):
                    next_offset_peer_id = last_msg.peer_id.channel_id
                    next_offset_peer_type = "channel"
                elif isinstance(last_msg.peer_id, types.PeerChat):
                    next_offset_peer_id = last_msg.peer_id.chat_id
                    next_offset_peer_type = "chat"
                elif isinstance(last_msg.peer_id, types.PeerUser):
                    next_offset_peer_id = last_msg.peer_id.user_id
                    next_offset_peer_type = "user"

        return {
            "results": messages_list,
            "next_rate": next_rate,
            "offset_id": next_offset_id,
            "offset_peer_id": next_offset_peer_id,
            "offset_peer_type": next_offset_peer_type,
            "has_more": bool(next_rate or next_offset_id)
        }

    def search_hashtag(
        self, 
        hashtag: str, 
        limit: int,
        offset_rate: int = 0, 
        offset_id: int = 0, 
        offset_peer_id: Optional[int] = None, 
        offset_peer_type: Optional[str] = None
    ) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(
            self._search_hashtag_async(hashtag, limit, offset_rate, offset_id, offset_peer_id, offset_peer_type), 
            loop
        )
        return future.result()

    async def _search_user_groups_async(self, username_or_id: str) -> Dict[str, Any]:
        if not self.client or not await self.client.is_user_authorized():
            raise Exception("Telegram client is not authorized")

        try:
            if username_or_id.isdigit():
                target = await self.client.get_input_entity(int(username_or_id))
            else:
                clean_username = username_or_id.lstrip('@')
                target = await self.client.get_input_entity(clean_username)
        except Exception as e:
            raise Exception(f"Could not resolve username/ID: {str(e)}")

        result = await self.client(GetCommonChatsRequest(
            user_id=target,
            max_id=0,
            limit=100
        ))
        
        chats_list = []
        seen_ids = set()
        
        # 1. Process live common chats
        for chat in result.chats:
            is_channel = isinstance(chat, types.Channel)
            is_group = is_channel and not getattr(chat, 'broadcast', False)
            is_broadcast = is_channel and getattr(chat, 'broadcast', False)
            chat_type = "group" if is_group else ("channel" if is_broadcast else "chat")
            
            seen_ids.add(chat.id)
            chats_list.append({
                "id": chat.id,
                "title": chat.title,
                "username": getattr(chat, 'username', None),
                "type": chat_type,
                "participants_count": getattr(chat, 'participants_count', None),
                "link": f"https://t.me/{chat.username}" if getattr(chat, 'username', None) else None,
                "source": "live"
            })

        # 2. Process database memberships
        try:
            target_id = None
            if hasattr(target, 'user_id'):
                target_id = target.user_id
            elif hasattr(target, 'id'):
                target_id = target.id
            
            if not target_id:
                try:
                    full_user = await self.client.get_entity(target)
                    target_id = full_user.id
                except Exception:
                    pass

            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            rows = []
            if target_id:
                cursor.execute("""
                SELECT c.id, c.title, c.username, c.type, c.member_count 
                FROM chats c JOIN members m ON c.id = m.chat_id 
                WHERE m.user_id = ?
                """, (target_id,))
                rows = cursor.fetchall()
            
            if not rows and not username_or_id.isdigit():
                clean_username = username_or_id.lstrip('@').strip().lower()
                cursor.execute("""
                SELECT c.id, c.title, c.username, c.type, c.member_count 
                FROM chats c JOIN members m ON c.id = m.chat_id 
                WHERE LOWER(m.username) = ? OR LOWER(m.username) LIKE ?
                """, (clean_username, f"%{clean_username}%"))
                rows = cursor.fetchall()

            for row in rows:
                chat_id = row[0]
                if chat_id not in seen_ids:
                    seen_ids.add(chat_id)
                    chats_list.append({
                        "id": chat_id,
                        "title": row[1],
                        "username": row[2],
                        "type": row[3],
                        "participants_count": row[4],
                        "link": f"https://t.me/{row[2]}" if row[2] else None,
                        "source": "database"
                    })
            conn.close()
        except Exception as e:
            print(f"Error querying local DB for user groups: {e}")

        return {
            "results": chats_list,
            "note": "این لیست ترکیبی از گروه‌های مشترک زنده و گروه‌های ذخیره شده در دیتابیس محلی شما است."
        }

    def search_user_groups(self, username_or_id: str) -> Dict[str, Any]:
        future = asyncio.run_coroutine_threadsafe(self._search_user_groups_async(username_or_id), loop)
        return future.result()

    async def _crawl_group_async(self, group_username_or_id: str):
        self.crawling_status[group_username_or_id] = {
            "status": "crawling",
            "crawled": 0,
            "total": 0,
            "error": None
        }
        
        try:
            if not self.client:
                raise Exception("Client not initialized")
            
            # Resolve group
            try:
                if group_username_or_id.isdigit():
                    entity = await self.client.get_entity(int(group_username_or_id))
                else:
                    clean_group = group_username_or_id.lstrip('@').strip()
                    if 't.me/' in clean_group:
                        clean_group = clean_group.split('/')[-1]
                    entity = await self.client.get_entity(clean_group)
            except Exception as e:
                raise Exception(f"Could not resolve group: {str(e)}")

            if not isinstance(entity, (types.Chat, types.Channel)):
                raise Exception("Entity is not a group or channel")

            chat_type = "group"
            if isinstance(entity, types.Channel) and getattr(entity, 'broadcast', False):
                chat_type = "channel"
            elif isinstance(entity, types.Chat):
                chat_type = "group"
            
            member_count = getattr(entity, 'participants_count', 0)
            
            # Save group details to DB
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO chats VALUES (?, ?, ?, ?, ?)", 
                           (entity.id, entity.title, entity.username, chat_type, member_count))
            conn.commit()
            
            self.crawling_status[group_username_or_id]["total"] = member_count or 0
            
            # Scrape members
            count = 0
            async for user in self.client.iter_participants(entity):
                cursor.execute("INSERT OR REPLACE INTO members VALUES (?, ?, ?, ?, ?)",
                               (user.id, user.first_name or "", user.last_name or "", user.username or "", entity.id))
                count += 1
                if count % 100 == 0:
                    conn.commit()
                    self.crawling_status[group_username_or_id]["crawled"] = count
            
            conn.commit()
            conn.close()
            
            self.crawling_status[group_username_or_id]["status"] = "completed"
            self.crawling_status[group_username_or_id]["crawled"] = count
            
        except Exception as e:
            self.crawling_status[group_username_or_id]["status"] = "failed"
            self.crawling_status[group_username_or_id]["error"] = str(e)

    def crawl_group(self, group_username_or_id: str) -> Dict[str, Any]:
        asyncio.run_coroutine_threadsafe(self._crawl_group_async(group_username_or_id), loop)
        return {"status": "started"}


manager = TelegramManager()
# Trigger background client connection on start if config exists
if manager.api_id and manager.api_hash:
    manager.init_client()

# ----------------- Pure Python HTTP Server Handler -----------------
class TelegramSearchHTTPHandler(BaseHTTPRequestHandler):
    
    def log_message(self, format, *args):
        # Override to suppress standard HTTP logging to terminal unless desired
        pass

    def send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def send_json(self, status_code: int, data: Dict[str, Any]):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def send_error_json(self, status_code: int, message: str):
        self.send_json(status_code, {"detail": message})

    def serve_static(self, rel_path: str):
        # Serve files from static/ directory
        if not rel_path or rel_path == "/":
            rel_path = "/index.html"
        
        # Prevent directory traversal attacks
        #safe_path = os.path.normpath(rel_path).lstrip('/')
        safe_path = os.path.normpath(rel_path).lstrip('/\\')
        full_path = os.path.join(os.path.dirname(__file__), "static", safe_path)
        
        if not full_path.startswith(os.path.join(os.path.dirname(__file__), "static")):
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"403 Forbidden")
            return

        if not os.path.exists(full_path) or os.path.isdir(full_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"404 Not Found")
            return

        # Determine MIME type
        content_type = 'text/plain'
        if full_path.endswith('.html'):
            content_type = 'text/html; charset=utf-8'
        elif full_path.endswith('.css'):
            content_type = 'text/css; charset=utf-8'
        elif full_path.endswith('.js'):
            content_type = 'application/javascript; charset=utf-8'
        elif full_path.endswith('.png'):
            content_type = 'image/png'
        elif full_path.endswith('.jpg') or full_path.endswith('.jpeg'):
            content_type = 'image/jpeg'

        try:
            with open(full_path, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(f"500 Internal Error: {e}".encode('utf-8'))

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Route API requests
        if path.startswith("/api/"):
            try:
                if path == "/api/status":
                    status_info = manager.get_status()
                    self.send_json(200, status_info)
                
                elif path == "/api/search/messages":
                    q = query_params.get("query", [""])[0]
                    limit = int(query_params.get("limit", [50])[0])
                    offset_rate = int(query_params.get("offset_rate", [0])[0])
                    offset_id = int(query_params.get("offset_id", [0])[0])
                    
                    offset_peer_id = query_params.get("offset_peer_id", [None])[0]
                    offset_peer_type = query_params.get("offset_peer_type", [None])[0]
                    if offset_peer_id == 'null' or offset_peer_id == 'None':
                        offset_peer_id = None
                    if offset_peer_type == 'null' or offset_peer_type == 'None':
                        offset_peer_type = None

                    if not q:
                        return self.send_error_json(400, "Query parameter 'query' is required")
                    res = manager.search_messages(
                        q, limit, offset_rate, offset_id, offset_peer_id, offset_peer_type
                    )
                    self.send_json(200, res)

                elif path == "/api/search/chats":
                    q = query_params.get("query", [""])[0]
                    limit = int(query_params.get("limit", [50])[0])
                    if not q:
                        return self.send_error_json(400, "Query parameter 'query' is required")
                    res = manager.search_chats(q, limit)
                    self.send_json(200, res)

                elif path == "/api/search/hashtag":
                    h = query_params.get("hashtag", [""])[0]
                    limit = int(query_params.get("limit", [50])[0])
                    offset_rate = int(query_params.get("offset_rate", [0])[0])
                    offset_id = int(query_params.get("offset_id", [0])[0])
                    
                    offset_peer_id = query_params.get("offset_peer_id", [None])[0]
                    offset_peer_type = query_params.get("offset_peer_type", [None])[0]
                    if offset_peer_id == 'null' or offset_peer_id == 'None':
                        offset_peer_id = None
                    if offset_peer_type == 'null' or offset_peer_type == 'None':
                        offset_peer_type = None

                    if not h:
                        return self.send_error_json(400, "Query parameter 'hashtag' is required")
                    res = manager.search_hashtag(
                        h, limit, offset_rate, offset_id, offset_peer_id, offset_peer_type
                    )
                    self.send_json(200, res)

                elif path == "/api/search/user-groups":
                    u = query_params.get("username_or_id", [""])[0]
                    if not u:
                        return self.send_error_json(400, "Query parameter 'username_or_id' is required")
                    res = manager.search_user_groups(u)
                    self.send_json(200, res)
                
                elif path == "/api/crawl/status":
                    self.send_json(200, manager.crawling_status)
                
                else:
                    self.send_error_json(404, "Endpoint not found")
            except Exception as e:
                self.send_error_json(500, str(e))
        else:
            # Serve static files
            self.serve_static(path)

    def do_POST(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path

        if path.startswith("/api/"):
            try:
                # Read POST body
                content_length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(body) if body else {}

                if path == "/api/configure":
                    api_id = str(data.get("api_id", "")).strip()
                    api_hash = str(data.get("api_hash", "")).strip()
                    
                    if not api_id or not api_hash:
                        manager.clear_config()
                        manager.logout()
                        return self.send_json(200, {"status": "unconfigured"})
                        
                    try:
                        int(api_id)
                    except ValueError:
                        return self.send_error_json(400, "api_id must be a numeric integer")

                    manager.save_config(api_id, api_hash)
                    success = manager.init_client()
                    if not success:
                        manager.clear_config()
                        return self.send_error_json(500, "Failed to initialize Telegram client with credentials")
                    
                    self.send_json(200, manager.get_status())

                elif path == "/api/send-code":
                    phone = str(data.get("phone", "")).strip()
                    if not phone:
                        return self.send_error_json(400, "Phone number is required")
                    
                    if not manager.api_id or not manager.api_hash:
                        return self.send_error_json(400, "App is not configured with API credentials")

                    res = manager.send_code(phone)
                    self.send_json(200, res)

                elif path == "/api/login-code":
                    code = str(data.get("code", "")).strip()
                    if not code:
                        return self.send_error_json(400, "Verification code is required")
                    
                    res = manager.login_code(code)
                    self.send_json(200, res)

                elif path == "/api/login-password":
                    password = str(data.get("password", "")).strip()
                    if not password:
                        return self.send_error_json(400, "Password is required")
                    
                    res = manager.login_password(password)
                    self.send_json(200, res)

                elif path == "/api/logout":
                    manager.logout()
                    self.send_json(200, {"status": "disconnected"})

                elif path == "/api/crawl":
                    group = str(data.get("group", "")).strip()
                    if not group:
                        return self.send_error_json(400, "Group username/ID is required")
                    res = manager.crawl_group(group)
                    self.send_json(200, res)

                else:
                    self.send_error_json(404, "Endpoint not found")

            except Exception as e:
                self.send_error_json(400, str(e))
        else:
            self.send_error_json(405, "Method not allowed")

# ----------------- Server Runner -----------------
def run(server_class=HTTPServer, handler_class=TelegramSearchHTTPHandler, port=8000):
    server_address = ('127.0.0.1', port)
    httpd = server_class(server_address, handler_class)
    print(f"Server running locally at http://127.0.0.1:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping HTTP server...")
        httpd.server_close()

if __name__ == "__main__":
    run(port=8000)
