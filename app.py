from flask import (
    Flask,
    request,
    jsonify,
    send_file,
    send_from_directory,
    render_template,
)
from werkzeug.datastructures import FileStorage
import sqlite3
import hashlib
import os
import random
import logging
import jwt
import sys
import shutil
import json
import lzma
import io
import argon2
import threading
from typing import TypedDict
from pathlib import Path
from datetime import datetime, timedelta
from flask_cors import CORS
from secret_key import SECRET_KEY
import secrets
import time
import string

logging.basicConfig(
    level=logging.INFO,  # Minimum level to log
    format="%(asctime)s UTC/GMT - %(name)s - %(levelname)s - %(message)s",
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)
file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter(
    "%(asctime)s UTC/GMT - %(name)s - %(levelname)s - %(message)s"
)
file_formatter.converter = time.gmtime
file_handler.setFormatter(file_formatter)

logger = logging.getLogger(__name__)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def generate_slug(length=5):
    alphabet = string.ascii_letters + string.digits  # a-zA-Z0-9
    return "".join(secrets.choice(alphabet) for _ in range(length))


ADMIN_USERNAME = "$argon2id$v=19$m=262144,t=6,p=4$9K5BLdDU2RRDslxij+wnVw$T+lEuuqudJKFWKrY20pfcUSA1hAAGzga02oSP+AqaRY"
ADMIN_HASHED_PASSWORD = "$argon2id$v=19$m=262144,t=6,p=4$gvj5zf9Szcc2MNnpsKctWg$0I7QS/A2VIZPNPZr+4bhsBkLPMePUtkhwtGELoWyS1A"

ph = argon2.PasswordHasher()


class WorldDataStatisticsItem(TypedDict):

    id: str
    lastModifiedTime: str
    size: int


def human_readable_time(dt: datetime) -> str:
    """
    Converts a datetime object to a human-readable string.
    """

    now = datetime.now()
    diff = now - dt

    seconds = diff.total_seconds()
    minutes = seconds / 60
    hours = seconds / 3600
    days = seconds / 86400
    months = days / 30
    years = days / 365

    if seconds < 60:
        return "just now"
    elif minutes < 60:
        return f"{int(minutes)} minute{'s' if minutes >= 2 else ''} ago"
    elif hours < 24:
        return f"{int(hours)} hour{'s' if hours >= 2 else ''} ago"
    elif days < 2:
        return "Yesterday"
    elif days < 30:
        return f"{int(days)} day{'s' if days >= 2 else ''} ago"
    elif months < 12:
        return f"{int(months)} month{'s' if months >= 2 else ''} ago"
    else:
        return f"{int(years)} year{'s' if years >= 2 else ''} ago"


class App:

    def __init__(self):
        # Base directory: PythonAnywhere path for linux, else the module directory
        if sys.platform.startswith("linux"):
            self.base_dir = "/home/mcworldsyncutils/mysite"
        else:
            self.base_dir = os.path.abspath(os.path.dirname(__file__))

        logger.info("Templates directory: %s" % os.path.join(self.base_dir, "templates"))
        logger.info("App started")

        self.worlds_lock = threading.Lock()

        self.app = Flask(
            __name__, template_folder=os.path.join(self.base_dir, "templates")
        )
        CORS(self.app)

        self.revoked_tokens: set[int] = set()

        self._initialize_database()
        self._enable_write_ahead_logging()
        self._migrate_database()
        self._run_deferred_tasks()

        self.app.add_url_rule(
            "/upload", view_func=self._on_upload_data, methods=["POST"]
        )
        self.app.add_url_rule(
            "/upload/batch", view_func=self._on_upload_data_batched, methods=["POST"]
        )
        self.app.add_url_rule(
            "/remove", view_func=self._on_remove_data, methods=["DELETE"]
        )
        self.app.add_url_rule(
            "/remove/batch", view_func=self._on_remove_data_batched, methods=["POST"]
        )
        self.app.add_url_rule(
            "/get_data", view_func=self._on_get_server_world_data, methods=["GET"]
        )
        self.app.add_url_rule(
            "/create", view_func=self._on_create_world, methods=["POST"]
        )
        self.app.add_url_rule(
            "/delete_world", view_func=self._on_delete_world, methods=["DELETE"]
        )

        self.app.add_url_rule(
            "/exists", view_func=self._on_does_world_exist, methods=["GET"]
        )
        self.app.add_url_rule(
            "/download", view_func=self._on_download_file, methods=["GET"]
        )
        self.app.add_url_rule(
            "/api/worlds", view_func=self._query_worlds, methods=["GET"]
        )
        self.app.add_url_rule("/api/login", view_func=self._login, methods=["POST"])
        self.app.add_url_rule(
            "/api/create_redirect_url",
            view_func=self._create_redirect_url,
            methods=["POST"],
        )
        self.app.add_url_rule(
            "/api/get_redirect_location",
            view_func=self._find_redirect_url,
            methods=["GET"],
        )
        self.app.add_url_rule(
            "/api/get_free_space", view_func=self._get_free_space, methods=["GET"]
        )
        self.app.add_url_rule(
            "/api/world/compression_info",
            view_func=self._get_world_files_compression_info,
            methods=["GET"],
        )
        self.app.add_url_rule("/manage", view_func=self._manage, methods=["GET"])
        self.app.add_url_rule("/", view_func=self._landing, methods=["GET"])
        self.app.add_url_rule(
            "/api/revoke_token", view_func=self._revoke_token, methods=["GET"]
        )
        self.app.add_url_rule("/r", view_func=self._redirect)

        self.app.add_url_rule(
            "/assets/<path:filename>", view_func=self._serve_assets, methods=["GET"]
        )

        logger.info("Main thread ready to serve requests")

    @staticmethod
    def _get_last_modified_time_file_unix(file_path: str):
        return int(os.stat(file_path).st_mtime)

    def _run_deferred_startup_tasks_task(self):
        logger.info("Run deferred tasks...")
        try:
            self.worlds_lock.acquire()
            self._clean_database()
            self._migrate_per_file_compressions()
            self._detect_double_compression()
        except Exception as e:
            logger.error(f"deferred tasks failed: {e}")
        finally:
            logger.info("deferred tasks worlds lock released")
            self.worlds_lock.release()
        logger.info("Deferred tasks complete")

    def _run_deferred_tasks(self):

        thread = threading.Thread(
            target=self._run_deferred_startup_tasks_task,
            daemon=True,
            name="DeferredStartupTasks-Thread",
        )
        thread.start()
        logger.info("Deferred tasks thread started")

    def _enable_write_ahead_logging(self):
        conn = sqlite3.connect(os.path.join(self.base_dir, "database.db"))
        conn.execute("PRAGMA journal_mode=TRUNCATE")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.commit()
        conn.close()

    def _clean_database(self):

        logger.info("running clean db job")

        conn, cursor = self._get_db()

        try:

            cursor.execute("SELECT * FROM worlds")
            rows = cursor.fetchall()

            for row in rows:
                id = int(row[0])
                table_name = "world_" + str(id)

                # check if table exists

                cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                )
                table_exists = cursor.fetchone() is not None

                if not table_exists:
                    try:
                        logger.info(
                            f"[ DELETE WORLD ] delete {table_name}. reason: table does not exist"
                        )
                        cursor.execute("DELETE FROM worlds WHERE id = ?", (id,))
                    except Exception as e:
                        logger.error(f"delete world failed: {e}")
                    continue

                # check if folder exists

                folder_path = os.path.join(self.base_dir, "objects", table_name)
                folder_exists = os.path.exists(folder_path)

                if not folder_exists:
                    try:
                        logger.info(
                            f"[ DELETE WORLD ] delete {table_name}. reason: folder does not exist"
                        )
                        # delete row
                        cursor.execute("DELETE FROM worlds WHERE id = ?", (id,))
                        # drop table if it exists
                        logger.info(
                            f"[ DROP TABLE ] drop {table_name}, reason: folder does not exist"
                        )
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception as e:
                        logger.error(f"delete world failed: {e}")
                    continue

                # check if folder is empty

                folder_contents = os.listdir(folder_path)
                if len(folder_contents) == 0:
                    try:
                        logger.info(
                            f"[ DELETE WORLD ] delete {table_name}. reason: folder is empty"
                        )
                        # delete row
                        cursor.execute("DELETE FROM worlds WHERE id = ?", (id,))
                        # drop table if it exists
                        logger.info(
                            f"[ DROP TABLE ] drop {table_name}, reason: folder is empty"
                        )
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except Exception as e:
                        logger.error(f"delete world failed: {e}")
                    continue

            for row in rows:
                id = int(row[0])
                table_name = "world_" + str(id)

                if not self._does_table_exist(table_name):
                    continue  # cleaned up previously

                cursor.execute(f"SELECT * FROM {table_name}")

                for file in cursor.fetchall():
                    id = file[0]
                    path = file[1]
                    hash = file[2]

                    # check if file exists, and remove row if it doesn't

                    blob_path = os.path.join(
                        self.base_dir, "objects", table_name, f"blob_{hash}.bin"
                    )
                    if os.path.exists(blob_path) is False:
                        # delete row
                        try:
                            cursor.execute(
                                f"DELETE FROM {table_name} WHERE id = ?", (id,)
                            )
                            logger.info(
                                f"[ DELETE ROW ] delete in {table_name} row {path} because it doesn't exist"
                            )
                        except Exception as e:
                            logger.error(f"failed to delete row: {e}")
                        continue

                # do the opposite, loop through all files, check if it exists in the table, and delete file if it doesn't

                folder_contents = os.listdir(
                    os.path.join(self.base_dir, "objects", table_name)
                )
                for file in folder_contents:
                    if file.startswith("blob_"):
                        try:
                            hash = file[5:-4]
                            cursor.execute(
                                f"SELECT * FROM {table_name} WHERE hash = ?", (hash,)
                            )
                            if cursor.fetchone() is None:
                                logger.info(
                                    f"[ DELETE FILE ] delete in {table_name} filehash {hash} because it doesn't exist in table"
                                )
                                os.remove(
                                    os.path.join(
                                        self.base_dir, "objects", table_name, file
                                    )
                                )
                        except Exception as e:
                            logger.error(f"failed to delete file: {e}")

            for world in os.listdir(os.path.join(self.base_dir, "objects")):
                if not world.startswith("world_"):
                    continue

                # check if table exists

                if not self._does_table_exist(world):
                    # delete row
                    # logger.info(f"[ DELETE WORLD ] delete {world}. reason: table does not exist")
                    # cursor.execute(f"DROP TABLE IF EXISTS {world}")
                    # delete from worlds if it exists

                    try:
                        shutil.rmtree(os.path.join(self.base_dir, "objects", world))
                    except Exception as e:
                        logger.error("unused folder delete failed")
                        logger.error(e)

                    logger.info(
                        f"DROP TABLE IF EXISTS: {world} reason: table does not exist"
                    )
                    try:
                        cursor.execute(
                            "DELETE FROM worlds WHERE id = ?", (int(world[6:]),)
                        )
                    except Exception as e:
                        logger.error(f"cannot delete from row: {e}")
                    continue
        except Exception as e:
            logger.error(f"cleanup job failed: {e}")
        conn.commit()
        conn.close()
        logger.info("clean db job complete")
        logger.info("running vacumn job")
        try:
            conn, cursor = self._get_db()
            cursor.execute("VACUUM")
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"vacumn job failed: {e}")
        logger.info("vacumn job complete")

    def _get_free_space(self):
        _total, _used, free = shutil.disk_usage(self.base_dir)
        return jsonify(ok=True, message="OK", data=free)

    def _manage(self):

        return render_template("index.html")

    def _landing(self):
        return render_template("landing.html")

    def _redirect(self):
        return render_template("redirect.html")

    def _serve_assets(self, filename):
        return send_from_directory(
            os.path.join(self.base_dir, "static/assets"), filename
        )

    def _get_db(self):
        """
        PLEASE DON'T FORGET TO CALL .CLOSE() ON THE CONNECTION ONCE YOU ARE DONE, THANKS!!!
        WE DON'T WANT MEMORY LEAKS PLAGUING OUR SERVER!!!
        """
        db_path = os.path.join(self.base_dir, "database.db")
        conn = sqlite3.connect(
            db_path, isolation_level=None, timeout=30
        )  # auto-commit, 30 second lock wait timeout
        cursor = conn.cursor()
        return (conn, cursor)

    def _initialize_database(self):
        conn, cursor = self._get_db()
        cursor.execute("CREATE TABLE IF NOT EXISTS worlds (id INTEGER PRIMARY KEY)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS shortened_urls (id INTEGER PRIMARY KEY, slug TEXT, url TEXT)"
        )
        conn.commit()
        conn.close()

    def _migrate_database(self):
        try:
            conn, cursor = self._get_db()
            cursor.execute("ALTER TABLE worlds ADD COLUMN compressed INTEGER DEFAULT 0")
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"database migration failed: {e}")

    def _load_double_compression_cache(self) -> dict[str, int] | None:
        cache_file_path = os.path.join(
            self.base_dir, "cache", "double_compression_cache.json"
        )
        if os.path.exists(cache_file_path) is False:
            return None
        with open(cache_file_path, "r") as f:
            return json.load(f)

    def _save_double_compression_cache(self, cache_data: dict[str, int]):
        cache_file_path = os.path.join(
            self.base_dir, "cache", "double_compression_cache.json"
        )
        os.makedirs(os.path.dirname(cache_file_path), exist_ok=True)
        with open(cache_file_path, "w") as f:
            json.dump(cache_data, f)

    def _detect_double_compression(self):
        # loop through every file in objects
        conn, cursor = self._get_db()
        cursor.execute("SELECT * FROM worlds")
        rows = cursor.fetchall()

        logger.info("Run double-compression detection")

        double_compression_cache = self._load_double_compression_cache() or {}
        new_compression_cache: dict[str, int] = {}

        for row in rows:
            id = row[0]
            table_name = f"world_{id}"
            if self._does_table_exist(table_name) is False:
                continue
            try:
                cursor.execute(f"SELECT * FROM {table_name}")
                files = cursor.fetchall()
                for file in files:
                    hash = file[2]
                    # get file path
                    file_path = os.path.join(
                        self.base_dir, "objects", table_name, f"blob_{hash}.bin"
                    )
                    if os.path.exists(file_path) is False:
                        continue
                    current_last_modified_time = self._get_last_modified_time_file_unix(
                        file_path
                    )
                    new_compression_cache[file_path] = current_last_modified_time
                    if double_compression_cache.get(file_path) is not None:
                        cached_last_modified_time = double_compression_cache[file_path]
                        if current_last_modified_time == cached_last_modified_time:
                            logger.info(
                                f"Skipped double-compression check: {file_path}"
                            )
                            continue

                    # read
                    with open(file_path, "rb") as f:
                        file_data = f.read()

                    # attempt first decompression
                    try:
                        decompressed_once = lzma.decompress(file_data)
                    except lzma.LZMAError:
                        continue  # file not compressed or corrupted, skip

                    # attempt second decompression
                    try:
                        _ = lzma.decompress(decompressed_once)
                    except lzma.LZMAError:
                        logger.info(f"double-compression not detected for {file_path}")
                        continue  # only single compression, skip
                    else:
                        # Double compression detected, fix by keeping only one layer
                        logger.info(
                            f"[FIX] Double compression detected for {file_path}"
                        )
                        # recompress once if you want to keep compressed storage
                        fixed_data = decompressed_once
                        with open(file_path, "wb") as f:
                            f.write(fixed_data)

                        # update DB compressed flag to 1
                        cursor.execute(
                            f"UPDATE {table_name} SET compressed = 1 WHERE hash = ?",
                            (hash,),
                        )

            except Exception as e:
                logger.error(f"cannot add column: {e}")
                continue
        conn.close()

        self._save_double_compression_cache(new_compression_cache)

    def _migrate_per_file_compressions(self):
        conn, cursor = self._get_db()
        cursor.execute("SELECT * FROM worlds")
        rows = cursor.fetchall()

        for row in rows:
            id = row[0]
            table_name = f"world_{id}"
            if self._does_table_exist(table_name) is False:
                continue
            try:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN compressed INTEGER DEFAULT 0"
                )

                cursor.execute(f"SELECT * FROM {table_name}")
                files = cursor.fetchall()
                for file in files:
                    hash = file[2]
                    # get file path
                    file_path = os.path.join(
                        self.base_dir, "objects", table_name, f"blob_{hash}.bin"
                    )
                    if os.path.exists(file_path) is False:
                        continue

                    # read
                    with open(file_path, "rb") as f:
                        file_data = f.read()

                    # compress
                    isCompressed, processedData = self._compress_file(file_data)

                    logger.info(f"process: {file_path} compressed: {isCompressed}")

                    # write
                    with open(file_path, "wb") as f:
                        f.write(processedData)

                    cursor.execute(
                        f"UPDATE {table_name} SET compressed = ? WHERE hash = ?",
                        (isCompressed, hash),
                    )

            except Exception as e:
                logger.error(f"cannot add column: {e}")
                continue
        conn.close()

    def _generate_unique_slug(self, cursor, length=5):
        while True:
            slug = generate_slug(length)
            cursor.execute("SELECT 1 FROM shortened_urls WHERE slug=?", (slug,))
            if not cursor.fetchone():
                return slug

    def _find_redirect_url(self):
        slug_to_find = request.args.get("slug")

        conn, cursor = self._get_db()

        cursor.execute("SELECT url FROM shortened_urls WHERE slug = ?", (slug_to_find,))
        row = cursor.fetchone()  # fetchone() returns None if no match

        if row:
            url = row[0]  # url is the first (and only) column selected
            logger.info(f"URL for slug {slug_to_find}: {url}")
            return jsonify(ok=True, message="URL found", url=url), 200
        else:
            logger.info(f"No URL found for slug {slug_to_find}")
            return jsonify(ok=False, message="No URL found"), 404

        conn.close()

    def _create_redirect_url(self):
        data = request.get_json()
        if not data:
            return jsonify(ok=False, message="Missing JSON body"), 400

        url = data.get("url")
        if not url:
            return jsonify(ok=False, message="Missing URL"), 400

        username = data.get("username")
        password = data.get("password")
        if not username:
            return jsonify(ok=False, message="Missing username"), 400
        if not password:
            return jsonify(ok=False, message="Missing password"), 400

        if not self._verify_credentials(username, password):
            return jsonify(ok=False, message="Invalid credentials"), 401

        # Now write to database, generate a random slug

        conn, cursor = self._get_db()
        slug = self._generate_unique_slug(cursor, length=7)
        cursor.execute(
            "INSERT INTO shortened_urls (slug, url) VALUES (?, ?)", (slug, url)
        )
        conn.commit()
        conn.close()

        return jsonify(ok=True, message="URL created", url=f"/r?q={slug}&v2=true"), 200

    def _revoke_token(self):

        token = request.args.get("token")

        if not token:
            return jsonify(ok=False, message="No token provided"), 400

        if not self._is_token_valid(token):
            return jsonify(ok=False, message="Invalid token"), 401

        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        id = payload.get("id")

        if id == None:
            return jsonify(ok=False, message="Invalid token"), 401

        self.revoked_tokens.add(id)

        return jsonify(ok=True, message="Token revoked"), 200

    def _on_delete_world(self):
        world = request.args.get("world")
        if not world:
            return jsonify(ok=False, message="No world ID provided"), 400

        token = request.args.get("token")
        if not token:
            return jsonify(ok=False, message="No token provided"), 400

        if not self._is_token_valid(token):
            return jsonify(ok=False, message="Invalid token"), 401

        # Sanitize the world ID: allow only digits
        if not world.isdigit():
            return jsonify(ok=False, message="Invalid world ID"), 400

        world_path = os.path.join(self.base_dir, "objects", f"world_{world}")

        # Ensure the path is within base_dir to prevent traversal
        if not os.path.commonpath([self.base_dir, world_path]).startswith(
            self.base_dir
        ):
            return jsonify(ok=False, message="Invalid world path"), 400

        if not os.path.exists(world_path):
            return jsonify(ok=False, message="World not found"), 404

        try:
            # Delete database entry first
            conn, cursor = self._get_db()
            cursor.execute("DELETE FROM worlds WHERE id = ?", (world,))
            conn.commit()
            conn.close()

            # Recursively delete folder
            shutil.rmtree(world_path)
        except Exception as e:
            logger.error("Deleting world failed: %s" % e)
            return jsonify(ok=False, message=f"Error deleting world: {e}"), 500

        return jsonify(ok=True, message="World deleted"), 200

    def _issue_jwt(self):

        payload = {
            "id": random.randint(1000000, 9999999),
            "exp": datetime.utcnow() + timedelta(minutes=1),
            "iat": datetime.utcnow(),
        }

        token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")

        return token

    def _is_token_valid(self, token: str):
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            id = payload.get("id")
            if id == None:
                return False
            if id in self.revoked_tokens:
                return False
            return True
        except jwt.ExpiredSignatureError:
            return False
        except jwt.InvalidTokenError:
            return False

    def _query_size_of_folder(self, path: str):

        total_size = 0

        for name in os.listdir(path):

            child_path = os.path.join(path, name)
            total_size += os.path.getsize(child_path)

        return total_size

    def _query_last_modified_date_folder(self, path: str):

        folder = Path(path)

        latest_mtime = max(child.stat().st_mtime for child in folder.iterdir())

        return datetime.fromtimestamp(latest_mtime)

    def _query_worlds(self):
        try:

            token = request.args.get("token")
            if token == None:
                return jsonify(ok=False, message="No token provided"), 400

            if not self._is_token_valid(token):
                return jsonify(ok=False, message="Invalid token"), 401

            conn, cursor = self._get_db()
            cursor.execute("SELECT id FROM worlds")
            result = cursor.fetchall()
            conn.close()

            returnedData: list[WorldDataStatisticsItem] = []

            for row in result:

                id = row[0]
                try:
                    world_folder = os.path.join(self.base_dir, "objects", f"world_{id}")

                    total_size = self._query_size_of_folder(world_folder)
                    last_modified_time = self._query_last_modified_date_folder(
                        world_folder
                    )
                    last_modified_time_str = human_readable_time(last_modified_time)

                    returnedData.append(
                        {
                            "id": id,
                            "lastModifiedTime": last_modified_time_str,
                            "size": total_size,
                        }
                    )
                except Exception:
                    logger.error(f"Failed to query details for: {id}")

                    returnedData.append(
                        {"id": id, "lastModifiedTime": "Unknown", "size": 0}
                    )

            return jsonify(ok=True, data=returnedData), 200
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return jsonify(ok=False, message="Internal Server Error"), 500

    def _verify_credentials(self, username: str, password: str):
        isUsernameCorrect = False
        isPasswordCorrect = False

        try:
            ph.verify(ADMIN_USERNAME, username)
            isUsernameCorrect = True
        except argon2.exceptions.VerifyMismatchError:
            logger.warning("Invalid credentials detected")
            pass
        try:
            ph.verify(ADMIN_HASHED_PASSWORD, password)
            isPasswordCorrect = True
        except argon2.exceptions.VerifyMismatchError:
            logger.warning("Invalid credentials detected")
            pass

        if isUsernameCorrect == False or isPasswordCorrect == False:
            return False
        return True

    def _login(self):

        data: dict = request.get_json()

        if not data:
            return jsonify(ok=False, message="Missing JSON body"), 400

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return jsonify(ok=False, message="Missing username or password"), 400

        if not self._verify_credentials(username, password):
            logger.warning("Invalid credentials detected")
            return jsonify(ok=False, message="Invalid credentials"), 401

        token = self._issue_jwt()

        return jsonify(ok=True, message="Login successful", data=token), 200

    def _does_table_exist(self, name: str) -> bool:
        conn, cursor = self._get_db()
        cursor.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """,
            (name,),
        )
        result = cursor.fetchone()
        conn.close()
        return result is not None

    def _on_get_server_world_data(self):
        id = request.args.get("world")
        if id == None:
            return jsonify(ok=False, message="No world ID provided"), 400

        table_name = f"world_{id}"
        if not self._does_table_exist(table_name):
            return jsonify(ok=False, message="World not found"), 404

        conn, cursor = self._get_db()
        cursor.execute(f"SELECT * from {table_name}")
        result = cursor.fetchall()
        conn.close()

        returnedData = []
        for row in result:
            # By order: ID (int) Path (string) Hash (string)
            id = row[0]
            path = row[1]
            hash = row[2]
            returnedData.append({"id": id, "path": path, "hash": hash})

        return jsonify(ok=True, data=returnedData), 200

    def _hash_bytes(self, b: bytes):
        sha1 = hashlib.sha1()
        sha1.update(b)
        return sha1.hexdigest()

    def _compress_file(self, fileData: bytes) -> tuple[bool, bytes]:
        "Returns if file was compressed (0), and the compressed/uncompressed data (1)"

        compressedData = lzma.compress(fileData)

        compressionRatio = 1
        if len(fileData) != 0:
            compressionRatio = len(compressedData) / len(fileData)

        if compressionRatio >= 1:
            # not worth to compress
            logger.info(
                f"Compression ratio: {compressionRatio} -- compression reversed"
            )
            return (False, fileData)
        else:
            logger.info(f"Compression ratio: {compressionRatio} -- compression applied")
            return (True, compressedData)

    def _decompress_file(self, fileData: bytes):
        return lzma.decompress(fileData)

    def _on_download_file(self):
        world_id = request.args.get("world")
        hash = request.args.get("blob")
        client_supports_compression = (
            request.args.get("client_supports_compression") == "true"
        )

        if world_id == None or hash == None:
            return jsonify(ok=False, message="No world ID or hash provided"), 400

        folder_name = f"world_{world_id}"
        path = os.path.join(self.base_dir, "objects", folder_name, f"blob_{hash}.bin")

        if not os.path.exists(path):
            return jsonify(ok=False, message="File not found"), 404
        table_name = f"world_{world_id}"
        if self._does_table_exist(table_name) is False:
            return jsonify(ok=False, message="World not found"), 404
        try:
            logger.info("wait for lock release (wait deferred tasks finished)")
            self.worlds_lock.acquire()
            # Find it in the database
            conn, cursor = self._get_db()
            try:
                # get row
                cursor.execute(f"SELECT * FROM {table_name} WHERE hash = ?", (hash,))
                row = cursor.fetchone()
                if row == None:
                    self.worlds_lock.release()
                    conn.close()
                    return jsonify(ok=False, message="File not found")
            except Exception as e:
                logger.error("Download failed: %s" % e)
                raise Exception(e)
            finally:
                conn.close()

            # Read compressed data
            with open(path, "rb") as f:
                compressed_data = f.read()

            isCompressed = row[3]
            decompressed_data = compressed_data
            if client_supports_compression is False:
                logger.info("old client -- compression unsupported")
                if isCompressed == 1:
                    logger.info("decompress")
                    decompressed_data = self._decompress_file(compressed_data)
                else:
                    logger.info(f"don't decompress: isCompressed = {isCompressed}")
            else:
                logger.info("new client -- compression supported")

            # Wrap in BytesIO so Flask can send it as a file
            file_stream = io.BytesIO(decompressed_data)

            # Optionally, give the downloaded file a name
            filename = "blob.bin"

            return send_file(
                file_stream,
                as_attachment=True,
                download_name=filename,
                mimetype="application/octet-stream",
            )
        except Exception as e:
            logger.error(f"failed to send download: {e}")
            return jsonify(ok=False, message="Internal Server Error"), 500
        finally:
            logger.info("release worlds lock")
            self.worlds_lock.release()

    def _get_world_files_compression_info(self):
        world_id = request.args.get("world")

        table_name = f"world_{world_id}"
        if self._does_table_exist(table_name) is False:
            return jsonify(ok=False, message="World not found"), 404

        conn, cursor = self._get_db()
        cursor.execute(f"SELECT * FROM {table_name}")

        rows = cursor.fetchall()
        compression_info_dict: dict[str, bool] = {}

        for row in rows:
            hash = row[2]
            compressed = row[3]
            compression_info_dict[hash] = True if compressed else False
        conn.close()

        return jsonify(ok=True, data=compression_info_dict, message="OK"), 200

    def _insert_file(
        self,
        file: FileStorage,
        treepath: str | None,
        worldid: str | None,
        client_compressed: bool = False,
        client_is_compressed: bool = False,
        client_provided_hash: str | None = None,
    ):
        if worldid is None:
            return jsonify(ok=False, message="No world ID provided"), 400

        if treepath is None:
            return jsonify(ok=False, message="No path provided"), 400

        if file.filename == "":
            return jsonify(ok=False, message="No file provided"), 400

        if self._does_table_exist(f"world_{worldid}") is False:
            return jsonify(ok=False, message="World not found"), 404
        try:
            logger.info("wait for lock release (wait deferred tasks finished)")
            self.worlds_lock.acquire()
            table_name = f"world_{worldid}"
            file_data: bytes = file.read()
            logger.info(f"Received: {len(file_data)} bytes from the client")

            file_hash = client_provided_hash
            if client_provided_hash == None:
                file_hash = self._hash_bytes(file_data)

            # if client is compressing, trust client with compression data

            is_compressed = False
            compressed_file_data = file_data

            if client_compressed is False:
                logger.info("old client -- does not support compression")
                is_compressed, compressed_file_data = self._compress_file(file_data)
            else:
                logger.info("new client -- supports compression")
                is_compressed = client_is_compressed
                compressed_file_data = file_data

            conn, cursor = self._get_db()
            cursor.execute(
                f"INSERT OR REPLACE INTO {table_name} (path, hash, compressed) VALUES (?, ?, ?)",
                (treepath, file_hash, is_compressed),
            )

            # Create directory (absolute)
            objects_dir = os.path.join(self.base_dir, "objects", table_name)
            os.makedirs(objects_dir, exist_ok=True)

            blob_path = os.path.join(objects_dir, f"blob_{file_hash}.bin")
            with open(blob_path, "wb") as f:
                f.write(compressed_file_data)

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"file insert failed: {e}")
            raise RuntimeError(f"File Insert Failed: {e}")
        finally:
            logger.info("release worlds lock")
            self.worlds_lock.release()

    def _on_upload_data(self):
        if "file" not in request.files:
            return jsonify(ok=False, message="No file provided"), 400

        file = request.files["file"]
        treepath = request.form.get("path")
        worldid = request.form.get("world")

        client_compressed = request.form.get("client_compressed") == "true"
        client_is_compressed = request.form.get("client_is_compressed") == "true"
        client_provided_hash = request.form.get("client_provided_hash")

        self._insert_file(
            file,
            treepath,
            worldid,
            client_compressed=client_compressed,
            client_is_compressed=client_is_compressed,
            client_provided_hash=(
                client_provided_hash if client_provided_hash != "" else None
            ),
        )

        return jsonify(ok=True, message="Uploaded"), 200

    def _on_upload_data_batched(self):
        files = request.files.getlist("files")
        paths = request.form.getlist("paths")
        client_hashes = request.form.getlist("client_hashes")
        client_is_compressed = request.form.getlist("client_is_compressed")
        client_compressed = request.form.get("client_compressed") == "true"

        worldid = request.form.get("world")

        if not files or len(files) != len(paths):
            return jsonify(ok=False, message="Mismatched files and paths"), 400

        client_hashes_list: list[str] | list[None] = client_hashes

        if not client_is_compressed:
            client_is_compressed = ["false"] * len(files)
            client_hashes_list = [None] * len(files)

        for file, treepath, is_compressed, client_hash in zip(
            files, paths, client_is_compressed, client_hashes_list
        ):
            logger.info(f"upload tree path: {treepath}")
            self._insert_file(
                file,
                treepath,
                worldid,
                client_compressed=client_compressed,
                client_is_compressed=is_compressed == "true",
                client_provided_hash=client_hash,
            )

        return jsonify(ok=True, message="Uploaded"), 200

    def _remove_entry(self, table_name: str, file_path: str):
        conn, cursor = self._get_db()
        cursor.execute(f"""SELECT * FROM {table_name} WHERE path = ?""", (file_path,))
        row = cursor.fetchone()
        if row == None:
            conn.close()
            return jsonify(ok=False, message="File not found"), 404

        id, path, hash = row
        cursor.execute(f"""DELETE FROM {table_name} WHERE id = ?""", (id,))

        # Check if anyone else having the same hash
        cursor.execute(f"""SELECT * FROM {table_name} WHERE hash = ?""", (hash,))
        if cursor.fetchone() == None:
            blob_path = os.path.join(
                self.base_dir, "objects", table_name, f"blob_{hash}.bin"
            )
            if os.path.exists(blob_path):
                os.remove(blob_path)
            else:
                logger.info(f"Warn: file not found: {blob_path}")
        else:
            logger.info("dbg: Another entry still using this blob")

        conn.commit()
        conn.close()

    def _on_remove_data_batched(self):

        paths = request.form.getlist("paths")
        worldid = request.form.get("world")

        if not paths:
            return jsonify(ok=False, message="No paths provided"), 400

        table_name = f"world_{worldid}"

        for path in paths:
            self._remove_entry(table_name, path)

        return jsonify(ok=True, message="Files deleted"), 200

    def _on_remove_data(self):
        id = request.args.get("world")
        if id == None:
            return jsonify(ok=False, message="No world ID provided"), 400

        table_name = f"world_{id}"
        if not self._does_table_exist(table_name):
            return jsonify(ok=False, message="World not found"), 404

        file_path = request.args.get("path")
        if file_path == None:
            return jsonify(ok=False, message="No path provided"), 400

        self._remove_entry(table_name, file_path)

        return jsonify(ok=True, message="File deleted"), 200

    def _create_world_storage(self):
        logger.info("Creating new world storage entry in DB")

        conn, cursor = self._get_db()
        cursor.execute(
            "INSERT INTO worlds (id) VALUES (?)", (random.randint(1000000, 9999999),)
        )
        new_world_id = cursor.lastrowid
        conn.commit()

        table_name = f"world_{new_world_id}"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                path STRING UNIQUE,
                hash STRING,
                compressed INTEGER DEFAULT 0
            )
            """)

        conn.commit()
        conn.close()

        logger.info("Entry successfully created")
        return new_world_id

    def _on_create_world(self):
        created_world_id = self._create_world_storage()
        return jsonify(ok=True, message="World created", data=created_world_id), 200

    def _on_does_world_exist(self):
        id = request.args.get("world")
        if id == None:
            return jsonify(ok=False, message="World Id not provided"), 400

        table_name = f"world_{id}"
        if not self._does_table_exist(table_name):
            return jsonify(ok=False, message="World not found"), 404

        return jsonify(ok=True, message="World found"), 200
