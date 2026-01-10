from flask import Flask, request, jsonify, send_file
import sqlite3
import hashlib
import os
import random
import sys


class App:
    
    def __init__(self):
        
        self.app = Flask(__name__)
        

        
        self._initialize_database()
        
        self.app.add_url_rule("/upload", view_func=self._on_upload_data, methods=["POST"])
        self.app.add_url_rule("/remove", view_func=self._on_remove_data, methods=["DELETE"])
        self.app.add_url_rule("/get_data", view_func=self._on_get_server_world_data, methods=["GET"])
        self.app.add_url_rule("/create", view_func=self._on_create_world, methods=["POST"])
        self.app.add_url_rule("/exists", view_func=self._on_does_world_exist, methods=["GET"])
        self.app.add_url_rule("/download", view_func=self._on_download_file, methods=["GET"])
    
    """
    PLEASE DON'T FORGET TO CALL .CLOSE() ON THE CONNECTION ONCE YOU ARE DONE, THANKS!!!
    WE DON'T WANT MEMORY LEAKS PLAGUING OUR SERVER!!!
    """
    def _get_db(self):
        
        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()
        
        return (conn, cursor)
    
    
        
        
    def _initialize_database(self):
        
        conn, cursor = self._get_db()
        
        
        cursor.execute("CREATE TABLE IF NOT EXISTS worlds (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
    
    def _does_table_exist(self, name: str) -> bool:
        
        conn, cursor = self._get_db()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (name,))
        
        result = cursor.fetchone()
        
        conn.close()
        
        return result is not None
    
    def _on_get_server_world_data(self):
        
        
        id = request.args.get("world")
        
        if id == None: return jsonify(ok=False, message="No world ID provided"), 400
        
        table_name = f"world_{id}"
        
        if not self._does_table_exist(table_name):
            return jsonify(ok=False, message="World not found"), 404
        
        # Read through the entire entry of the table
        
        
        conn, cursor = self._get_db()
        
        cursor.execute(f"SELECT * from {table_name}")
        
        result = cursor.fetchall()
        
        conn.close()
        
        returnedData = []
        
        
        for row in result:
            
            # By order: ID (int) Path (string) Hash (string)
            
            id, path, hash = row
            
            returnedData.append(
                {
                    "id": id,
                    "path": path,
                    "hash": hash
                }
            )
        
        return jsonify(ok=True, data=returnedData), 200
    
    def _hash_bytes(self, b: bytes):
        
        sha1 = hashlib.sha1()
        sha1.update(b)
        return sha1.hexdigest()
    
    def _on_download_file(self):
        
        world_id = request.args.get("world")
        hash = request.args.get("blob")
        
        if world_id == None or hash == None:
            return jsonify(ok=False, message="No world ID or hash provided"), 400
        
        folder_name = f"world_{world_id}"
        
        path = f"objects/{folder_name}/blob_{hash}.bin"
        
        if not os.path.exists(path):
            return jsonify(ok=False, message="File not found"), 404
        
        
        return send_file(path, as_attachment=True)
    
    def _on_upload_data(self):
        
        if "file" not in request.files:
            return jsonify(ok=False, message="No file provided"), 400
        
        file = request.files["file"]
        treepath = request.form.get("path")
        worldid = request.form.get("world")
        
        if worldid == None:
            return jsonify(ok=False, message="No world ID provided"), 400
        
        if treepath == None:
            return jsonify(ok=False, message="No path provided"), 400
        
        if file.filename == "":
            return jsonify(ok=False, message="No file provided"), 400
        
        if self._does_table_exist(f"world_{worldid}") == False:
            return jsonify(ok=False, message="World not found"), 404
        
        table_name = f"world_{worldid}"
        
        file_data: bytes = file.read()
        
        
        print(f"Received: {len(file_data)} bytes from the client")
        
        
        
        
        
        file_hash = self._hash_bytes(file_data)
        

        compressed_file_data = file_data
        
        # print(f"File hash: {file_hash}, compression ratio: {len(file_data)/len(compressed_file_data)}")
        
        conn, cursor = self._get_db()
        
        cursor.execute(
            f"INSERT OR REPLACE INTO {table_name} (path, hash) VALUES (?, ?)",
            (treepath, file_hash)
        )
        
        
        
        # Create directory
        
        os.makedirs(f"objects/{table_name}", exist_ok=True)
        
        with open(f"objects/{table_name}/blob_{file_hash}.bin", "wb") as f:
            f.write(compressed_file_data)
            
        conn.commit()
        
        conn.close()
            
        return jsonify(ok=True, message="Uploaded"), 200    
        
    def _on_remove_data(self):
        
        id = request.args.get("world")
        
        if id == None:
            return jsonify(ok=False, message="No world ID provided"), 400
        
        table_name = f"world_{id}"
        
        if not self._does_table_exist(table_name):
            return jsonify(ok=False, message="World not found"), 404
        
        # Delete a file
        
        file_path = request.args.get("path")
        
        if file_path == None:
            return jsonify(ok=False, message="No path provided"), 400
        
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
        
            if os.path.exists(f"objects/{table_name}/blob_{hash}.bin"):
                os.remove(f"objects/{table_name}/blob_{hash}.bin")
            else:
                print(f"Warn: file not found: objects/{table_name}/blob_{hash}.bin")
        else:
            print(f"dbg: Another entry still using this blob")
        
        conn.commit()
        conn.close()
        
        return jsonify(ok=True, message="File deleted"), 200
        
        
        
    def _create_world_storage(self):
        
        # Do user validation, etc.
        # TODO: User validation/authentication & backend anti-abuse
        
        
        print(f"Creating new world storage entry in DB")
        
        conn, cursor = self._get_db()
        
        cursor.execute("INSERT INTO worlds (id) VALUES (?)", (random.randint(1000000, 9999999),))
        new_world_id = cursor.lastrowid
        conn.commit()
        
        table_name = f"world_{new_world_id}"
        
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                path STRING UNIQUE,
                hash STRING
            )
            """
        )
        
        conn.commit()
        conn.close()
        
        print(f"Entry successfully created")
        
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
    
    def run(self):
        if sys.platform.startswith("linux"): return
        self.app.run()