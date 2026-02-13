from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import os
import sqlite3
import json
import datetime
from ansible.plugins.callback import CallbackBase

DOCUMENTATION = '''
    callback: sqlite_logger
    type: notification
    short_description: Log playbook results to a SQLite database
    description:
      - Logs inventory_hostname, playbook name, module name, task results, and timestamps.
      - Default location: /var/cache/ansible_logs/logs.db
'''

class CallbackModule(CallbackBase):
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'notification'
    CALLBACK_NAME = 'sqlite_logger'

    def __init__(self):
        super(CallbackModule, self).__init__()
        self.db_path = os.getenv('ANSIBLE_SQLITE_PATH', '/var/cache/ansible_logs/logs.db')
        self.db_connection = None
        self.playbook_name = "Ad-Hoc" # Default if running ad-hoc commands
        self.playbook_uuid = "N/A"
        self._initialize_db()

    def _initialize_db(self):
        """Create the database and table, and migrate schema if needed."""
        # 1. Create Directory
        full_path = os.path.abspath(os.path.expanduser(self.db_path))
        db_dir = os.path.dirname(full_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
            except OSError as e:
                self._display.warning(f"SQLite Callback Error: Could not create directory {db_dir}: {e}")
                return

        # 2. Connect and Create Table
        try:
            self.db_connection = sqlite3.connect(full_path, timeout=10)
            cursor = self.db_connection.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    inventory_hostname TEXT,
                    playbook TEXT,
                    playbook_uuid TEXT,
                    module TEXT,
                    task_name TEXT,
                    status TEXT,
                    result TEXT
                )
            ''')
            
            # 3. SCHEMA MIGRATION (For existing databases)
            # We try to add the columns; if they exist, sqlite throws an error which we ignore.
            try:
                cursor.execute("ALTER TABLE task_logs ADD COLUMN playbook TEXT")
            except sqlite3.OperationalError:
                pass # Column likely exists
            
            try:
                cursor.execute("ALTER TABLE task_logs ADD COLUMN playbook_uuid TEXT")
            except sqlite3.OperationalError:
                pass # Column likely exists
            
            try:
                cursor.execute("ALTER TABLE task_logs ADD COLUMN module TEXT")
            except sqlite3.OperationalError:
                pass # Column likely exists

            self.db_connection.commit()
        except sqlite3.Error as e:
            self._display.warning(f"SQLite Callback Error: Could not initialize DB at {full_path}: {e}")

    def v2_playbook_on_start(self, playbook):
        """Capture the playbook name when execution starts."""
        self.playbook_name = os.path.basename(playbook._file_name)
        if hasattr(playbook, '_uuid'):
            self.playbook_uuid = playbook._uuid

    def _log_result(self, result, status):
        """Helper to write result to DB."""
        if not self.db_connection:
            return

        inventory_hostname = result._host.get_name()
        task_name = result.task_name
        # Extract the module name (e.g., 'command', 'copy', 'yum')
        module_name = result._task.action 
        
        try:
            result_json = json.dumps(result._result, default=str)
        except Exception:
            result_json = "Could not serialize result"

        try:
            cursor = self.db_connection.cursor()
            cursor.execute('''
                INSERT INTO task_logs (timestamp, inventory_hostname, playbook, playbook_uuid, module, task_name, status, result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (datetime.datetime.now(), inventory_hostname, self.playbook_name, self.playbook_uuid, module_name, task_name, status, result_json))
            self.db_connection.commit()
        except sqlite3.Error as e:
            self._display.warning(f"SQLite Callback Error: Could not log task: {e}")

    # --- Callback Overrides ---
    def v2_runner_on_ok(self, result):
        status = 'CHANGED' if result._result.get('changed', False) else 'OK'
        self._log_result(result, status)

    def v2_runner_on_failed(self, result, ignore_errors=False):
        status = 'FAILED_IGNORED' if ignore_errors else 'FAILED'
        self._log_result(result, status)

    def v2_runner_on_unreachable(self, result):
        self._log_result(result, 'UNREACHABLE')

    def v2_runner_on_skipped(self, result):
        self._log_result(result, 'SKIPPED')
