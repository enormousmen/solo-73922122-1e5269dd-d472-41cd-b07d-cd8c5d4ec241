#!/usr/bin/env python3
"""单卷恢复服务。

场景:虚拟机数据盘尚未恢复完,值班员需要立刻恢复业务读写。

- 本地块用 SQLite 持久化(<DATA_DIR>/volume.db);本地缺块时从只读备份镜像读透;
- 后台线程把缺块从备份回填到本地(INSERT OR IGNORE:业务写入永远优先,
  迟到的备份数据不会覆盖已提交的业务整块写入);
- 网页可按块挂起/放行后台备份读取;挂起期间同一块的整块写入照常提交;
- 进程重启后保留已落盘的块,并自动继续回填;断开标志同样持久化;
- 只要还有缺块就拒绝断开备份;补齐后允许断开,断开后数据盘纯本地读写。

慢读竞争语义(本文件重点):
- 缺块读可以带 delay,模拟备份端慢读;慢读期间业务整块写入照常提交,
  块一旦补齐即可断开备份,断开不需要等慢读返回;
- 备份读取在锁外进行(在途计数 _inflight),断开只置标志位;镜像句柄
  延后到最后一个在途备份读结束后才关闭;
- 在途备份读结束后一律复查本地:本地已补齐就返回本地内容(即使备份读
  成功也不返回旧备份数据,即使备份读失败也一样走本地复查);
- 每次重置/停止卷都递增 epoch(请求世代):跨越重置的旧读一律作废,
  接口提示重试——哪怕同号块已被重新写入;
- 断开之后的新读只访问本地,缺块直接 410,不再打开备份镜像。
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time

from flask import Flask, jsonify, render_template, request

from backup_util import make_image as make_backup_image

log = logging.getLogger("recovery")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks(
  idx        INTEGER PRIMARY KEY,
  data       BLOB NOT NULL,
  source     TEXT NOT NULL,          -- backfill | business
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS suspended(
  idx          INTEGER PRIMARY KEY,
  suspended_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS meta(
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

# 单次读透允许模拟的最大慢读秒数(演示保护,避免误操作挂死请求)
MAX_READ_DELAY = 120.0


class Volume:
    """本地块存储(SQLite)+ 只读备份镜像 + 后台回填 worker。"""

    def __init__(self, data_dir, backup_path, block_size, block_count, backfill_delay):
        self.data_dir = data_dir
        self.backup_path = backup_path
        self.block_size = block_size
        self.block_count = block_count
        self.backfill_delay = backfill_delay
        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.RLock()
        # 在途备份读的计数/通知复用同一把锁
        self._inflight_cond = threading.Condition(self.lock)
        self.db = sqlite3.connect(os.path.join(data_dir, "volume.db"), check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        with self.lock:
            self.db.executescript(SCHEMA)
            self.db.commit()
        self._backup_fd = None
        self._fd_epoch = None     # 当前 fd 打开时的请求世代
        self._fd_users = {}       # fd -> 正在使用它的在途备份读数
        self._retired_fds = set()  # 已退休(世代变更/待关闭)、等最后一个读者收尾的 fd
        self._inflight = 0          # 正在 pread(含模拟慢读等待)中的备份读总数
        self._worker = None
        self._stop = threading.Event()
        # generation:回填 worker 世代,reset/stop 时 +1,旧 worker 自行退出
        self.generation = 0
        # epoch:请求世代,reset/stop 时 +1;跨越重置的在途读回来后据此作废
        self.epoch = 0
        self.stats = {
            "backup_reads": 0,
            "backfilled": 0,
            "business_writes": 0,
            "late_ignored": 0,
            "local_after_wait": 0,   # 备份读结束后改从本地取块的次数
            "stale_rejected": 0,     # 跨越卷重置、被作废并要求重试的旧读次数
        }

    # ---------- 元数据 ----------
    def meta_get(self, key, default="0"):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def meta_set(self, key, value):
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.db.commit()

    # ---------- 备份镜像(只读) ----------
    def _close_fd_locked(self, fd, reason):
        """关闭并清理一个镜像 fd(调用方持有 _inflight_cond)。"""
        os.close(fd)
        self._fd_users.pop(fd, None)
        self._retired_fds.discard(fd)
        if self._backup_fd == fd:
            self._backup_fd = None
            self._fd_epoch = None
        log.info("镜像句柄已释放:%s (fd=%d)", reason, fd)

    def read_backup(self, idx, delay=0.0):
        """从只读备份镜像读一个块(os.pread 线程安全)。

        真正的读在锁外进行;期间用 _inflight/_fd_users 跟踪在途读取。
        delay>0 时在 pread 前睡指定秒数,模拟备份端慢读——这段时间
        业务写入、断开与重置都不需要等待本调用。
        """
        with self._inflight_cond:
            if self.meta_get("backup_disconnected") == "1":
                raise RuntimeError("backup disconnected")
            if self._backup_fd is None:
                self._backup_fd = os.open(self.backup_path, os.O_RDONLY)
                self._fd_epoch = self.epoch
                self._fd_users[self._backup_fd] = 0
            fd = self._backup_fd
            self._fd_users[fd] += 1
            self._inflight += 1
            self.stats["backup_reads"] += 1
        try:
            if delay:
                time.sleep(delay)
            data = os.pread(fd, self.block_size, idx * self.block_size)
            if len(data) != self.block_size:
                raise RuntimeError(f"short read from backup at block {idx}")
            return data
        finally:
            # 锁外结束:交出引用。fd 只有在"退休且无读者"时才真正 close,
            # 因此世代变更后新旧读各用各的 fd,绝不会误关对方的句柄。
            with self._inflight_cond:
                self._inflight -= 1
                self._fd_users[fd] -= 1
                retired = fd in self._retired_fds
                if retired and self._fd_users.get(fd, 0) == 0:
                    self._close_fd_locked(fd, "在途读结束,退休句柄回收")
                elif (
                    self.meta_get("backup_disconnected") == "1"
                    and fd == self._backup_fd
                    and self._fd_users.get(fd, 0) == 0
                ):
                    self._close_fd_locked(fd, "备份已断开,在途读全部结束")
                self._inflight_cond.notify_all()

    def _retire_current_fd_locked(self):
        """把当前 fd 标记退休(断开/世代变更时调用,持有锁)。

        仍有在途读者则保留到最后一个读者收尾;无读者则当场关闭。
        """
        fd = self._backup_fd
        if fd is None:
            return
        self._retired_fds.add(fd)
        self._backup_fd = None
        self._fd_epoch = None
        if self._fd_users.get(fd, 0) == 0:
            self._close_fd_locked(fd, "无在途读,立即回收")

    # ---------- 挂起 / 放行 ----------
    def is_suspended_locked(self, idx):
        return self.db.execute("SELECT 1 FROM suspended WHERE idx=?", (idx,)).fetchone() is not None

    def suspend(self, idx):
        with self.lock:
            self.db.execute(
                "INSERT OR IGNORE INTO suspended(idx, suspended_at) VALUES(?,?)",
                (idx, time.time()),
            )
            self.db.commit()

    def resume(self, idx):
        with self.lock:
            self.db.execute("DELETE FROM suspended WHERE idx=?", (idx,))
            self.db.commit()

    def suspended_list(self):
        with self.lock:
            return [r[0] for r in self.db.execute("SELECT idx FROM suspended ORDER BY idx")]

    # ---------- 块状态 ----------
    def present_map(self):
        with self.lock:
            return {r[0]: r[1] for r in self.db.execute("SELECT idx, source FROM blocks")}

    def missing_count_locked(self):
        (present,) = self.db.execute("SELECT COUNT(*) FROM blocks").fetchone()
        return self.block_count - present

    def next_missing_unsuspended_locked(self):
        present = {r[0] for r in self.db.execute("SELECT idx FROM blocks")}
        susp = {r[0] for r in self.db.execute("SELECT idx FROM suspended")}
        for i in range(self.block_count):
            if i not in present and i not in susp:
                return i
        return None

    def _local_row_locked(self, idx):
        return self.db.execute(
            "SELECT data, source FROM blocks WHERE idx=?", (idx,)
        ).fetchone()

    # ---------- 业务读写 ----------
    def read_block(self, idx, delay=0.0):
        """读块,返回 (status, data, origin)。

        status:
          local     本地命中(含"慢读期间被补齐、备份读回来后改走本地")
          backup    本地缺块且备份读透成功
          suspended 缺块且该块后台备份读被挂起(409)
          gone      备份已断开且本地仍缺(410)
          stale     本次读等待期间卷被重置,结果作废,请重试(409)
          error     备份读失败且本地无块可取(502)
        """
        with self.lock:
            epoch = self.epoch
            row = self._local_row_locked(idx)
            if row:
                return "local", row[0], row[1]
            if self.meta_get("backup_disconnected") == "1":
                return "gone", None, None
            if self.is_suspended_locked(idx):
                return "suspended", None, None
        # 本地缺块:从备份读透(可模拟慢读),不落盘(由后台回填负责补齐)。
        # 读在锁外进行,期间写入/断开/重置都可以并发发生。
        try:
            data = self.read_backup(idx, delay=delay)
        except Exception as exc:  # 备份读失败:仍给本地一次机会
            with self.lock:
                if self.epoch != epoch:
                    self.stats["stale_rejected"] += 1
                    return "stale", None, None
                row = self._local_row_locked(idx)
                if row:
                    self.stats["local_after_wait"] += 1
                    return "local", row[0], row[1]
                if self.meta_get("backup_disconnected") == "1":
                    return "gone", None, None
                log.warning("备份读失败且本地无块 (block=%d): %s", idx, exc)
                return "error", None, str(exc)
        # 备份读成功返回:先确认这还是同一个卷世代,再复查本地——
        # 等待期间补齐的块绝不返回备份旧内容。
        with self.lock:
            if self.epoch != epoch:
                self.stats["stale_rejected"] += 1
                log.info("块 %d 的备份读跨越了卷重置,旧请求作废,提示重试", idx)
                return "stale", None, None
            row = self._local_row_locked(idx)
            if row:
                self.stats["local_after_wait"] += 1
                log.info("块 %d 已在备份读等待期间补齐,改从本地返回", idx)
                return "local", row[0], row[1]
            if self.meta_get("backup_disconnected") == "1":
                # 等待期间断开且仍无本地块
                return "gone", None, None
        return "backup", data, None

    def write_block(self, idx, data):
        """整块写入(必须恰好 block_size 字节),业务写入永远生效。"""
        assert len(data) == self.block_size
        with self.lock:
            self.db.execute(
                "INSERT INTO blocks(idx,data,source,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(idx) DO UPDATE SET data=excluded.data, "
                "source=excluded.source, updated_at=excluded.updated_at",
                (idx, data, "business", time.time()),
            )
            self.db.commit()
            self.stats["business_writes"] += 1

    # ---------- 后台回填 ----------
    def _backfill_loop(self, generation):
        log.info("回填 worker 启动 (gen=%d)", generation)
        while not self._stop.is_set():
            with self.lock:
                if generation != self.generation:
                    log.info("回填 worker 退出:世代已变更")
                    return
                if self.meta_get("backfill_running") != "1":
                    log.info("回填 worker 退出:已停止")
                    return
                if self.meta_get("backup_disconnected") == "1":
                    log.info("回填 worker 退出:备份已断开")
                    return
                idx = self.next_missing_unsuspended_locked()
                done = self.missing_count_locked() == 0
            if done:
                with self.lock:
                    self.meta_set("backfill_running", "0")
                log.info("回填完成:无缺块")
                return
            if idx is None:
                time.sleep(0.2)  # 剩余缺块都被挂起,等待放行
                continue
            try:
                data = self.read_backup(idx)
            except Exception:
                log.exception("备份读取失败,稍后重试 (block=%d)", idx)
                time.sleep(1.0)
                continue
            # 模拟备份端拷贝耗时,也让进度在页面上可见;
            # 这个窗口内若业务写入抢先落盘,下面的 INSERT OR IGNORE 会丢弃本份迟到数据。
            time.sleep(self.backfill_delay)
            if self._stop.is_set():
                return
            with self.lock:
                if generation != self.generation:
                    return
                if self.is_suspended_locked(idx):
                    # 取数后被挂起:丢弃本次备份数据,等放行后再取
                    self.stats["late_ignored"] += 1
                    continue
                cur = self.db.execute(
                    "INSERT OR IGNORE INTO blocks(idx,data,source,updated_at) VALUES(?,?,?,?)",
                    (idx, data, "backfill", time.time()),
                )
                self.db.commit()
                if cur.rowcount:
                    self.stats["backfilled"] += 1
                else:
                    # 业务写入已抢先落盘:迟到的备份数据不能覆盖它
                    self.stats["late_ignored"] += 1

    def start_backfill(self):
        with self.lock:
            if self.meta_get("backup_disconnected") == "1":
                return False, "备份已断开,无法回填"
            if self.missing_count_locked() == 0:
                self.meta_set("backfill_running", "0")
                return True, "没有缺块,无需回填"
            if (
                self.meta_get("backfill_running") == "1"
                and self._worker is not None
                and self._worker.is_alive()
            ):
                return True, "回填已在进行中"
            self.meta_set("backfill_running", "1")
            self._stop.clear()
            self._worker = threading.Thread(
                target=self._backfill_loop, args=(self.generation,),
                daemon=True, name="backfill",
            )
            self._worker.start()
            return True, "后台回填已启动"

    def maybe_resume(self):
        """进程重启后:若上次回填未完成且备份未断开,自动继续。"""
        with self.lock:
            should = (
                self.meta_get("backfill_running") == "1"
                and self.meta_get("backup_disconnected") != "1"
                and self.missing_count_locked() > 0
            )
        if should:
            ok, msg = self.start_backfill()
            log.info("检测到未完成的回填,已自动继续: %s", msg)
            return True
        return False

    def _join_worker(self):
        worker, self._worker = self._worker, None
        if worker is not None and worker.is_alive():
            worker.join(timeout=3)

    def stop(self):
        """停止 worker(不清持久状态,模拟进程停止;重启后 maybe_resume 继续)。"""
        with self.lock:
            self.generation += 1
            self.epoch += 1  # 在途旧读回来后按 stale 处理,不得跨"进程"生效
            self._stop.set()
            self._retire_current_fd_locked()  # 旧世代 fd 不得被新读复用
        self._join_worker()
        with self.lock:
            self._stop.clear()

    def reset(self):
        """清空本地卷(演示用):已落盘块、挂起表、断开标记全部复位。

        递增请求世代:所有还在等备份的旧读回来后一律提示重试,
        即使同号块随后被重新写入;旧镜像 fd 退休,由在途读收尾关闭。
        """
        with self.lock:
            self.generation += 1
            self.epoch += 1
            self._stop.set()
            self.meta_set("backfill_running", "0")
            self._retire_current_fd_locked()
        self._join_worker()
        with self.lock:
            self.db.execute("DELETE FROM blocks")
            self.db.execute("DELETE FROM suspended")
            self.meta_set("backup_disconnected", "0")
            self.db.commit()
            for k in self.stats:
                self.stats[k] = 0
            self._stop.clear()

    # ---------- 断开备份 ----------
    def disconnect(self):
        """逻辑断开备份。

        - 有缺块一律拒绝(409 由 API 层映射);
        - 补齐后立即断开:只置持久标志,不等待任何在途慢读;
        - 没有在途读时当场关闭镜像句柄;有在途读时句柄延后到
          最后一个读结束(见 read_backup 的 finally);
        - 断开后的新读只访问本地,不会再打开镜像。
        """
        with self._inflight_cond:
            if self.meta_get("backup_disconnected") == "1":
                return False, "备份已断开,无需重复操作"
            missing = self.missing_count_locked()
            if missing:
                return False, f"还有 {missing} 个缺块未回填,拒绝断开备份"
            self.meta_set("backup_disconnected", "1")
            self.meta_set("backfill_running", "0")
            pending = self._inflight
            fd = self._backup_fd
            self._retire_current_fd_locked()
            if pending == 0 or fd is None:
                return True, "缺块已补齐,备份已断开;数据盘转为纯本地读写"
            return True, (
                f"缺块已补齐,备份已断开(无需等待 {pending} 个在途慢读);"
                "在途读结束后自动释放镜像句柄,新读只访问本地"
            )

    # ---------- 状态 ----------
    def status(self):
        with self.lock:
            (present,) = self.db.execute("SELECT COUNT(*) FROM blocks").fetchone()
            return {
                "block_size": self.block_size,
                "block_count": self.block_count,
                "present": present,
                "missing": self.block_count - present,
                "progress": round(present * 100.0 / self.block_count, 1),
                "backfill_running": self.meta_get("backfill_running") == "1",
                "backfill_complete": present == self.block_count,
                "backup_disconnected": self.meta_get("backup_disconnected") == "1",
                "inflight_backup_reads": self._inflight,
                # 任何镜像句柄仍打开即为 True(含已退休、等在途读收尾的旧 fd)
                "backup_fd_open": self._backup_fd is not None or bool(self._retired_fds),
                "epoch": self.epoch,
                "suspended": self.suspended_list(),
                "stats": dict(self.stats),
            }

    def close(self):
        self.stop()
        with self._inflight_cond:
            # 等待在途备份读结束(它们不拿 db 锁,只在收尾时进这把锁)
            self._inflight_cond.wait_for(lambda: self._inflight == 0, timeout=10)
            # 兜底:退休表中若还有残留(理论上 wait_for 后都已回收),强制关闭
            for fd in list(self._retired_fds):
                try:
                    os.close(fd)
                except OSError:
                    pass
                self._retired_fds.discard(fd)
            if self._backup_fd is not None:
                os.close(self._backup_fd)
                self._backup_fd = None
                self._fd_epoch = None
            self.db.close()


def _preview_text(data: bytes) -> str:
    return data.split(b"\x00")[0].decode("utf-8", "replace")[:96]


def create_app(config=None):
    cfg = {
        "BLOCK_SIZE": int(os.environ.get("BLOCK_SIZE", 4096)),
        "BLOCK_COUNT": int(os.environ.get("BLOCK_COUNT", 256)),
        "BACKFILL_DELAY": float(os.environ.get("BACKFILL_DELAY", 0.05)),
        "DATA_DIR": os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "data")),
        "BACKUP_IMAGE": os.environ.get(
            "BACKUP_IMAGE", os.path.join(os.path.dirname(__file__), "backup.img")
        ),
    }
    if config:
        cfg.update(config)

    if not os.path.exists(cfg["BACKUP_IMAGE"]):
        log.info("备份镜像不存在,生成确定性镜像: %s", cfg["BACKUP_IMAGE"])
        make_backup_image(cfg["BACKUP_IMAGE"], cfg["BLOCK_COUNT"], cfg["BLOCK_SIZE"])

    vol = Volume(
        cfg["DATA_DIR"], cfg["BACKUP_IMAGE"],
        cfg["BLOCK_SIZE"], cfg["BLOCK_COUNT"], cfg["BACKFILL_DELAY"],
    )
    resumed = vol.maybe_resume()

    app = Flask(__name__)
    app.vol = vol  # 便于测试关停

    def err(message, code):
        return jsonify(ok=False, message=message), code

    def check_idx(idx):
        return 0 <= idx < vol.block_count

    # ---------- 页面 ----------
    @app.get("/")
    def index():
        return render_template("index.html")

    # ---------- 状态 ----------
    @app.get("/api/status")
    def api_status():
        st = vol.status()
        st["resumed_on_boot"] = resumed
        return jsonify(st)

    @app.get("/api/blocks")
    def api_blocks():
        return jsonify(present=vol.present_map(), suspended=vol.suspended_list())

    # ---------- 块读写 ----------
    @app.get("/api/block/<int:idx>")
    def api_read(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        # ?delay=秒:模拟备份端慢读,仅对真正走到备份读透的缺块读生效
        delay = 0.0
        raw = request.args.get("delay")
        if raw is not None:
            try:
                delay = float(raw)
            except (TypeError, ValueError):
                return err("delay 必须是非负数字(秒)", 400)
            if delay < 0 or delay > MAX_READ_DELAY:
                return err(f"delay 需在 0~{MAX_READ_DELAY:.0f} 秒之间", 400)
        status, data, origin = vol.read_block(idx, delay=delay)
        if status == "local":
            return jsonify(ok=True, idx=idx, source="local", origin=origin,
                           text=_preview_text(data), size=len(data))
        if status == "backup":
            return jsonify(ok=True, idx=idx, source="backup",
                           text=_preview_text(data), size=len(data))
        if status == "suspended":
            return err(f"块 {idx} 本地缺失,且其后台备份读取已挂起", 409)
        if status == "stale":
            return err(
                f"块 {idx} 的读取在等待备份期间卷被重置,旧结果已作废,请重试(重新发起读取)",
                409,
            )
        if status == "gone":
            return err(f"块 {idx} 本地缺失,且备份已断开", 410)
        return err(f"块 {idx} 备份读取失败,且本地暂无此块: {data}", 502)

    @app.post("/api/block/<int:idx>/write")
    def api_write(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        body = request.get_json(silent=True) or {}
        text = body.get("data")
        if text is None:
            return err("缺少 data 字段", 400)
        raw = str(text).encode("utf-8")
        if len(raw) > vol.block_size:
            return err(f"数据 {len(raw)} 字节,超过整块大小 {vol.block_size} 字节", 400)
        vol.write_block(idx, raw.ljust(vol.block_size, b"\x00"))
        return jsonify(ok=True, message=f"块 {idx} 已整块写入本地({vol.block_size} 字节)")

    # ---------- 挂起 / 放行 ----------
    @app.post("/api/block/<int:idx>/suspend")
    def api_suspend(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        vol.suspend(idx)
        return jsonify(ok=True, message=f"块 {idx} 的后台备份读取已挂起")

    @app.post("/api/block/<int:idx>/resume")
    def api_resume(idx):
        if not check_idx(idx):
            return err("块号越界", 404)
        vol.resume(idx)
        return jsonify(ok=True, message=f"块 {idx} 的后台备份读取已放行")

    # ---------- 回填 / 断开 / 重置 ----------
    @app.post("/api/backfill/start")
    def api_backfill_start():
        ok, msg = vol.start_backfill()
        return jsonify(ok=ok, message=msg), (200 if ok else 409)

    @app.post("/api/disconnect")
    def api_disconnect():
        ok, msg = vol.disconnect()
        return jsonify(ok=ok, message=msg), (200 if ok else 409)

    @app.post("/api/reset")
    def api_reset():
        vol.reset()
        return jsonify(ok=True, message="本地卷已重置(全部缺块,备份恢复连接)")

    return app


if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("PORT", 5000))
    log.info("监听 0.0.0.0:%d", port)
    application.run(host="0.0.0.0", port=port, threaded=True)
