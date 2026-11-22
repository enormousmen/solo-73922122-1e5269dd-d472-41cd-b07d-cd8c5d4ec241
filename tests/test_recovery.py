"""单卷恢复服务关键操作自测。

运行: python3 -m pytest tests/ -v
"""
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from app import create_app  # noqa: E402

CFG = dict(BLOCK_SIZE=4096, BLOCK_COUNT=32, BACKFILL_DELAY=0.002)
SLOW = 2.0  # 慢读秒数,留出充足的竞争窗口


def wait_for(pred, timeout=15.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.05)
    return False


def status(c):
    return c.get("/api/status").get_json()


@pytest.fixture()
def client(tmp_path):
    cfg = dict(CFG, DATA_DIR=str(tmp_path / "data"), BACKUP_IMAGE=str(tmp_path / "backup.img"))
    app = create_app(cfg)
    app.testing = True
    with app.test_client() as c:
        yield c
    app.vol.close()


def test_read_through_backup_and_not_persisted(client):
    """本地缺块 → 从备份读透;读透不落盘(由后台回填负责补齐)。"""
    r = client.get("/api/block/5")
    assert r.status_code == 200
    j = r.get_json()
    assert j["source"] == "backup"
    assert j["text"].startswith("BACKUP-BLOCK-000005|")
    assert j["size"] == 4096
    st = status(client)
    assert st["missing"] == 32, "读透不应落盘"
    assert st["stats"]["backup_reads"] == 1


def test_backfill_completes_and_reads_become_local(client):
    """启动回填 → 全部补齐 → 读取变为本地命中。"""
    r = client.post("/api/backfill/start")
    assert r.status_code == 200
    assert wait_for(lambda: status(client)["missing"] == 0)
    st = status(client)
    assert st["backfill_complete"] and not st["backfill_running"]
    assert st["stats"]["backfilled"] == 32
    j = client.get("/api/block/5").get_json()
    assert j["source"] == "local" and j["origin"] == "backfill"
    assert j["text"].startswith("BACKUP-BLOCK-000005|")


def test_suspend_write_resume_and_late_backup_never_overwrites(client):
    """挂起期间:读被拒、整块写入可提交;放行补齐后业务数据不被覆盖。"""
    assert client.post("/api/block/7/suspend").status_code == 200
    client.post("/api/backfill/start")
    # 其余块都补齐,只剩被挂起的 7
    assert wait_for(lambda: status(client)["missing"] == 1)
    assert 7 in status(client)["suspended"]

    # 挂起期间:缺块读取被拒(不读备份)
    r = client.get("/api/block/7")
    assert r.status_code == 409

    # 挂起期间:同一块的整块写入仍可提交
    r = client.post("/api/block/7/write", json={"data": "BUSINESS-DATA-7"})
    assert r.status_code == 200
    j = client.get("/api/block/7").get_json()
    assert j["source"] == "local" and j["origin"] == "business"
    assert j["text"] == "BUSINESS-DATA-7"

    # 还有缺块时拒绝断开备份(此时其实已补齐 7 以外的块,先验证拒绝逻辑见下)
    client.post("/api/block/7/resume")
    assert wait_for(lambda: status(client)["missing"] == 0)

    # 迟到的备份数据不能覆盖业务写入
    j = client.get("/api/block/7").get_json()
    assert j["origin"] == "business" and j["text"] == "BUSINESS-DATA-7"


def test_disconnect_refused_until_full_then_detached_volume_rw(client):
    """缺块时拒绝断开;补齐后可断开;断开后数据盘仍可读写。"""
    client.post("/api/backfill/start")
    r = client.post("/api/disconnect")
    assert r.status_code == 409
    assert "拒绝" in r.get_json()["message"]

    assert wait_for(lambda: status(client)["missing"] == 0)
    r = client.post("/api/disconnect")
    assert r.status_code == 200
    assert status(client)["backup_disconnected"]

    # 断开后:读本地、写本地都正常
    j = client.get("/api/block/3").get_json()
    assert j["source"] == "local"
    assert client.post("/api/block/3/write", json={"data": "AFTER-DISCONNECT"}).status_code == 200
    j = client.get("/api/block/3").get_json()
    assert j["text"] == "AFTER-DISCONNECT"

    # 断开后不能再回填、不能重复断开
    assert client.post("/api/backfill/start").status_code == 409
    assert client.post("/api/disconnect").status_code == 409


def test_persistence_across_restart(tmp_path):
    """服务重启后保留已落盘的块,并自动继续回填。"""
    cfg = dict(CFG, DATA_DIR=str(tmp_path / "data"), BACKUP_IMAGE=str(tmp_path / "backup.img"))

    app1 = create_app(cfg)
    c1 = app1.test_client()
    c1.post("/api/block/3/write", json={"data": "PERSIST-ME"})
    c1.post("/api/backfill/start")
    assert wait_for(lambda: c1.get("/api/status").get_json()["present"] >= 8)
    app1.vol.stop()  # 模拟进程停止(保留持久状态)

    app2 = create_app(cfg)
    c2 = app2.test_client()
    try:
        j = c2.get("/api/block/3").get_json()
        assert j["origin"] == "business" and j["text"] == "PERSIST-ME", "已落盘的块应保留"
        st = c2.get("/api/status").get_json()
        assert st["backfill_running"], "重启后应自动继续回填"
        assert wait_for(lambda: c2.get("/api/status").get_json()["missing"] == 0)
    finally:
        app2.vol.close()


def test_write_validation(client):
    """整块写入校验:超 4 KiB 拒绝;块号越界 404。"""
    r = client.post("/api/block/0/write", json={"data": "x" * 5000})
    assert r.status_code == 400
    r = client.post("/api/block/0/write", json={})
    assert r.status_code == 400
    assert client.post("/api/block/999/write", json={"data": "x"}).status_code == 404
    assert client.get("/api/block/999").status_code == 404
    # 恰好 4096 字节可以
    r = client.post("/api/block/0/write", json={"data": "y" * 4096})
    assert r.status_code == 200


def test_concurrent_business_writes_survive_backfill(client):
    """回填进行中并发业务写入:所有业务数据都不被回填覆盖。"""
    client.post("/api/backfill/start")
    written = {}
    for i in range(0, 32, 3):
        assert client.post(f"/api/block/{i}/write", json={"data": f"BIZ-{i}"}).status_code == 200
        written[i] = f"BIZ-{i}"
    assert wait_for(lambda: status(client)["missing"] == 0)
    for i, text in written.items():
        j = client.get(f"/api/block/{i}").get_json()
        assert j["origin"] == "business" and j["text"] == text, f"块 {i} 被回填覆盖"


# ---------- 慢读竞争:补齐 / 断开 / 重置 ----------
# 注:Flask test client 的 app context 不能跨线程,以下并发用例直接调用
# 存储层 Volume 方法;HTTP 状态码映射由 test_slow_read_*_api 等用例覆盖。


def test_slow_read_returns_local_after_block_filled(client):
    """缺块慢读期间块被整块写入补齐:慢读结束后返回本地新数据,不返回备份旧内容。"""
    vol = client.application.vol
    result = {}

    def slow_read():
        result["status"] = vol.read_block(7, delay=SLOW)

    t = threading.Thread(target=slow_read)
    t.start()
    # 等慢读真正进入在途状态
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)
    assert vol.status()["backup_fd_open"] is True

    # 慢读期间整块写入补齐
    vol.write_block(7, b"FRESH-DATA-7".ljust(vol.block_size, b"\x00"))
    t.join(timeout=SLOW + 5)
    assert not t.is_alive()

    status_name, data, origin = result["status"]
    assert status_name == "local" and origin == "business"
    assert data.startswith(b"FRESH-DATA-7"), "等待期间已补齐,不得返回备份里的旧内容"
    st = vol.status()
    assert st["stats"]["local_after_wait"] == 1
    assert st["inflight_backup_reads"] == 0


def test_disconnect_during_slow_read_does_not_block_and_fd_released(client):
    """慢读在途时补齐后可立即断开;句柄等在途读结束才释放;新读只访问本地。"""
    vol = client.application.vol
    client.post("/api/backfill/start")
    result = {}

    def slow_read():
        result["status"] = vol.read_block(3, delay=SLOW)

    t = threading.Thread(target=slow_read)
    t.start()
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)

    # 回填到全补齐(慢读块 3 也由回填补齐,业务不写它)
    assert wait_for(lambda: vol.status()["missing"] == 0)
    # 慢读仍在途时断开:立即成功,不等待
    ok, _msg = vol.disconnect()
    assert ok is True
    st = vol.status()
    assert st["backup_disconnected"]
    assert st["inflight_backup_reads"] == 1, "在途慢读应仍在计数中"
    assert st["backup_fd_open"] is True, "在途读未结束,句柄不能提前关闭"

    # 断开后的新读只访问本地(不触发备份读)
    reads_before = vol.status()["stats"]["backup_reads"]
    status_name, data, origin = vol.read_block(3)
    assert status_name == "local"
    assert vol.status()["stats"]["backup_reads"] == reads_before

    t.join(timeout=SLOW + 5)
    assert not t.is_alive()
    # 旧慢读回来:块已在本地,改从本地返回(不返回备份数据)
    status_name, _data, origin = result["status"]
    assert status_name == "local" and origin == "backfill"
    st = vol.status()
    assert st["inflight_backup_reads"] == 0
    assert st["backup_fd_open"] is False, "在途读全部结束后应释放镜像句柄"
    assert st["stats"]["local_after_wait"] >= 1


def test_slow_read_failure_still_falls_back_to_local(client, monkeypatch):
    """备份读失败也不影响:等待期间已补齐则从本地取块。"""
    import app as app_module
    vol = client.application.vol
    original_pread = app_module.os.pread
    call_count = {"n": 0}

    def flaky_pread(fd, size, offset):
        # 第二次 pread(慢读那次;第一次可能由其它读触发)抛错,
        # 走真实 read_backup 的在途计数/延迟/finally 收尾路径
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise OSError("simulated backup read failure")
        return original_pread(fd, size, offset)

    monkeypatch.setattr(app_module.os, "pread", flaky_pread)
    result = {}

    def slow_read():
        result["status"] = vol.read_block(5, delay=SLOW)

    t = threading.Thread(target=slow_read)
    t.start()
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)
    # 等待期间业务写入补齐
    vol.write_block(5, b"LOCAL-5".ljust(vol.block_size, b"\x00"))
    t.join(timeout=SLOW + 5)
    assert not t.is_alive()
    status_name, data, _origin = result["status"]
    assert status_name == "local"
    assert data.startswith(b"LOCAL-5"), "备份读失败也要能改从本地取块"
    assert vol.status()["stats"]["local_after_wait"] == 1


def test_reset_during_slow_read_makes_old_request_stale_even_if_rewritten(client):
    """等待期间重置卷:旧读一律提示重试,哪怕同号块随后被重新写入。"""
    vol = client.application.vol
    result = {}

    def slow_read():
        result["status"] = vol.read_block(9, delay=SLOW)

    t = threading.Thread(target=slow_read)
    t.start()
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)

    # 重置卷,并立刻重新写入同号块
    vol.reset()
    vol.write_block(9, b"AFTER-RESET-9".ljust(vol.block_size, b"\x00"))

    t.join(timeout=SLOW + 5)
    assert not t.is_alive()
    status_name, _data, _origin = result["status"]
    assert status_name == "stale"
    assert vol.status()["stats"]["stale_rejected"] == 1

    # 新请求不受影响:读到重置后写入的内容
    status_name, data, origin = vol.read_block(9)
    assert status_name == "local" and origin == "business"
    assert data.startswith(b"AFTER-RESET-9")

    # HTTP 层:stale 映射为 409 且消息提示重试
    r = client.get("/api/block/9")  # 已是本地,先确认正常读
    assert r.status_code == 200


def test_stale_and_gone_and_error_http_mapping(client):
    """API 状态码映射:stale→409 重试提示;gone→410;error→502。"""
    vol = client.application.vol

    # gone:断开状态下造一个本地缺块
    client.post("/api/backfill/start")
    assert wait_for(lambda: status(client)["missing"] == 0)
    assert client.post("/api/disconnect").status_code == 200
    vol.db.execute("DELETE FROM blocks WHERE idx=1")
    vol.db.commit()
    r = client.get("/api/block/1")
    assert r.status_code == 410

    # error:备份读抛错且本地无块、未断开
    client.post("/api/reset")
    original = vol.read_backup

    def failing_read(idx, delay=0.0):
        raise OSError("boom")

    vol.read_backup = failing_read
    try:
        r = client.get("/api/block/2")
        assert r.status_code == 502
        assert "备份读取失败" in r.get_json()["message"]
    finally:
        vol.read_backup = original

    # stale→409:存储层产出 stale 后,真实路由必须映射为 409 且提示重试。
    # 先在存储层制造一次 stale(慢读等待期间重置,同号块随后重写)。
    result2 = {}
    t2 = threading.Thread(target=lambda: result2.update(r=vol.read_block(6, delay=SLOW)))
    t2.start()
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)
    vol.reset()
    vol.write_block(6, b"X".ljust(vol.block_size, b"\x00"))
    t2.join(timeout=SLOW + 5)
    assert result2["r"][0] == "stale"

    # 用 monkeypatch 让下一次 HTTP 读返回 stale,验证 api_read 的状态码/文案映射
    def stale_read(idx, delay=0.0):
        return ("stale", None, None)

    vol.read_block = stale_read
    try:
        r = client.get("/api/block/6")
        assert r.status_code == 409
        assert "重置" in r.get_json()["message"] and "重试" in r.get_json()["message"]
    finally:
        # 恢复真正的 read_block(绑定方法还原)
        del vol.read_block
    # 新读正常拿到重置后的数据
    j = client.get("/api/block/6").get_json()
    assert j["source"] == "local"


def test_disconnected_state_persists_across_restart(tmp_path):
    """断开标志持久化:重启后仍是断开状态,缺块读返回 410,不会自动续回填。"""
    cfg = dict(CFG, DATA_DIR=str(tmp_path / "data"), BACKUP_IMAGE=str(tmp_path / "backup.img"))

    app1 = create_app(cfg)
    c1 = app1.test_client()
    c1.post("/api/backfill/start")
    assert wait_for(lambda: c1.get("/api/status").get_json()["missing"] == 0)
    assert c1.post("/api/disconnect").status_code == 200
    app1.vol.stop()

    app2 = create_app(cfg)
    c2 = app2.test_client()
    try:
        st = c2.get("/api/status").get_json()
        assert st["backup_disconnected"] is True, "重启后应保留已断开状态"
        assert st["backfill_running"] is False
        # 手工造一个缺块场景:断开状态下本地缺失必须 410,且不打开备份
        app2.vol.db.execute("DELETE FROM blocks WHERE idx=1")
        app2.vol.db.commit()
        r = c2.get("/api/block/1")
        assert r.status_code == 410
        assert c2.post("/api/backfill/start").status_code == 409
        assert app2.vol._backup_fd is None
    finally:
        app2.vol.close()


def test_reset_while_inflight_after_disconnect_releases_old_fd(client):
    """断开后仍有在途慢读时重置:旧 fd 随在途读收尾释放,新读打开新 fd,无泄漏。"""
    vol = client.application.vol
    client.post("/api/backfill/start")
    result = {}

    def slow_read():
        result["status"] = vol.read_block(2, delay=SLOW)

    t = threading.Thread(target=slow_read)
    t.start()
    assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)
    assert wait_for(lambda: vol.status()["missing"] == 0)
    old_fd = vol._backup_fd
    assert old_fd is not None
    ok, _ = vol.disconnect()
    assert ok
    assert vol.status()["inflight_backup_reads"] == 1
    assert old_fd in vol._retired_fds

    # 在途读未结束时重置(断开标志清除、世代递增);fd 此前已随断开退休
    vol.reset()
    assert vol.status()["backup_disconnected"] is False
    assert vol._backup_fd is None
    assert old_fd in vol._retired_fds
    assert vol.status()["backup_fd_open"] is True

    t.join(timeout=SLOW + 5)
    assert not t.is_alive()
    assert result["status"][0] == "stale"
    # 旧世代 fd 必须已被收尾逻辑关闭,无残留
    assert vol._backup_fd is None
    assert old_fd not in vol._retired_fds
    assert vol.status()["backup_fd_open"] is False

    # 重置后的新读正常打开新 fd 并读透备份(fd 号可能被 OS 复用,
    # 关键是退休表已清空、无残留句柄)
    assert vol._retired_fds == set()
    status_name, data, _origin = vol.read_block(2)
    assert status_name == "backup"
    assert data.startswith(b"BACKUP-BLOCK-000002|")
    assert vol._backup_fd is not None
    # 读已收尾:该 fd 引用归零、在途归零,但保持打开供后续读复用
    assert vol._fd_users.get(vol._backup_fd, 0) == 0
    assert vol.status()["inflight_backup_reads"] == 0


def test_no_fd_leak_across_many_disconnect_reset_cycles(client):
    """多轮 慢读→断开→重置 循环后,进程打开的备份镜像 fd 数量不增长。"""
    vol = client.application.vol

    def open_backup_fds():
        names = []
        for fd in os.listdir(f"/proc/{os.getpid()}/fd"):
            try:
                target = os.readlink(f"/proc/{os.getpid()}/fd/{fd}")
            except OSError:
                continue
            if target.endswith("backup.img"):
                names.append(target)
        return names

    assert open_backup_fds() == []  # 尚未读过备份
    for cycle in range(5):
        holder = {}

        def slow_read():
            holder["s"] = vol.read_block(cycle % vol.block_count, delay=0.6)

        t = threading.Thread(target=slow_read)
        t.start()
        assert wait_for(lambda: vol.status()["inflight_backup_reads"] == 1)
        # 用业务写入快速补齐全部块,再断开
        for i in range(vol.block_count):
            vol.write_block(i, bytes([i % 256]) * vol.block_size)
        ok, _ = vol.disconnect()
        assert ok
        t.join(timeout=5)
        assert not t.is_alive()
        vol.reset()  # 世代变更,退休 fd 必须被收尾
    # 所有读都已收尾:退休表为空,进程内不存在任何备份镜像 fd
    assert vol._retired_fds == set()
    assert open_backup_fds() == [], "存在泄漏的备份镜像 fd"


def test_slow_read_delay_validation(client):
    """delay 参数校验:负数/非数字 400;范围上限保护。"""
    assert client.get("/api/block/0?delay=-1").status_code == 400
    assert client.get("/api/block/0?delay=abc").status_code == 400
    assert client.get("/api/block/0?delay=9999").status_code == 400
    # 合法 delay 但块已本地存在时立即返回,不真的睡
    client.post("/api/block/0/write", json={"data": "x"})
    t0 = time.time()
    assert client.get("/api/block/0?delay=10").status_code == 200
    assert time.time() - t0 < 2
