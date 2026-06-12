#!/usr/bin/env python3
"""
開機補跑腳本：確保電腦重開機後自動補齊缺漏資料。

流程：
1. 等待網路就緒（最多 120 秒）
2. 清理殘留的 scraper.lock
3. 確認 webapp 有在跑，沒有就重啟 launchd job
4. 檢查今天是否有 stock_state 快照，沒有就補跑
"""

import os
import sys
import time
import socket
import subprocess
import sqlite3
from datetime import datetime, date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOCK_FILE = os.path.join(LOG_DIR, 'scraper.lock')
DB_PATH = os.path.join(BASE_DIR, 'stocks.db')
LOG_FILE = os.path.join(LOG_DIR, 'startup_catchup.log')


def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass


def wait_for_network(timeout=120):
    """等待網路就緒，測試 DNS 解析"""
    start = time.time()
    hosts = ['openapi.twse.com.tw', 'www.google.com']
    while time.time() - start < timeout:
        for host in hosts:
            try:
                socket.setdefaulttimeout(5)
                socket.getaddrinfo(host, 443)
                log(f"網路就緒（{host} 可解析），等待 {time.time()-start:.0f} 秒")
                return True
            except (socket.gaierror, socket.timeout, OSError):
                pass
        time.sleep(5)
    log(f"等待網路超時（{timeout} 秒），放棄本次補跑")
    return False


def clean_stale_lock():
    """清理殘留的 lock 檔（重開機後舊 PID 不存在）"""
    if not os.path.exists(LOCK_FILE):
        return
    try:
        with open(LOCK_FILE, 'r') as f:
            content = f.read().strip()
        if content:
            pid = int(content.split()[0])
            # 檢查 PID 是否還活著
            try:
                os.kill(pid, 0)
                log(f"Lock 檔的 PID {pid} 仍在執行，不清理")
                return
            except ProcessLookupError:
                pass
            except PermissionError:
                log(f"Lock 檔的 PID {pid} 存在但無權限檢查，不清理")
                return
        os.remove(LOCK_FILE)
        log(f"已清理殘留的 scraper.lock（舊 PID: {content}）")
    except Exception as e:
        log(f"清理 lock 失敗: {e}")


def _wait_port_free(port, timeout=30):
    """等待 port 釋放"""
    start = time.time()
    while time.time() - start < timeout:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('0.0.0.0', port))
            s.close()
            return True
        except OSError:
            s.close()
            time.sleep(2)
    return False


def ensure_webapp():
    """確認 webapp (port 5000) 有在跑"""
    try:
        result = subprocess.run(
            ['lsof', '-i', ':5000', '-t'],
            capture_output=True, text=True, timeout=5
        )
        pids = result.stdout.strip().split('\n') if result.stdout.strip() else []

        # 檢查是否有 python 在用 5000
        for pid in pids:
            if not pid:
                continue
            ps = subprocess.run(
                ['ps', '-p', pid, '-o', 'comm='],
                capture_output=True, text=True, timeout=5
            )
            comm = ps.stdout.strip()
            if 'python' in comm.lower() or 'Python' in comm:
                log(f"webapp 正在執行（PID {pid}）")
                return

        # 等待 port 5000 釋放（AirPlay 可能佔著）
        if not _wait_port_free(5000, timeout=60):
            log("port 5000 持續被佔用，無法啟動 webapp")
            return

        # 沒有 python 在跑 5000，重置 launchd throttle 再啟動
        log("webapp 未執行，重置 launchd job...")
        uid = os.getuid()
        plist = os.path.expanduser('~/Library/LaunchAgents/com.stock.webapp.plist')
        # bootout 清除 throttle 記錄
        subprocess.run(['launchctl', 'bootout', f'gui/{uid}/com.stock.webapp'],
                      capture_output=True, timeout=10)
        time.sleep(2)
        # bootstrap 重新載入
        subprocess.run(['launchctl', 'bootstrap', f'gui/{uid}', plist],
                      capture_output=True, timeout=10)
        # kickstart 強制立即啟動（不等 throttle）
        subprocess.run(['launchctl', 'kickstart', f'gui/{uid}/com.stock.webapp'],
                      capture_output=True, timeout=10)
        log("webapp launchd job 已重置並啟動")
        time.sleep(8)

        # 確認是否啟動成功，失敗則 nohup 直接拉起
        result2 = subprocess.run(
            ['lsof', '-i', ':5000', '-t'],
            capture_output=True, text=True, timeout=5
        )
        if not result2.stdout.strip():
            log("launchd 啟動失敗（可能 throttle），改用 nohup 直接啟動...")
            log_dir = os.path.join(BASE_DIR, 'logs')
            subprocess.Popen(
                [sys.executable, '-u', os.path.join(BASE_DIR, 'app.py')],
                cwd=BASE_DIR,
                stdout=open(os.path.join(log_dir, 'webapp_stdout.log'), 'a'),
                stderr=open(os.path.join(log_dir, 'webapp_stderr.log'), 'a'),
                start_new_session=True
            )
            log("webapp 已用 nohup 啟動")
            time.sleep(5)
    except Exception as e:
        log(f"檢查 webapp 失敗: {e}")


def reload_all_schedules():
    """重載所有排程（bootout+bootstrap 清除 throttle），確保重啟後排程恢復"""
    uid = os.getuid()
    schedules = ['com.stock.quick', 'com.stock.scraper', 'com.stock.maintenance',
                 'com.stock.institutional', 'com.stock.backfill', 'com.stock.dbguard']
    for name in schedules:
        plist = os.path.expanduser(f'~/Library/LaunchAgents/{name}.plist')
        if not os.path.exists(plist):
            continue
        try:
            subprocess.run(['launchctl', 'bootout', f'gui/{uid}/{name}'],
                          capture_output=True, timeout=10)
            time.sleep(0.5)
            subprocess.run(['launchctl', 'bootstrap', f'gui/{uid}', plist],
                          capture_output=True, timeout=10)
        except Exception as e:
            log(f"重載 {name} 失敗: {e}")
    log(f"已重載 {len(schedules)} 個排程")


def needs_catchup():
    """檢查今天是否需要補跑"""
    today = date.today()
    # 週六日不需要（台股休市）
    if today.weekday() >= 5:
        log(f"今天是{'週六' if today.weekday() == 5 else '週日'}，不需要補跑")
        return False

    today_str = today.strftime('%Y-%m-%d')
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT COUNT(*) FROM stock_state WHERE date = ?",
            (today_str,)
        ).fetchone()
        conn.close()
        count = row[0] if row else 0
        if count > 0:
            log(f"今天({today_str})已有 {count} 筆快照，不需要補跑")
            return False
        else:
            log(f"今天({today_str})沒有快照資料，需要補跑")
            return True
    except Exception as e:
        log(f"檢查 DB 失敗: {e}，預設需要補跑")
        return True


def run_catchup():
    """執行補跑：先 quick_update 再 run_prices"""
    python = sys.executable
    now = datetime.now()

    # 判斷現在時間決定跑什麼
    # 盤後（13:35 之後）：跑 run_prices（含股價+評價+push）
    # 盤前或盤中：只跑 quick_update（MOPS 營收/季報）
    if now.hour >= 14 or (now.hour == 13 and now.minute >= 35):
        log("盤後時段，執行 run_prices + quick_update...")
        # 先跑 quick_update 更新營收
        log("步驟 1/2：quick_update（MOPS 營收/季報）...")
        r1 = subprocess.run(
            [python, '-u', os.path.join(BASE_DIR, 'scraper.py'), '--quick'],
            cwd=BASE_DIR, timeout=1800,
            capture_output=False
        )
        log(f"quick_update 完成，exit code: {r1.returncode}")

        # 再跑 run_prices 更新股價+評價
        log("步驟 2/2：run_prices（股價+評價+push）...")
        r2 = subprocess.run(
            [python, '-u', os.path.join(BASE_DIR, 'scraper.py'), '--prices'],
            cwd=BASE_DIR, timeout=600,
            capture_output=False
        )
        log(f"run_prices 完成，exit code: {r2.returncode}")
    else:
        log("盤前/盤中時段，只執行 quick_update...")
        r = subprocess.run(
            [python, '-u', os.path.join(BASE_DIR, 'scraper.py'), '--quick'],
            cwd=BASE_DIR, timeout=1800,
            capture_output=False
        )
        log(f"quick_update 完成，exit code: {r.returncode}")


def main():
    log("=" * 50)
    log("開機補跑腳本啟動")
    log("=" * 50)

    # 1. 等待網路
    if not wait_for_network():
        return

    # 2. 清理殘留 lock
    clean_stale_lock()

    # 3. 確認 webapp
    ensure_webapp()

    # 3.5 重載所有排程（清除 throttle，確保重啟後恢復）
    reload_all_schedules()

    # 4. 檢查是否需要補跑
    if needs_catchup():
        run_catchup()

    log("開機補跑腳本結束")


if __name__ == "__main__":
    main()
