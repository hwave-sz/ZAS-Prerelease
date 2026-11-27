#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""在一张卡上制造持续负载（模拟副本间容量/负载不对称）。

用法：python3 hog.py <port> <concurrency> <max_tokens> <duration_s>
用意：utility_v2 的价值主张是"避开忙的副本"。2 副本对称时无差异是必然的；
只有制造副本间真实差距，才能判定这个算法到底有没有用。
"""
import json
import sys
import threading
import time
import urllib.request

port = sys.argv[1] if len(sys.argv) > 1 else "8001"
conc = int(sys.argv[2]) if len(sys.argv) > 2 else 12
max_tok = int(sys.argv[3]) if len(sys.argv) > 3 else 512
dur = int(sys.argv[4]) if len(sys.argv) > 4 else 240

BASE = "http://127.0.0.1:%s" % port
MODEL = "qwen27b"
PROMPT = ("请逐条解释下面这段系统设计说明，并给出反例。" * 12)
stop = threading.Event()
n_done = [0]
lock = threading.Lock()


def one():
    body = json.dumps({"model": MODEL,
                       "messages": [{"role": "user", "content": PROMPT}],
                       "max_tokens": max_tok, "temperature": 0,
                       "stream": True}).encode()
    req = urllib.request.Request(BASE + "/v1/chat/completions", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=dur + 60) as r:
        while r.read(8192):
            pass


def worker():
    while not stop.is_set():
        try:
            one()
            with lock:
                n_done[0] += 1
        except Exception:
            time.sleep(0.5)


def main():
    ths = [threading.Thread(target=worker, daemon=True) for _ in range(conc)]
    for t in ths:
        t.start()
    t0 = time.time()
    while time.time() - t0 < dur:
        time.sleep(10)
        print("[hog %s] %.0fs done=%d" % (port, time.time() - t0, n_done[0]), flush=True)
    stop.set()
    print("[hog %s] finished, total=%d" % (port, n_done[0]), flush=True)


if __name__ == "__main__":
    main()
