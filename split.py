#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""读 jsonl，打印每个臂的落点分布 + 关键分位（用于判定 utility_v2 是否真的steer）。"""
import json
import os
import sys

OUT = "/tmp/phaseD"


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[k]


def main(tags):
    print("%-8s %5s %5s %7s %8s %8s %8s %8s %9s" % (
        "arm", "n", "ok", "succ%", "ttft50", "ttft99", "tpot50", "tok/s", "inst_split"))
    print("-" * 92)
    for tag in tags:
        p = os.path.join(OUT, "%s.jsonl" % tag)
        if not os.path.isfile(p):
            print("%-8s (missing)" % tag)
            continue
        recs = [json.loads(l) for l in open(p)]
        ok = [r for r in recs if r.get("ok")]
        ttft = [r["first_ms"] for r in ok if r.get("first_ms")]
        tpot = [r["tpot_ms"] for r in ok if r.get("tpot_ms")]
        dur = max([r.get("elapsed_s", 0) for r in recs] or [1])
        toks = sum(r.get("tokens", 0) for r in ok)
        split = {}
        for r in ok:
            k = r.get("inst") or "-"
            k = k.replace("qwen27b-live", "L").replace("-live", "")
            split[k] = split.get(k, 0) + 1
        tot = sum(split.values()) or 1
        sp = " ".join("%s:%.0f%%" % (k, v * 100.0 / tot) for k, v in
                      sorted(split.items(), key=lambda x: -x[1]))
        print("%-8s %5d %5d %7.2f %8s %8s %8s %8s %9s" % (
            tag, len(recs), len(ok), len(ok) * 100.0 / max(1, len(recs)),
            pct(ttft, .5), pct(ttft, .99), pct(tpot, .5),
            round(toks / 90.0, 1), sp))


if __name__ == "__main__":
    main(sys.argv[1:] or ["B24", "C24", "B24b", "B24h", "C24h"])
