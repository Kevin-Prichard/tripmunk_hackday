#!/usr/bin/env python2.7

from collections import defaultdict
from datetime import datetime
from math import log10, sqrt
from numpy import isfinite, mean, std
import msgpack
import os
import sys
import json


def alerts_ingest(pathname, overwrite=False):
    save_file = "alerts.mpk"
    if os.path.exists(save_file) or overwrite:
        with open(save_file, "r") as f:
            return msgpack.load(f)

    user_alerts = defaultdict(dict)
    with open(pathname,"r") as af:
      for L in af:
        ua = L.split(' ')
        ts = datetime.strptime("{} {} 2018 {}".format(*ua[:3]), "%b %d %Y %H:%M:%S")
        user_alerts[ua[3]][int(ts.strftime("%s"))] = ua[4].strip()
    with open(save_file, "w") as f:
        msgpack.dump(user_alerts, f)
    return user_alerts

def imports_ingest(pathname, overwrite=False):
    save_file = "imports.mpk"
    if os.path.exists(save_file) or overwrite:
        with open(save_file, "r") as f:
            return msgpack.load(f)

    user_imports = defaultdict(list)
    with open(pathname,"r") as ai:
      for L in ai:
        ub = L.split('\t')
        ts = datetime.strptime(ub[1].strip(), "%Y-%m-%d %H:%M:%S")
        uid = ub[0].strip()
        user_imports[uid].append(int(ts.strftime("%s")))

    with open(save_file, "w") as f:
        msgpack.dump(user_imports, f)
    return user_imports


def merge_imports_into_alerts(user_imports, user_alerts):
    for uid, times in user_imports.items():
        for ts in times:
            try:
                user_alerts[uid][ts] = "email"
            except KeyError as e:
                break


def get_bigram_stdevs(user_alerts):
    bigrams = defaultdict(list)
    for id, time_alerts in user_alerts.items():
        alerts = iter(sorted(time_alerts.keys()))
        last_ts = next(alerts)
        last_alert = time_alerts[last_ts]
        while True:
            try:
                ts = next(alerts)
            except StopIteration:
                break
            alert = time_alerts[ts]
            dur = ts - last_ts
            if dur < 86400*10:
                bigrams[(last_alert, alert)].append(dur)
            last_ts = ts
            last_alert = alert
    stdevs = dict()
    for bi, diffs in bigrams.items():
        stdevs[bi] = (std(diffs), mean(diffs))
    return stdevs


def gather_stories(user_alerts, user_imports, bigram_stdevs):
    break_thresh = 60 * 60 * 24 * 10
    all_bins = []

    for id, time_alerts in user_alerts.items():
        last_ts = None
        bin = []
        alerts = iter(sorted(time_alerts.keys()))
        last_ts = next(alerts)
        last_alert = time_alerts[last_ts]
        bigram = None
        while True:
            try:
                ts = next(alerts)
            except StopIteration:
                break
            alert = time_alerts[ts]
            bigram = (last_alert, alert)
            pair = bigram_stdevs.get(bigram, None)
            if pair is None or ts - last_ts > pair[1] + pair[0]*0.1:
                all_bins.append(bin)
                bin = []
            bin.append((alert, ts - last_ts))
            last_ts = ts
            last_alert = alert
            bigram = None

        if bin:
            all_bins.append(bin)

    return all_bins


def main():
    user_alerts = alerts_ingest("/Users/i870290/Downloads/alerts-oct-1-5.txt")
    user_imports = imports_ingest("/Users/i870290/Downloads/imports-oct-1-6.txt")
    merge_imports_into_alerts(user_imports, user_alerts)

    bigrams_stdev = get_bigram_stdevs(user_alerts)
    all_bins = gather_stories(user_alerts, user_imports, bigrams_stdev)

    bins = defaultdict(list)
    for bin in all_bins:
        bin_key = []
        time_sum = 0
        for alert_ts in bin:
            bin_key.append(alert_ts[0])
            time_sum += alert_ts[1]
        bins[tuple(bin_key)].append(time_sum)

    cnt = 0
    ttl = 0
    for bin_key, times in bins.items():
        ttl += len(times)
        cnt += 1

    avg_contribs = ttl / cnt
    princip = defaultdict(int)
    for bin_key, times in bins.items():
        if bin_key and len(times) > avg_contribs*1.5:
            print bin_key, round(mean(times) / 3600, 1), len(times)
            try:
                princip[bin_key[0]] += 1
            except:
                import pudb; pu.db
                x = 1

#     print json.dumps(princip, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
