# -*- coding: utf-8 -*-

scheduler.queue_task("fetchall",
    timeout = 180,               #seconds
    prevent_drift = True,
    period = 900,                #seconds
    immediate = False,
    repeats = 0,                 # 0=unlimited
    retry_failed = -1,           # -1=unlimited
)