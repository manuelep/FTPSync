# -*- coding: utf-8 -*-

db.scheduler_task.truncate('RESTART IDENTITY CASCADE')

scheduler.queue_task("fetchall",
    timeout = 600,               #seconds
    prevent_drift = True,
    period = 900,                #seconds
    immediate = False,
    repeats = 0,                 # 0=unlimited
    retry_failed = -1,           # -1=unlimited
)

db.commit()