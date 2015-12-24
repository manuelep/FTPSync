# -*- coding: utf-8 -*-

from app.utils import prettydate

# prova qualcosa come
def index(): return dict(message="hello from scheduler.py")

def archive():

    def _next_update(row):
        period = datetime.timedelta(seconds=appconf[row.archname].period)
        d1 = row.last_update+period
        return prettydate(d1, use_suffix=True)
        
    db.archive.checksum.readable = False
    grid = SQLFORM.grid(
        db.archive,
        user_signature=False,
        links = [
            dict(header="time gap", body=lambda r: prettydate(r.last_update, use_suffix=False, now=r.modified_on)),
            dict(header="next update", body=_next_update),
        ],
        create=False, deletable=False, editable=True, csv=False,
    )
    return dict(grid=grid)

def task():

    db.scheduler_task.start_time.represent = lambda v,r: prettydate(v)
    db.scheduler_task.next_run_time.represent = lambda v,r: prettydate(v)

    grid = SQLFORM.smartgrid(
        db.scheduler_task,
        linked_tables = dict(scheduler_task=db.scheduler_run),
        fields = [
            db.scheduler_task.task_name,
            db.scheduler_task.status,
            db.scheduler_task.vars,
            db.scheduler_task.start_time,
            db.scheduler_task.next_run_time,
            db.scheduler_task.times_run,
            db.scheduler_task.times_failed,
            db.scheduler_run.status,
            db.scheduler_run.start_time,
            db.scheduler_run.stop_time,
            db.scheduler_run.run_output,
            db.scheduler_run.run_result,
            db.scheduler_run.traceback
        ],
        create=False, deletable=False, editable=True, csv=False, user_signature=False,
        paginate = 10,
        orderby = dict(scheduler_task=~db.scheduler_task.next_run_time)
    )

    return dict(grid=grid)