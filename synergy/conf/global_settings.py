settings = dict(
    mq_timeout_sec=300.0,
    mq_durable=True,
    mq_exclusive=False,
    mq_auto_delete=False,
    mq_delivery_mode=2,
    mq_no_ack=False,

    gc_run_interval=60,          # number of seconds between GarbageCollector runs
    gc_life_support_hours=48,    # number of hours from UOW creation time to keep UOW re-posting to MQ
    gc_resubmit_after_hours=1,   # number of hours, GC waits for the worker to pick up the UOW from MQ before re-posting
    gc_release_lag_minutes=15,   # number of minutes, GC keeps the UOW in the queue before posting it into MQ

    mx_host='0.0.0.0',           # management extension host (0.0.0.0 opens all interfaces)
    mx_port=5000,                # management extension port
    mx_children_limit=168,       # maximum number of children at any given level returned by MX
    perf_ticker_interval=60,     # seconds between performance ticker messages
)
