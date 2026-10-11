-- Demo credentials only. Use your own login provisioning in production.
CREATE USER postgremq_metrics PASSWORD 'metrics';
GRANT USAGE ON SCHEMA postgremq TO postgremq_metrics;
GRANT SELECT ON postgremq.queues, postgremq.queue_messages, postgremq.dead_letter_queue TO postgremq_metrics;
GRANT EXECUTE ON FUNCTION postgremq.queue_metrics() TO postgremq_metrics;
SELECT postgremq.create_topic('demo');
SELECT postgremq.create_queue('demo', 'demo', 3, false);
SELECT postgremq.create_queue('empty', 'demo', 3, false);
