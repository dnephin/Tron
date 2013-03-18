"""
Resume batches that were left in UNKNOWN state after trond has restarted.
"""
import logging
import json
import optparse
import subprocess

from tron.commands import client
from tron.core.actionrun import ActionRun
from tron.core.job import Job


log = logging.getLogger(__name__)
logging.basicConfig()
log.setLevel(logging.INFO)


def write_status(jobs, filename):
    with open(filename, 'w') as fh:
        json.dump(jobs, fh, indent=4)


def read_status(filename):
    with open(filename, 'r') as fh:
        return json.load(fh)


def get_jobs_by_status(opts, status):
    jobs = client.Client(opts).jobs()
    log.info("Found %s jobs" % len(jobs))
    jobs = filter(lambda job: job['status'] == status, jobs)
    log.info("Found %s jobs with status %s", len(jobs), status)
    return jobs


def create_status(opts):
    running_jobs = get_jobs_by_status(opts, Job.STATUS_RUNNING)
    write_status(running_jobs, opts.filename)


def call(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return proc.returncode, stdout, stderr


def find_process(action_run):
    log.info("Looking for '%s' on %s", action_run['command'], action_run['node'])
    _, stdout, _ = call(['ssh', action_run['node'], 'pgrep -x -f "%s"' % action_run['command']])
    pids = stdout.strip()
    if pids:
        log.info('%s still running as %s', action_run['id'], pids)
        return True


def resume_job_run(opts, action_run_id):
    if not opts.resume:
        print "\ntronctl fail %s; tronctl skip %s\n" % (action_run_id, action_run_id)
        return

    api_client = client.Client(opts)
    url = api_client.get_url_from_identifier(action_run_id)
    client.request(opts.server, url, {'command': 'fail'})
    client.request(opts.server, url, {'command': 'skip'})


def get_action_runs(api_client, job):
    state_filter = lambda job_run: job_run['state'] == ActionRun.STATE_RUNNING.short_name
    for job_run in job['runs']:
        for action_run in filter(state_filter, job_run['runs']):
            try:
                current_state = api_client.action(action_run['id'])
            except client.RequestError:
                log.warn("%s history is gone", action_run['id'])
                continue

            if current_state['state'] != ActionRun.STATE_UNKNOWN.short_name:
                log.info('%s already resumed: %s', action_run['id'], current_state['state'])
                continue
            yield action_run


def handle_job(opts, job):
    api_client = client.Client(opts)
    for action_run in get_action_runs(api_client, job):
        if find_process(action_run):
            return

        log.info('%s finished', action_run['id'])
        resume_job_run(opts, action_run['id'])


def main(opts):
    running_jobs = read_status(opts.filename)
    log.info("Examining %s jobs" % len(running_jobs))
    for job in running_jobs:
        handle_job(opts, job)


def options():
    parser = optparse.OptionParser()
    parser.add_option('-w', '--create-status', action='store_true',
        help="Create a config for the currently running actions.")
    parser.add_option('-r', '--resume', action='store_true',
        help="Automatically resume jobs.")
    parser.add_option('-s', '--server', default='http://localhost:8089',
        help="Ex: http://localhost:8089")
    parser.add_option('-f', '--filename')
    opts, _ = parser.parse_args()
    opts.warn = True
    opts.num_displays = 20

    if not opts.filename:
        parser.error("Filename is required.")

    return opts

if __name__ == "__main__":
    opts = options()
    if opts.create_status:
        create_status(opts)
    else:
        main(opts)