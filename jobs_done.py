#!/opt/rh/rh-python36/root/usr/bin/python
from datetime import datetime
import argparse
import os
import subprocess
import time
import numpy as np
from termcolor import colored


PREV_JOBS = os.path.dirname(os.path.realpath(__file__)) + "/prev_job"
STATS_FILE = os.path.dirname(os.path.realpath(__file__)) + "/stats"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
WIDTH = 24


def check_prev_jobs():
    if os.path.isfile(PREV_JOBS):
        with open(PREV_JOBS) as f:
            temp = f.read().splitlines()
        return temp[:-1], temp[-1]

    else:
        return [], None


def save_jobid(jobid, date, state):
    with open(PREV_JOBS, "a+") as f:
        f.write(jobid + "\n")

    with open(STATS_FILE, "a+") as f:
        f.write(jobid + ";" + date + ";" + state + "\n")


def isTime(input):
    try:
        time.strptime(input, "%H:%M")
        return True
    except ValueError:
        return False


def save_date():
    lines = open(PREV_JOBS).read().splitlines()

    now = datetime.now()
    if isTime(lines[-1]):
        lines[-1] = now.strftime(DATE_FORMAT)
    else:
        lines.append(now.strftime(DATE_FORMAT))

    open(PREV_JOBS, "w").write("\n".join(lines))


def call_sacct(start_time, format_cmd):
    cmd = ["sacct", "-u", "williamb", format_cmd, "-n"]
    if start_time:
        cmd.append("-S")
        cmd.append(start_time)

    out = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()

    if stderr == None:
        return stdout.decode()
    else:
        raise RuntimeError(stderr.decode())


def get_finished_jobs(lines, n_cmds, state_idx):
    jobs = []
    for idx, line in enumerate(lines):
        if line.isdigit() and len(line) > 3:
            job = []
            for i in range(n_cmds):
                job.append(lines[idx + i])

            # Don't bother with jobs that are still running
            if job[state_idx] != "RUNNING":
                jobs.append(job)

    return jobs


def create_print(jobs, prev_jobs, state_idx, day):
    jobs_message = []
    sorting_dates = []

    for job in jobs:
        #XXX: hack to get jobindex to show when called with --day
        job_idx = job[0]
        if day:
            job[0] = "0"

        if job[0] not in prev_jobs and "PENDING" not in job:
            job[0] = job_idx
            state = job[state_idx]

            start = datetime.strptime(job[4], DATE_FORMAT)
            job[4] = str(start.strftime("%b-%d %H:%M"))
            if "Unknown" != job[5]:
                end = datetime.strptime(job[5], DATE_FORMAT)
                job[5] = str(end.strftime("%b-%d %H:%M"))
            else:
                job[5] = "Unknown"

            message = [x + " "*(WIDTH - len(x)) for x in job]
            message = []
            for idx, txt in enumerate(job):
                if idx == 0:  # Format job ID
                    message.append(f"{txt:<10}")

                elif idx == 2:  # Format CPUs
                    message.append(f"{txt:<5}")

                elif idx == 3:  # Format Elapsed
                    message.append(f"{txt:<10}")

                elif idx == 4:  # Format Start
                    message.append(f"{txt:<14}")

                elif idx == 5:  # Format End
                    message.append(f"{txt:<14}")

                else:
                    message.append(txt + " "*(WIDTH - len(txt)))

            # Show COMPLETED as green and FAILED/CANCELLED etc. as red
            if message[state_idx].strip() == "COMPLETED":
                message[state_idx] = colored(message[state_idx].strip(), "green")
            else:
                message[state_idx] = colored(message[state_idx], "red")

            sorting_dates.append(message[4])
            
            # Skip jobid when printing
            jobs_message.append("".join(message))
            
            if not day:
                save_jobid(job[0], str(start), state)

    sorted_indices = np.argsort(sorting_dates)
    jobs_message = np.array(jobs_message)[sorted_indices]
    jobs_message = list(jobs_message)
    return jobs_message


def main():
    parser = argparse.ArgumentParser(description="Get info on finished jobs")
    parser.add_argument(
            "--day",
            default=False,
            action="store_true",
            help="Show finished jobs since 00:00 today"
        )

    prev_jobs, last_session = check_prev_jobs()

    format_cmd = "--format=jobid,jobname%30,alloccpus,elapsed,start,end,state"
    n_cmds = len(format_cmd.split(","))
    state_idx = format_cmd.split(",").index("state")

    args = parser.parse_args()
    if args.day:
        sacct_output = call_sacct("00:00", format_cmd)
    else:
        sacct_output = call_sacct(last_session, format_cmd)

    lines = sacct_output.split()
    jobs = get_finished_jobs(lines, n_cmds, state_idx)
    jobs_message = create_print(jobs, prev_jobs, state_idx, args.day)

    if jobs_message:
        print(colored("Jobs completed since last session:", attrs=["bold", "underline"]))
        headers = [
                colored((x + " "*(WIDTH - len(x))).capitalize(), attrs=["bold"]) for x in format_cmd[9:].split(",")
            ]
        print("".join(headers))
        for job in jobs_message:
            print(job)
    else:
        last_session = datetime.strptime(last_session, DATE_FORMAT)
        print(colored(f"No jobs has finished since {last_session.date()}", attrs=["bold", "underline"]))
    save_date()


if __name__ == "__main__":
    exit(main())
