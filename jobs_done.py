#!/opt/rh/rh-python36/root/usr/bin/python
import os
import time
from datetime import datetime
from termcolor import colored
import subprocess


PREV_JOBS = os.path.dirname(os.path.realpath(__file__)) + "/prev_job"
STATS_FILE = os.path.dirname(os.path.realpath(__file__)) + "/stats"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
WIDTH = 12


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


def main():
    prev_jobs, last_session = check_prev_jobs()
    
    format_cmd = "--format=JobId,jobname,alloccpus,elapsed,start,state"
    n_cmds = len(format_cmd.split(","))
    state_idx = format_cmd.split(",").index("state")
    cmd = ["sacct", "-u", "williamb", format_cmd, "-n"]
    if last_session:
        cmd.append("-S")
        cmd.append(last_session)

    out = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    
    if stderr == None:
        lines = stdout.decode().split()
        count = 0
        jobs = []
        for idx, line in enumerate(lines):
            if line.isdigit() and len(line) > 3:
                job = []
                for i in range(n_cmds):
                    job.append(lines[idx + i])

                # Don't bother with jobs that are still running
                if job[state_idx] == "RUNNING":
                    job = []
                else:
                    jobs.append(job)

        count = 0
        jobs_message = []
        for job in jobs:
            if job[0] not in prev_jobs:
                state = job[state_idx]
                date = datetime.strptime(job[4], DATE_FORMAT)
                job[4] = str(date.strftime("%b-%d"))
                message = [x + " "*(WIDTH - len(x)) for x in job]

                # Show COMPLETED as green and FAILED/CANCELLED etc. as red
                if message[state_idx].strip() == "COMPLETED":
                    message[state_idx] = colored(message[state_idx].strip(), "green")
                else:
                    message[state_idx] = colored(message[state_idx], "red")
                
                jobs_message.append("".join(message[1:]))

                save_jobid(job[0], str(date), state)

                count += 1
        if count != 0:
            print(colored("Jobs completed since last session:", attrs=["bold", "underline"]))
            for job in jobs_message:
                print(job)
        else:
            last_session = datetime.strptime(last_session, DATE_FORMAT)
            print(colored(f"No jobs has finished since {last_session.date()}", attrs=["bold", "underline"]))
        save_date()
    else:
        print(stderr.decode())


if __name__ == "__main__":
    exit(main())
