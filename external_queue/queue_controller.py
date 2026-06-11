#!/usr/bin/env python3
import datetime
import string
import pandas as pd
from pathlib import Path
import sys
import glob
import re
import subprocess
import random
import os


relative_queued_path = "external_queue/queued"
relative_finished_path = "external_queue/finished"
relative_running_path = "external_queue/running"
relative_node_config_path = "external_queue/runner_config"

# In the node config, if something is empty or marked ALL, it will accept any job (but only if that job wasnt let in on another node)
wildcard_key = "ALL"

max_runners = 8

class JobInfo(object):
    input_path = None
    node_partition = ""
    our_path = None

    display_name = ""
    job_id = 0
    status = ""

    def __init__(self, input_path, node_partition, our_path, job_id, display_name, status):
        self.input_path = Path(input_path)
        self.node_partition = node_partition
        self.our_path = our_path
        self.job_id = int(job_id) if job_id is not None else 0
        self.display_name = display_name
        self.status = status

def get_value_of_key_from_string_list(key, string_list, can_be_absent = False):
    try:
        return string_list[string_list.index(key) + 1].rstrip()
    except:
        if can_be_absent: return None
        else: print("Could not find key", key)

def get_job_objects(relative_path):
    jobjects = list()
    pathlist = Path(relative_path).glob('*')
    
    for path in pathlist:
        with open(path, "r") as queue_file:
            file_content = queue_file.read().split()

            jobjects.append(JobInfo(
                get_value_of_key_from_string_list("input_path", file_content, False), 
                get_value_of_key_from_string_list("node_partition", file_content, False),
                path,
                get_value_of_key_from_string_list("job_id", file_content, True),
                get_value_of_key_from_string_list("display_name", file_content, True),
                get_value_of_key_from_string_list("status", file_content, True),
                ))
    
    return jobjects

def get_node_config(config_path):
    path = Path(config_path)
    with open(path, "r") as config_file:
        node_assignments = config_file.read().splitlines()
    
    # For empty slots, return them as ALL to indicate every job is welcome there
    for i in range(max_runners - len(node_assignments)):
        node_assignments.append("ALL") 

    return node_assignments

def submit_job(script_path):
    parent_directory = script_path.parent

    saved_content = ""
    # Write an update command to the bash file
    with open(script_path, "r+") as script_file:
        saved_content = script_file.read()
        script_file.write("\npython ~/external_queue/queue_controller.py equpdate ${SLURM_JOBID} -r")

    failed = False
    try:
        result = subprocess.run(["sbatch", 
                                "--parsable",
                                script_path.name], 
            cwd = parent_directory,
            capture_output = True,
            text = True,
            check = True,
            )
    except:
        print("Error detected in sbatch command:\n", result)
        failed = True

    # Restore the batch file as it was when the user send the command, so the update command is only seen by slurm and not the user
    with open(script_path, "w") as script_file:
        script_file.write(saved_content)

    if failed:
        sys.exit("Exited due to issue with job submission")

    job_id = int(result.stdout.strip())
    print(f"Submitted job with ID {job_id}")

    return job_id

def submit_queued_job(job):
    Path.unlink(job.our_path)

    job_id = submit_job(job.input_path)

    file_name = (job.display_name + "." if job.display_name is not None else "") + str(job_id) + ".job"
    with open(f"{relative_running_path}/{file_name}", "x") as running_file:
        running_file.write("input_path" + " " + str(job.input_path) + "\n")
        running_file.write("node_partition" + " " + job.node_partition + "\n")
        if job.display_name is not None:
            running_file.write("display_name" + " " + job.display_name + "\n")
        running_file.write("job_id" + " " + str(job_id) + "\n")
        running_file.write("status running\n")

def get_current_running_count():
    result = subprocess.run(["squeue", 
                             "--me",
                             "--format=NODE", # Okay this is very stupid but this just replaces every job with the word NODE, but it makes it easy to count so whatever                
                             ],
        capture_output = True,
        text = True,
        check = True,
    )
    # -1 because we also counted the header
    queue_size = result.stdout.count("NODE") - 1
    return queue_size

def gather_results(write_file, input_path):
    write_file.write("RESULTS ARE HERE WAAAUW")

# Check what jobs are finished, and finish them if they are, then check the whole queue
# For when you need to manually prompt a full update for one reason or another
def update_everything():
    result = subprocess.run(["squeue", 
                            "--me",            
                            ],
    capture_output = True,
    text = True,
    check = True,
    )

    jobs = get_job_objects(relative_running_path)
    for job in jobs:
        if str(job.job_id) not in result.stdout:
            finish_job(job, check_for_updates = False)
    
    check_queue_and_submit_jobs()

def add_to_external_queue(script_path, node_partition = "", display_name = None):
    # this might not be unique enough
    file_name = display_name if display_name is not None else ''.join(random.choices(string.ascii_letters + string.digits, k = 8))

    with open(f"{relative_queued_path}/{file_name}.job", "x") as new_file:
        new_file.write("input_path" + " " + script_path + "\n")
        new_file.write("node_partition" + " " + node_partition + "\n")
        if display_name is not None:
            new_file.write("display_name" + " " + display_name + "\n")
        new_file.write("status queued\n")

def check_queue_and_submit_jobs(called_from_job = False):
    current_running_count = get_current_running_count()
    available_runners = max_runners - current_running_count

    if called_from_job:
        # The runner this is called from is about to free up, so we can lie a bit
        available_runners += 1

    print("Occupied:", current_running_count, "available:", available_runners)

    if available_runners <= 0:
        print("No available runners, waiting...")
        return
    
    queued_jobs = get_job_objects(relative_queued_path)

    node_config = get_node_config(relative_node_config_path)

    jobs_to_submit = list()

    for job in queued_jobs:
        if job.node_partition in node_config:
            node_config.remove(job.node_partition)
            jobs_to_submit.append(job)
        elif "ALL" in node_config:
            node_config.remove("ALL")
            jobs_to_submit.append(job)

    for job in jobs_to_submit:
        if(available_runners <= 0):
                break
        submit_queued_job(job)
        available_runners -= 1
    
def finish_job(job, check_for_updates = True, called_from_job = False):
    Path.unlink(job.our_path) # Delete the job file, all relevant data is in an object anyway

    # ternary abuse? must be a better way. also maybe make it a function
    file_name = (job.display_name + "." if job.display_name is not None else "") + str(job.job_id) + ".job"

    with open(f"{relative_finished_path}/{file_name}", "x") as finished_file:
        finished_file.write("input_path" + " " + str(job.input_path) + "\n")
        finished_file.write("node_partition" + " " + job.node_partition + "\n")
        if job.display_name is not None:
            finished_file.write("display_name" + " " + job.display_name + "\n")
        finished_file.write("job_id" + " " + str(job.job_id) + "\n")
        finished_file.write("status finished\n")

        gather_results(finished_file, job.input_path)
    
    if check_for_updates:
        # There should be some free space again
        check_queue_and_submit_jobs(called_from_job)

def run_command(command, arguments):
    match command:
        case "eqbatch":
            eqbatch(arguments)
        case "eqconfig":
            eqconfig(arguments)
        case "eq":
            eq(arguments)
        case "eqedit":
            eqedit(arguments)
        case "equpdate":
            equpdate(arguments)
    
def eqbatch(arguments):
    if len(arguments) < 2:
        print("Usage: eqbatch <script_path> <node_partition A/B/C/etc...> [display_name]")
        return
    
    # script path, node partition, display name (optional)
    add_to_external_queue(arguments[0], arguments[1], arguments[2] if len(arguments) > 2 else None)

    check_queue_and_submit_jobs()

def eqconfig(arguments):
    print("do something")

def eq(arguments):
    print_running, print_queued, print_finished = False, False, False

    if len(arguments) <= 0:
        print_running, print_queued = True, True
    else:
        match arguments[0] if len(arguments) > 0 else None:
            case "-a":
                print_running, print_queued, print_finished = True, True, True
            case "-f":
                print_finished = True
            case "-r":
                print_running = True
            case "-q":
                print_queued = True
            case _:
                print("eq [-a for all, -f for finished, -r for running and -q for queued jobs] [-s to not update]")
                print("Does not report on jobs not handled by the external queue.")
                return
    
    # If there's a mistake in the input file, it will automatically check whenever you call eq so it wont lie to you
    # -s blocks this because it can be a little expensive to double check everything
    if len(arguments) < 2 or arguments[1] != "-s":
        update_everything()

    jobs = list()
    if print_finished:
        jobs += get_job_objects(relative_finished_path)
    if print_queued:
        jobs += get_job_objects(relative_queued_path)
    if print_running:
        jobs += get_job_objects(relative_running_path)

    info = list()
    
    if len(jobs) > 0:
        for job in jobs:
            info.append([str(job.job_id), job.display_name, job.node_partition, str(job.input_path), job.status, "0"])
    else:
        # Let them know we did something 
        info.append(["none", "none", "none", "none", "none", "none"])
    table = pd.DataFrame(info, columns = ["JOBID", "NAME", "PARTITION", "PATH", "STATUS", "TIME"])
    print(table)

def eqedit(arguments):
    print("do something")

def equpdate(arguments):
    if len(arguments) > 0:
        # Important to track, because a job that is finishing will still make the script think no new jobs can be submitted
        # Also I dont know naming conventions but -r is for robot beep boop
        called_from_job = True if arguments[1] == "-r" else False
        for job in get_job_objects(relative_running_path):
            if job.job_id == int(arguments[0]):
                finish_job(job, called_from_job = called_from_job)
                break
    else:
        update_everything()

if len(sys.argv) > 1:
    print(sys.argv)
    run_command(sys.argv[1], sys.argv[2:])

