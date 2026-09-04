import datetime
import pandas as pd
from pathlib import Path
import sys
import subprocess
import random
import os
import tcmu

from data_collector import scrape_results

relative_queued_path = "_queued"
relative_finished_path = "_finished"
relative_running_path = "_running"
relative_node_config_path = "smartqueue/runner_config"

relative_excel_path = "smartqueue.xlsx"

# In the node config, if something is empty or marked ALL, it will accept any job (but only if that job wasnt let in on another node)
wildcard_key = "ALL"

# Degree by which our freedom is supressed 
# Maybe shouldn't be hardcoded? I also dont wanna overcomplicate things with ANOTHER config file
max_runners = 8

# Holds all the job info we need. Is written to .job files and read from .job files
class JobInfo(object):
    input_path = None
    node_partition = ""
    started_at = 0
    our_path = None

    display_name = ""
    job_id = 0
    status = ""

    def __init__(self, input_path, node_partition, started_at, our_path, job_id, display_name, status):
        self.input_path = Path(input_path)
        self.node_partition = node_partition
        self.started_at = started_at if started_at is not None else 0
        self.our_path = our_path
        self.job_id = job_id if job_id is not None else 0
        self.display_name = display_name
        self.status = status

def get_value_of_key_from_string_list(key:str, string_list, can_be_absent = False) -> str | None:
    try:
        return string_list[string_list.index(key) + 1].rstrip()
    except:
        if can_be_absent: 
            return None
        else: 
            print("Could not find key", key)

def get_job_objects(relative_path:str) -> list[JobInfo]:
    jobjects = list()
    pathlist = Path(relative_path).glob('*.job')
    
    for path in pathlist:
        with open(path, "r") as queue_file:
            file_content = queue_file.read().split()

            jobjects.append(JobInfo(
                get_value_of_key_from_string_list("input_path", file_content, False), 
                get_value_of_key_from_string_list("node_partition", file_content, False),
                get_value_of_key_from_string_list("started_at", file_content, True),
                path,
                get_value_of_key_from_string_list("job_id", file_content, True),
                get_value_of_key_from_string_list("display_name", file_content, True),
                get_value_of_key_from_string_list("status", file_content, True),
                ))
    
    return jobjects

def get_node_config(config_path:str) -> list[str]:
    path = Path(config_path)
    with open(path, "r") as config_file:
        node_assignments = config_file.read().splitlines()
    
    # For empty slots, return them as ALL to indicate every job is welcome there
    for i in range(max_runners - len(node_assignments)):
        node_assignments.append("ALL") 

    return node_assignments

def submit_job(script_path:Path) -> int:
    parent_directory = script_path.parent

    saved_content = ""
    # Write an update command to the bash file
    with open(script_path, "r") as script_file:
        saved_content = script_file.read()    

    # Make a copy of the input file, add a line to update the script, send it and then delete it
    # Note that this does get submitted with that name, so anything using the slurm name will have a '_' affixed 
    script_path_temp = script_path.with_name(script_path.name + "_")
    with open(script_path_temp, "x") as script_file_temp:
        script_file_temp.write(saved_content)
        # Could probably just call the path at this point so we're not on bash aliasses
        # 
        script_file_temp.write('\nbash -ic "squpdate ${SLURM_JOBID} -r"')

    failed = False

    try:
        result = subprocess.run(["sbatch", 
                                "--parsable",
                                script_path_temp.name], 
            cwd = parent_directory,
            capture_output = True,
            text = True,
            check = True,
            )
    # We NEED to handle this well or it will drive people insane when typo's in their slurm script secretely stop everything
    except subprocess.CalledProcessError as error:
        print("Error detected in sbatch command")
        print(error)
        print(error.stderr)
        failed = True

    # Delete the temporary file, we dont need it anymore
    Path.unlink(script_path_temp)

    if failed:
        sys.exit("Exited due to issue with job submission")

    job_id = int(result.stdout.strip())
    print(f"Submitted job with ID {job_id}")

    return job_id

# Turn a JobInfo object into a .job file. If a write_function is provided, it will be called after the normal definitons have been written
def write_job_file(folder_location:str, job:JobInfo, write_function = None):
    file_name = (job.display_name + "." if job.display_name is not None else "") + str(job.job_id) + ".job"

    # Make the dir in case it's not there (I have deleted them by accident more than once)
    os.makedirs(Path(folder_location), exist_ok = True)

    with open(f"{folder_location}/{file_name}", "x") as new_file:
        new_file.write("input_path" + " " + str(job.input_path) + "\n")
        new_file.write("node_partition" + " " + job.node_partition + "\n")
        new_file.write("started_at" + " " + str(datetime.datetime.today().timestamp()) + "\n")
        new_file.write("job_id" + " " + str(job.job_id) + "\n")
        new_file.write("status " + job.status + "\n")

        if job.display_name is not None:
            new_file.write("display_name" + " " + job.display_name + "\n")

        if write_function is not None:
            write_function(new_file, job)

# Submit a job from the queue, and move it to the running folder
def submit_queued_job(job:JobInfo):
    Path.unlink(job.our_path)

    # We change from the queue id (Q9999) to the actual slurm job id here
    job.job_id = submit_job(job.input_path)
    
    job.status = "running"

    write_job_file(relative_running_path, job)

# Get how many jobs we are currently running
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

# When using smartqueue for the first time, or when using utilities like PyFrag, we can end up with unmanaged jobs, so import them to ease the transition or support other tools
def import_unmanaged_jobs(managed_jobs:list[JobInfo]) -> list[JobInfo]:
    result = subprocess.run(["squeue", 
                             "--me",
                             "-h", # custom formatting
                             "-o", # no header
                            #  %A: Job id
                            #  %T: State, what the job is actually doing (RUNNING, QUEUED, PENDING etc)
                            #  %S: Time it has been running (already formatted) (this sucks because we need a timestamp)
                            #  %o: 'Command', but in our case a full path to the bash submission file we're running
                            #  All seperated with '|'
                            # See https://slurm.schedmd.com/squeue.html for more info
                             "%A|%T|%S|%o", 
                             ],
        capture_output = True,
        text = True,
        check = True,
    )    

    # Make a list of the managed id's so we can avoid checking them twice
    job_ids:list[int] = []
    
    for job in managed_jobs:
        job_ids.append(job.job_id)

    new_jobs:list[JobInfo] = list()

    # You should run the subprocess command in bash to see how it looks like, but it's something like this for every job:
    # 12345|RUNNING|2026-07-28T09:42:05|bla/bla/bla/job_file
    # Where long_job_string is a single line as you see above
    for long_job_string in result.stdout.split():
        split_job_string = long_job_string.split('|')

        # Already managed, skip!!!
        if split_job_string[0] in job_ids:
            continue
            
        # Write them to the running folder, officially importing them 🥹
        new_job = JobInfo(
            input_path = split_job_string[3],
            node_partition = "NONE",
            # If it's N/A we havent started yet, so just assume it's now (even if it's queued or something and technically hasn't started)
            started_at = int(datetime.datetime.fromisoformat(split_job_string[2])) if split_job_string[2] != "N/A" else int(datetime.datetime.today().timestamp()),
            our_path = None,
            job_id = split_job_string[0],
            display_name = "imported_slurm_job",
            status = split_job_string[1].lower(),
        )

        write_job_file(relative_running_path, new_job)
        new_jobs.append(new_job)

    return new_jobs

def update_running_status(running_jobs:list[JobInfo]) -> list[JobInfo]:
    result = subprocess.run(["squeue", 
                             "--me",
                             "-h", # custom formatting
                             "-o", # no header
                            #  %A: Job id
                            #  %T: State, what the job is actually doing (RUNNING, QUEUED, PENDING etc)
                            #  All seperated with '|'
                            # See https://slurm.schedmd.com/squeue.html for more info
                             "%A|%T", 
                             ],
        capture_output = True,
        text = True,
        check = True,
    )  

    # Make a dictionary of kind job_id = job for easy access
    job_id_index:dict[str, JobInfo] = {}
    for job in running_jobs:
        job_id_index[job.job_id] = job

    # long_job_string is something like :
    # 12345|RUNNING
    for long_job_string in result.stdout.split():
        # split into job_id and status
        split_job_string = long_job_string.split('|')

        # There can be unmanaged jobs in the result we get, which is not our problem (but is for import_unmanaged_jobs)
        if split_job_string[0] not in job_id_index:
            continue

        # get the job object belonging to the id
        job = job_id_index[split_job_string[0]]
        # Set job status to the new status 
        job.status = split_job_string[1].lower()

    return running_jobs

def gather_results(write_file, job:JobInfo):
    write_file.write("Results below this line:\n")
    print("Gathering results for job", job.job_id, job.display_name)

    calculation_directory = Path(job.input_path).parent
    results, seperate_results = scrape_results(calculation_directory, job.job_id)

    # Write neatly into the finished job file
    for key in results:
        write_file.write(key + " " + str(results[key]) + "\n")
    
    # Write these into their own results file. We could consider giving them their own directory to keep it overseeable?
    for extension in seperate_results:
        with open(f"{relative_finished_path}/{job.display_name + '.' if job.display_name is not None else ''}{str(job.job_id)}.{extension}", "x") as new_file: 
            new_file.write(seperate_results[extension])
            print("New file added:", new_file.name)

    # Write to excel in case we're not anarchists
    excel = pd.read_excel(relative_excel_path)

    presentable_dict = {"status": results["status"], "job_id":str(job.job_id), "name":job.display_name, "path":job.input_path}
    presentable_dict.update(seperate_results)
    presentable_dict.update(results)

    new_row = pd.DataFrame(presentable_dict, index = [0])
    excel = pd.concat([excel, new_row], ignore_index = True)

    with pd.ExcelWriter(relative_excel_path, mode = 'a', if_sheet_exists = 'overlay') as writer:
        excel.to_excel(writer, sheet_name = 'Jobs', index = False)
        print("Updated", relative_excel_path)

# def get_results(calculation_directory, job.job_id):
#     results = dict()
#     seperate_results = dict()

#     result_object = tcmu.read(calculation_directory)

#     results["engine"] = result_object.engine
#     results["status"] = tcmu.quick_status(calculation_directory)
#     results["energy_out"] = result_object.properties.energy.bond


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

# Add to our queue
def add_to_external_queue(script_path:str, node_partition = "", display_name = None):
    write_job_file(relative_queued_path, 
        JobInfo(
            input_path = script_path, 
            node_partition = node_partition,
            started_at = None,
            our_path = None, #it's a headache to add here and it doesnt matter since we're gonna instantly write it there anyway
            # what are the odds they overlap? I'll risk it
            job_id = 'Q' + str(random.randint(0, 99999)),
            display_name = display_name,
            status = "queued"
        ))

# Check how many jobs are running, and find a job from the queue (if any) that is allowed to run
def check_queue_and_submit_jobs(called_from_job = False):
    current_running_count = get_current_running_count()
    available_runners = max_runners - current_running_count

    if called_from_job:
        # The runner this is called from is about to free up, so we can lie a bit
        available_runners += 1

    if available_runners <= 0:
        return
    
    running_jobs = get_job_objects(relative_running_path)
    queued_jobs = get_job_objects(relative_queued_path)

    node_config = get_node_config(relative_node_config_path)

    jobs_to_submit = list()

    # Remove jobs from the config list if theyre already on that partition
    for running_job in running_jobs:
        if running_job.node_partition in node_config:
            node_config.remove(running_job.node_partition)
        elif "ALL" in node_config:
            node_config.remove("ALL")
    
    # Loop through partitions first so that the config order is also the priority
    used_partitions = list()
    for partition in node_config:
        job_to_remove = None
        for queued_job in queued_jobs:
            # Partitions can also look like 'B-', in which case we check for B partition jobs
            # So also check if the same partition matches with a '-' job
            if queued_job.node_partition == partition or (queued_job.node_partition + '-') == partition:
                jobs_to_submit.append(queued_job)
                job_to_remove = queued_job
                used_partitions.append(partition)
                break
            elif partition == "ALL":
                jobs_to_submit.append(queued_job)
                job_to_remove = queued_job
                used_partitions.append("ALL")
                break
        if job_to_remove is not None:
            queued_jobs.remove(job_to_remove)
    
    # Job paritions that end in '-' will pull an ALL job if there are no more jobs for that partition.
    for leftover_partition in list(set(node_config) - set(used_partitions)):
        job_to_remove = None
        if leftover_partition[-1] == '-':
            for queued_job in queued_jobs:
                jobs_to_submit.append(queued_job)
                job_to_remove = queued_job
                break
        if job_to_remove is not None:
            queued_jobs.remove(job_to_remove)

    for job in jobs_to_submit:
        if(available_runners <= 0):
                break
        submit_queued_job(job)
        available_runners -= 1
    
# The job finished, so gather the results and move it to the finished folder
def finish_job(job:JobInfo, check_for_updates = True, called_from_job = False):
    Path.unlink(job.our_path) # Delete the job file, all relevant data is in an object anyway

    job.status = "finished"

    write_job_file(relative_finished_path, job, gather_results)

    if check_for_updates:
        # There should be some free space again
        check_queue_and_submit_jobs(called_from_job)

def run_command(command:str, arguments:list[str]):
    match command:
        case "sqbatch":
            sqbatch(arguments)
        case "sqconfig":
            sqconfig(arguments)
        case "sq":
            sq(arguments)
        case "sqcancel":
            sqcancel(arguments)
        case "sqclear":
            sqclear(arguments)
        case "squpdate":
            squpdate(arguments)

# Submit a job to the smart queue
def sqbatch(arguments):
    if len(arguments) < 2:
        print("Usage: sqbatch <script_path> <node_partition A/B/C/etc...> [display_name]")
        return
    
    # script path, node partition, display name (optional)
    add_to_external_queue(arguments[0], arguments[1], arguments[2] if len(arguments) > 2 else None)

    check_queue_and_submit_jobs()

def sqconfig(arguments):
    if len(arguments) < 1:
        with open(relative_node_config_path, "r") as config_file:
            print("Current runner config:")
            print(config_file.read())
    elif arguments[0] == "help":
        print("Usage: sqconfig <A/B-/ALL/...> [A/B-/ALL/...] ...")
        print("Updates the external queue runner config")
        print("The order in the sqconfig is also the order by which jobs are pulled from the queue")
        print("Adding a '-' after the partition ('A-') will grab an 'ALL' job if there are no jobs for the 'A' partition.")
        print("Accepts partition string for:", max_runners, "runners")
    else:
        with open(relative_node_config_path, "w") as config_file:
            for string in arguments:
                config_file.write(string + "\n")
            for i in range(max_runners - len(arguments)):
                config_file.write("ALL\n")

# Get the current 'external queue', including the queued jobs at the top. 
# Comes with -r(unning), -q(ueued), -f(inished) and -a(ll) options too, but default is running and queued
def sq(arguments):
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
                print("sq [-a for all, -f for finished, -r for running and -q for queued jobs] [-s to not update]")
                print("Does not report on jobs not handled by the external queue.")
                return
    
    # If there's a mistake in the input file it will never update the job status, so we always double check if jobs are REALLY running
    # -s blocks this because it can be a little expensive to double check everything
    if len(arguments) < 2 or arguments[1] != "-s":
        update_everything()

    jobs = list()

    if print_finished: 
        jobs += get_job_objects(relative_finished_path)

    if print_queued:
        jobs += get_job_objects(relative_queued_path)

    if print_running:
        running_jobs = update_running_status(get_job_objects(relative_running_path))

        # If the amount of current running jobs is somehow more than the running jobs we are tracking, we got unmanaged jobs
        # Either from first time use, betrayal or unmanaged tools, we'd better just do our best and neatly import them
        if get_current_running_count() > len(running_jobs):
            running_jobs += import_unmanaged_jobs(running_jobs)

        jobs += running_jobs

    info = list()
    
    if len(jobs) > 0:
        for job in jobs:
            time_running = "N/A"
            if job.started_at != 0:
                time_running = str(datetime.timedelta(seconds = int(datetime.datetime.today().timestamp() - float(job.started_at))))

            info.append([str(job.job_id), job.display_name, job.node_partition, str(job.input_path), job.status, time_running])
    else:
        # Let them know we did something 
        info.append(["none", "none", "none", "none", "none", "none"])
    table = pd.DataFrame(info, columns = ["JOBID", "NAME", "PARTITION", "PATH", "STATUS", "TIME"])

    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.colheader_justify', 'left')

    print(table)

# Cancel a queued or running job using their job_id/queue_id
def sqcancel(arguments):
    if len(arguments) > 0:
        for job in get_job_objects(relative_running_path):
            if job.job_id == arguments[0]:
                subprocess.run(["scancel", str(job.job_id)], check = True)
                finish_job(job, check_for_updates = False)
                print(f"Cancelled job {(job.display_name + ' ') if job.display_name is not None else ''}with ID {job.job_id}")
                return
        for job in get_job_objects(relative_queued_path):
            if job.job_id == arguments[0]:
                Path.unlink(job.our_path)
                print(f"Cancelled queued job {(job.display_name + ' ') if job.display_name is not None else ''}with ID {job.job_id}")
                return
            
        print(f"Couldn't find job with id {arguments[0]}")
    else:
        print("Usage: sqcancel <job_id>")
        print("Cancels a running job and moves it to the finished queue. Does not pull in new jobs.")

# Clear a finished job from the finished folder
def sqclear(arguments):
    if len(arguments) > 0:
        for job in get_job_objects(relative_finished_path):
            if job.job_id == arguments[0]:
                # Just grab everything with the job_id so we get both the .job and possible .xyz 
                paths = list(Path(relative_finished_path).glob(f'*{job.job_id}*'))
                for path in paths:
                    Path.unlink(path)

                print(f"Cleared finished job {(job.display_name + ' ') if job.display_name is not None else ''}with ID {job.job_id}")
                return
        print(f"Couldn't find job with id {arguments[0]}")
    else:
        print("Usage: sqclear <job_id>")
        print("Clears a finished job from the finished queue. Does not pull in new jobs.")

def squpdate(arguments):
    if len(arguments) > 0:
        # Important to track, because a job that is finishing will still make the script think no new jobs can be submitted because that job is technically still running
        # Also I dont know naming conventions but -r is for robot beep boop
        called_from_job = True if arguments[1] == "-r" else False
        for job in get_job_objects(relative_running_path):
            if int(job.job_id) == int(arguments[0]):
                finish_job(job, called_from_job = called_from_job)
                break
    else:
        update_everything()

# Run whatever command was selected
# print("Received arguments:", sys.argv)
if len(sys.argv) > 1:
    # Make sure the working dir is always the rootdir. Easiest way is to get our location, which should be ~/external_queue/, and go back a step
    our_dir = Path(os.path.dirname(os.path.realpath(__file__)))
    root_dir = our_dir.parent
    old_dir = os.curdir

    # Make sure we return to the working dir the user was in even
    os.chdir(root_dir)
    # argv[0] is our file.py, argv[1] is the command, rest is arguments
    run_command(sys.argv[1], sys.argv[2:])
    # and put everything back toghether here
    os.chdir(old_dir)