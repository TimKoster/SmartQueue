import pandas as pd
from pathlib import Path
import sys
import glob
import re
import subprocess

# Path to our excel file with the overview
excel_path = "overview.xlsx"
# The degree by which our personal freedoms are suppressed
max_queue_size = 8 
# If False, we use max_jobs as the classical system. A job cannot autosubmit itself if the total current running jobs 
# is bigger than its max_jobs variable. This means that if you have 6 running robs, and all jobs in the external queue have
# a max_jobs of 6, it wont submit anything and keep 2 nodes always free
# If True, it will interpret max_jobs as standard priorities. If 6 jobs are running, it will add two new jobs from
# the external queue, starting from the queued job with the highest 
priority_instead_of_max_jobs = False # Not yet implemented

print("Updating", excel_path)

# sys.exit("Work in PROGRESS")

def collect_the_data(bash_path, excel, index) :
    bash_path = Path(bash_path)
    folder_path = bash_path.parent

    slurm_list = glob.glob('slurm*.out', root_dir = folder_path)
    # If there's a slurm, we're not queued anymore
    if len(slurm_list) > 0:
        slurm_path = Path(folder_path).joinpath(slurm_list[0])
        
        # This is all just for checking the state of the job. It sucks a bit but it's not otherwise reported
        # Would really suck if they changed the formatting or people used keywords such as CANCELLED and NORMAL_TERMINATION in their jobs files
        # Check if we terminated normally, otherwise we FAILED
        with open(slurm_path, "r") as slurm_file:
            # Check ook voor SCF convergence en geometry convergence
            slurm_text = slurm_file.read()
            if "NORMAL TERMINATION" not in slurm_text:
                results_folder = glob.glob('ams.results', root_dir = folder_path)
                # If there's a results folder, we're still working or we got cancelled
                if len(results_folder) > 0:
                    # There's a slurm, we didn't terminate normally but there's a results folder, so either we're still busy or we got killed halfway by (probably) user input
                    if "CANCELLED" in slurm_text:
                        excel.loc[index, "status"] = "CANCELLED"
                    else:
                        excel.loc[index, "status"] = "WORKING"
                # No results folder but there's a slurm and the job didn't terminate normally, meaning it failed :(
                else:
                    excel.loc[index, "status"] = "FAILED" 
            else :
                # We have a slurm, and it says 'NORMAL TERMINATION', yay! Collect some nice (?) data
                excel.loc[index, "status"] = "COMPLETE"
                # Put the coordinates in
                xyz_list = glob.glob('*.xyz', root_dir = folder_path)
                if len(xyz_list) > 0: # Zou ook kunnen missen by gekke jobs? Of als ie faalt
                    xyz_path = Path(folder_path).joinpath(xyz_list[0])
                    with open(xyz_path, "r") as xyz_file:      
                        excel.loc[index, "xyz_out"] = xyz_file.read()
                
                # Find the energy of formation
                try:
                    log_list = glob.glob('*.log', root_dir = folder_path)
                    if len(log_list) > 0:
                        log_path = Path(folder_path).joinpath(log_list[0])
                        with open(log_path, "r") as log_file:
                            raw_text = log_file.read()
                            # I'm not even gonna pretend to know how regex works, thanks chatgpt. 
                            # Update: chatgpt lied to me and now I need to manually update everything because all the energies are wrong
                            # I deserve this. It's fixed now though
                            m = list(re.finditer(r"ENERGY OF FORMATION", raw_text))[-1]
                            subtext = raw_text[m.end():]
                            if len(subtext) > 0:
                                m2 = re.search(r"([-+]?[0-9]*\.?[0-9]+)\s*KCAL/MOL", subtext, flags=re.IGNORECASE)
                                energy_kcal = float(m2.group(1))
                            
                            excel.loc[index, "energy_out"] = energy_kcal
                except:
                    excel.loc[index, "energy_out"] = "ERROR"

    # If there's no results folder and no slurm file, we're still queued 
    else:
        excel.loc[index, "status"] = "QUEUED"

def submit_job(script_path):
    parent_directory = Path(script_path).parent
    result = subprocess.run(["sbatch", 
                             "--parsable",
                             script_path], 
        cwd = parent_directory,
        capture_output = True,
        text = True,
        check = True,
        )
    # if result != 0:
    #     print("ERROR with", script_path)
    #     print(result)
    #     sys.exit("Exited due to with job submission")

    job_id = int(result.stdout.strip())

    return job_id

def add_to_excel(excel, script_path, nice_name, max_jobs):
    new_row = pd.DataFrame({'status': ['QUEUED'], 'name': [nice_name], 'path': [script_path], 'max_jobs': [int(max_jobs)]})
    return pd.concat([excel, new_row], ignore_index = True)

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

def get_submittable_job_paths(excel, running_jobs):
    list_of_paths = []
    index = -1 # Might as well track where we are since we're looping through everything anyway

    for job_id_column in excel["status"]:
        index += 1
            # If it's not queued, we dont care
        if(job_id_column != "QUEUED"): 
            continue

        # So if the max_jobs of the queued job is 5, and we're running 7 jobs, nothing happens
        # If we're running 4 jobs or less however, it can be submitted
        # This is how we enforce priority, so we can always have a few free nodes while the lower priority jobs can only take up so many
        if excel["max_jobs"][index] <= running_jobs:
            continue
        
        list_of_paths.append(excel["path"][index])

    return list_of_paths

#### Can either take no command line arguments to do all, or take a job ID to only update the one job

excel = pd.read_excel(excel_path)
# If we're called at the end of a job so we can start a new job, the queue will technically still include us and so it wont start a new job if we're going from 8 to 7
executed_inside_job = False

# If no params submitted, just update everything by going through every job_id in the excel file and searchin through their working folders
if len(sys.argv) < 2:
    # Find the status column and check if they're still busy
    status_column = excel["status"]

    index = 0
    for status_cell in status_column:
        # I like to add dividers sometimes okay
        if(status_cell != "COMPLETED" and len(status_cell) > 3):
            # For every still working entry, check their path and see if the results are in
            # If they are, COLLECT THAT DATA (and put it in the excel)
            collect_the_data(excel["path"][index], excel, index)
        index += 1

# If a job ID is supplied to the script, only update that entry
elif str(sys.argv[1]) == "update":
    if len(sys.argv) < 3:
        raise Exception("Missing job_id argument :(")
    if len(sys.argv) > 3:
        executed_inside_job = True
            
    job_id = int(sys.argv[2])

    # Apparently not straightforward. Loop through the excel object to get the right column, and then ask the excel object the index of that column (and then take the first item from that)
    index = excel.index[excel["job_id"] == job_id][0]
    collect_the_data(excel["path"][index], excel, index)

elif str(sys.argv[1]) == "submit":
    if len(sys.argv) < 3:
        raise Exception("Not enough arguments for update (also expecting script path and a name, with optional max_jobs integer)")
    excel = add_to_excel(excel, sys.argv[2], sys.argv[3], int(sys.argv[4]) if len(sys.argv) > 3 else 6)

# From here, we check if there's queue space and if so, auto add something to the queue
running_nodes = get_current_running_count() + (-1 if executed_inside_job else 0)

failsafe_sbatch_count = 0

print("TEST | Currently running nodes:", running_nodes)

# We got empty places!!!!! SUBMITTTT!!!!!!!!!! FLOOD THE STREETS WITH BLOOD OF SLURM!!! 
if running_nodes < max_queue_size:
    path_list = get_submittable_job_paths(excel, running_nodes)
    # TODO: There's probably a bug where, if you have a bunch of jobs with max_job 4, and got 8 free spots, it will fill beyond 4
    # Only happens when doing multiple at once, which I don't think can ever happen if you use all my wrappers, since it should immediately update and send the job?
    # Maybe if you just type them into excel it could happen
    for i in range(max_queue_size - running_nodes):
        # Check if there's enough queued jobs before we try to run a non-existent job
        if (i + 1) > len(path_list):
            break

        # Completely arbitrary but in case something somewhere goes wrong we shouldn't flood the cluster with 10 million jobs
        failsafe_sbatch_count += 1
        if failsafe_sbatch_count > 10:
            sys.exit("Infinite loop maybe detected???? ABORTED")
            break # it should already exit but im not taking chances

        job_id = submit_job(path_list[i])
        if job_id < 1:
            raise Exception("Returned job_id after sbatch was", job_id)
        
        index = excel.index[excel["path"] == path_list[i]][0]
        excel.loc[index, "job_id"] = job_id
        excel.loc[index, "status"] = "WORKING"

        print("Succesfully submitted", excel.loc[index, "name"], "as job", job_id)

# Write everything we just did to excel
with pd.ExcelWriter(excel_path, mode = 'a', if_sheet_exists = 'overlay') as writer:
    excel.to_excel(writer, sheet_name = 'Jobs', index = False)
    print("Updated", excel_path)