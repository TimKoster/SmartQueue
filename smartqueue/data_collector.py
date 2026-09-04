from pathlib import Path
import glob
import re

def scrape_results(calculation_directory, job_id):
    """
    Master function for scraping results from the calculation directory

    :param Path calculation_directory: Directory where the input file and results are located
    :param int job_id: The job_id of the finished job, in case you need to identify results by the job_id if there are multiple
    :return results: Dictionairy with (key, result), for example (formation_energy, 15,000kcal/mol)
    :return seperate_results: Returns a result Dictionairy (file extension, string of results) that needs to be put into a seperate file
    """
    
    results = dict()
    seperate_results = dict()

    results["status"] = "finished"

    # Scrape the slurm file for completion status
    try:
        results = scrape_slurm(calculation_directory, job_id, results)
    except:
        results["slurm_scraper"] = "Error detected, check slurm output for strack trace"

    try:
        results = scrape_log(calculation_directory, job_id, results)
    except:
        results["log_scraper"] = "Error detected, check slurm output for strack trace"
    
    # Scrape the .xyz for putting it in its own seperate file
    try:
        xyz_content = scrape_xyz(calculation_directory)
        if xyz_content: seperate_results["xyz"] = xyz_content
    except:
        results["xyz_scraper"] = "Error detected, check slurm output for strack trace"
        
    return results, seperate_results

# Scrape the slurm file for info on the calculation
def scrape_slurm(calculation_directory, job_id, results):
    slurm_list = glob.glob('slurm*.out', root_dir = calculation_directory)
    # If there's a slurm, we're not queued anymore
    for slurm in slurm_list:
        # There can be multiple slurms in the calculations directory, so only grab the one with our job_id
        # I doubt it ever really happens but let's just not do it wrong if it's easily avoided
        if str(job_id) not in str(slurm):
            continue
        slurm_path = Path(calculation_directory).joinpath(slurm)
        
        # This is all just for checking the state of the job. It sucks a bit but it's not otherwise reported
        # Would really suck if they changed the formatting or people used keywords such as CANCELLED and NORMAL_TERMINATION in their jobs files
        # Check if we terminated normally, otherwise we FAILED
        with open(slurm_path, "r") as slurm_file:
            slurm_content = slurm_file.read()
            if "NORMAL TERMINATION with errors" in slurm_content:
                results["TERMINATION:"] = "NORMAL TERMINATION with errors"
                results["status"] = "failed"
            elif "NORMAL TERMINATION" in slurm_content:
                results["TERMINATION:"] = "NORMAL TERMINATION"
            elif "CANCELLED AT"in slurm_content:
                results["TERMINATION"] = "CANCELLED"
                results["status"] = "cancelled"
            else:
                results["TERMINATION:"] = "UNKNOWN"
                results["status"] = "failed?"
        break

    return results

# Scrape the log file for energies and stuff
def scrape_log(calculation_directory, job_id, results):
    log_list = glob.glob('*.log', root_dir = calculation_directory)
    if len(log_list) > 0:
        log_path = Path(calculation_directory).joinpath(log_list[0])
        with open(log_path, "r") as log_file:
            raw_text = log_file.read()
            # I'm not even gonna pretend to know how regex works, thanks chatgpt. 
            # Nvm chatgpt lied and I've had to learn regex to fix it. the damage is incalculable
            m = list(re.finditer(r"ENERGY OF FORMATION", raw_text))[-1]
            subtext = raw_text[m.end():]
            if len(subtext) > 0:
                m2 = re.search(r"([-+]?[0-9]*\.?[0-9]+)\s*KCAL/MOL", subtext, flags=re.IGNORECASE)
                energy_kcal = float(m2.group(1))
            results["energy_out"] = energy_kcal
    return results

# Grab the contents of the .xyz file
def scrape_xyz(calculation_directory):
    xyz_list = glob.glob('*.xyz', root_dir = calculation_directory)
    if len(xyz_list) > 0:
        xyz_path = Path(calculation_directory).joinpath(xyz_list[0])
        with open(xyz_path, "r") as xyz_file:
            return xyz_file.read()
            