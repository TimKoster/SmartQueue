## SmartQueue

A smarter queueing system for the [Slurm Workload Manager](https://slurm.schedmd.com/overview.html).

1. Tracks the state and progress of all jobs, drastically improving efficiency and clarity.
2. Allows for partitioning of jobs, giving complete control over the queue when dealing with limited SLURM runners.
3. Automatically gathers relevant results using [TCMU](https://github.com/TheoChem-VU/TCMU). Only applies to [Amsterdam Modeling Suite](https://www.scm.com/amsterdam-modeling-suite/) jobs.

eqbatch

- Usage: eqbatch <script_path> <node_partition A/B/C/etc...> [display_name]
- Submit a slurm job at script_path with a specified node_partition and an optional display display_name

eqcancel

- Usage: eqcancel <job_id>
- Cancels a running job and moves it to the finished "queue"

eqconfig

- Usage: eqconfig <A/B-/ALL/...> [A/B/ALL/...] ...
- Updates the external queue runner config
- The order in the eqconfig is also the order by which jobs are pulled from the queue
- Adding a '-' after the partition ('A-') will grab an 'ALL' job if there are no jobs for the 'A' partition.
- Accepts partition string for: 8 runners

eqclear

- Usage: eqclear <job_id>
- Clears a finished job from the finished queue
- You can also safely delete the .job files from the finished/ folder (or the entire folder) to bulk remove jobs

equpdate

- Check progress of every job, and submit new jobs if they are finished
- Jobs will automatically update themselves and pull in new jobs when they are finished,
- so you do not need to call this. However, jobs cancelled by time or not added by eqbatch will NOT
- pull in new jobs, meaning you might need to call this.
- Note that the below 'eq' commands will also update all jobs if a discrepency is noticed

eq

- Usage: eq [-a for all, -f for finished, -r for running and -q for queued jobs] [-s to not update]
- Can show the running, queued and finished jobs
- Will internally call equpdate if a discrepency is noticed in running jobs. This can be supressed with -s

a
- Show all jobs (running, queued and finished)

r
- Show all running jobs

f
- Show all finished job

q
- Show all queued jobs

Partitions:

- Controls which jobs are allowed to run. By default, every 'open slot' is set to ALL
- You can change this with the 'eqconfig' command, for example: 'eq GEO GEO TSRC'. This will reserve two nodes for jobs with the partition 'GEO' and one with the partition 'TSRC'. The other 5 runners can pick any job from the queue, you do not need to specify 'ALL'. Note that 'GEO' jobs can also run in 'ALL' partitions if they 'GEO' runners are full and it happens to be next. If there are no 'GEO' or 'TSRC' jobs, those three runners will remain empty.
- Using 'eq GEO-' will try to reserve 1 runner for 'GEO' partition jobs, but if there are none it will pull in any job.

Viewing results:

- Results are stored in the finished/ folder, in for example the corresponding .job .xyz files.
- Results are also printed to overview.xlsx. Colums and rows can be moved freely. This can be used to rapidly export results to, for example, a personal bookkeeping system.

Technical details:

- A copy of the slurm input script is submitted (affixed with '_') with the instruction to call the queue_controller.py, which is how SmartQueue knows when to pull in new jobs. This means that cancelled jobs will not run this update, nor will jobs not submitted with eqbatch.
- All jobs are stored in either the queue/, running/ or finished/ folder. Editing or deleting these is supported, with the exception of running jobs.
