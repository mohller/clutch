import os
from numpy import genfromtxt
from subprocess import run, PIPE, Popen
from time import time, sleep

# Record starting time
starting_time = time()

def check_queue(username=''):
    """Determine the amount of running jobs based on the number
    of lines from calling squeue
    """
    sq_out = run(f'squeue -hu {username}', stdout=PIPE, shell=True)

    running_processes = sq_out.stdout.split('\n'.encode('utf-8'))

    return running_processes

def collect_nodes(username=''):
    """Return the nodes used, collecting them from the info returned 
    by squeue
    """
    sq_out = run(f'squeue -hu {username}', stdout=PIPE, shell=True)
    response_lines = sq_out.stdout.split('\n'.encode('utf-8'))
    nodes = set([rl.split()[-1].decode('ascii') for rl in response_lines[:-1]])
    
    return nodes

def update_nodes(nodes):
    """Save currently used nodes
    """
    with open('nodes_used.txt', 'w') as nodes_record:
        for nodename in nodes:
            nodes_record.write(nodename)
            nodes_record.write('\n')

def vigilante(username='', sleeptime=60, lifetime=3600, **kwargs):
    """Utility function to keep submitting jobs on a cluster.
       This script is intended to checkup on the running jobs, call
       for post-processing scripts, and re-submit jobs that did not
       complete successfully or crashed.
       
       Most importantly, to overcome the time limitation of a the queue
       in the cluster
       
       Arguments:
       -----------------
       - username: Your username in the submission cluster
       
       - sleeptime: The time (in seconds) to wait between checkups of 
        the running jobs

       - lifetime: The time (in seconds) an instance of vigilante is
       supposed to last. Reccommended: the time limit of the queue reduced
       by some margin (1-5 min).

       - post_processing_command: A string containing the command 
        used to post-process the output files, e.g. a call for a script 
        to perform this task. 

       - completion_check_command: A string containing the command 
        used to check that recent jobs have completed correctly.
        This can also be a call for another script that performs this task.

       - resubmission_command: A string containing the command 
        used when calling this script (i.e. vigilante.py)
       
       Use wisely!
    """
    print('Elapsed time:', time() - starting_time)

    # Storing nodes employed in case of cleanup needed
    if os.path.exists('nodes_used.txt'):
        nodes = set(genfromtxt('nodes_used.txt', dtype=str))
    else:
        nodes = set()
    
    while time() - starting_time < lifetime:
        running_processes = check_queue(username)
        print(running_processes)

        if len(running_processes) > 2:
            current_nodes = collect_nodes(username)

            if current_nodes.difference(nodes) != set():
                nodes = nodes.union(current_nodes)
                update_nodes(nodes)

            sleep(sleeptime)
            print(f'Slept {sleeptime}s, time to recheck!\n')
            continue

        if 'post_processing_command' in kwargs:
            # Call processing script
            post_proc_call = kwargs['post_processing_command']
            run(post_proc_call, stdout=PIPE, shell=True)

        if 'completion_check_command' in kwargs:
            # Check completed jobs
            done_check_call = kwargs['completion_check_command']
            run(done_check_call, stdout=PIPE, shell=True)

        if 'resubmission_command' in kwargs:
            # Resubmit uncompleted jobs
            resubmission_call = kwargs['resubmission_command']
            run(resubmission_call, stdout=PIPE, shell=True)

    # Save list of used nodes
    update_nodes(nodes)
            
    # Lifetime reached, call next instance of vigilante before stopping
    if 'resurrection_command' in kwargs:
        print('Calling the next guy now!')
        resurrection_script = kwargs['resurrection_command']
        Popen(resurrection_script, stdout=PIPE, shell=True, close_fds=True)

    sleep(5) # needed to allow time for the execution of resurrection_script
    return

if __name__ == "__main__":
    import sys
    print('starting vigilante')

    vigilante(runtype=sys.argv[1])
