# CLUTCH

Utilities to deploy scripts on computing clusters managed by job scheduling software (e.g. Slurm, PBS, etc.)

## General info

Deploying scripts on a computing cluster requires preparing additional code to manage the run, creating batch submission files, creating preparation and cleanup 
scripts, etc. All of these tasks are dependent on the job scheduling software and thus not scalable. Therefore, some applications are not possible, such as reusing
the submission scripts between different clusters or simultaneous submission to multiple clusters with different job schedulers.
