# 1. Introduction

CC-Daemon is an automated service to handle the launching, monitoring, and reporting of GAP pipeline jobs defined in a MySQL database. This README is designed to show users how to load and launch the GAP-Daemon on the Google Cloud environment. 

# 2. Setting up CC-Daemon on Google Compute Engine (GCE)
Much like a web server, CC-Daemon was designed to run indefinitely and silently provide it's services as long as it's live. This section describes how to setup a virtual machine (VM) on GCE capable of hosting CC-Daemon.

## 2.1 VM requirements
CC-Daemon requires a VM with at least 2 CPUs, 7.5GB RAM. The smaller the better as the runtime footprint of the daemon is relatively small. CC Daemon was designed to be run on the "cc-runner-image" disk image. It hasn't been tested on any other DaveLab disk images and probably--no definitely--won't work on any others.

## 2.2 Installing dependencies
### SQLAlchemy v1.2
	
    sudo pip install SQLAlchemy

### PyMySQL

	sudo pip install PyMySQL

### Installing MySQL

	sudo apt-get update
    sudo apt-get -y install mysql-server
    

## 2.3 Cloning CC-Daemon from GitLab
Once you've initialized your VM and can connect via SSH, the following steps gather necessary files in order to run the daemon. First, you'll need to clone the CC-Daemon git repository from GitLab. 

	sudo git clone --recursive git@gitlab.oit.duke.edu:davelab/CC-Daemon.git

If that doesn't work, you may need to make sure the instance's ssh key is registered with the gitlab project. Generate an ssh key if needed:

    ssh-keygen
    
And get the public key:

    cat .ssh/id_rsa.pub
     
Then register that public key under the user > settings > ssh_keys associated with your profile on GitLab.
You should now be able to clone the repository. 

The sudo command is necessary here because we've authenticated the root ssh-key with GitLab. Make sure you see the CC-Daemon repository:

	ls -l
If it worked you'll see something like this:     
		
        drwxr-xr-x 6 root            root            4096 Jan 19 23:07 CC-Daemon
Inside the CC-Daemon folder should look something like this:

		ls -l ./CC-Daemon
        
        -rw-r--r-- 1 root root  4259 Jan 19 23:07 CancelPipeline.py
		drwxr-xr-x 2 root root  4096 Jan 19 23:07 Config
		-rwxr-xr-x 1 root root 11348 Jan 19 23:07 cc-daemon
		drwxr-xr-x 6 root root  4096 Jan 19 23:07 CCDaemon
		drwxr-xr-x 2 root root  4096 Jan 19 23:07 Google
		-rw-r--r-- 1 root root  6066 Jan 19 23:07 ResizeQueue.py
		-rw-r--r-- 1 root root  5701 Jan 19 23:07 RunDaemon.py
Go ahead and change the permissions on the entire folder.

		sudo chmod -R 755 ./CC-Daemon
## 2.4 Transferring necessary config files to instance
CC-Daemon requires a config file and a Google Cloud authentication key file in order to run. These need to be transferred onto your instance. 

		# Transfer CC-Daemon config
		gsutil cp gs://davelab_data/CC_Daemon/cc_daemon_clouddb.config ~/
        # Transfer Dave lab access key file
        gsutil cp gs://davelab_data/CC_Daemon/cc_gcloud_key.json ~/
 
 Now, you'll have to modify the config file so that it actually points to the key file you just transferred to the instance. 
 
 		[platform]
        	zone = us-east1-c
        	nr_cpus = 2
        	mem = 8
        	disk_image = cc-runner-image
        	service_account_key_file = /new/path/to/key/file
IMPORTANT NOTE 1: In the previous code snippet, make sure to insert the actual full path to the key file.
IMPORTANT NOTE 2: DO NOT change the config in any other way unless you're REALLY, REALLY sure what you're doing.


# 3. Running CC-Daemon

Starting the daemon:

	~/CC-Daemon/cc-daemon start /path/to/config Google

Checking the daemon status:

	~/CC-Daemon/cc-daemon status

Stopping the daemon:

	~/CC-Daemon/cc-daemon stop
    
Viewing the current log:

	~/CC-Daemon/cc-daemon viewlog <num-lines>
    
Cancelling a currently running pipeline:

	~/CC-Daemon/cc-daemon cancel-pipeline <pipeline-id>

Resize the current pipeline queue:

	~/CC-Daemon/cc-daemon resize-queue [options]
    