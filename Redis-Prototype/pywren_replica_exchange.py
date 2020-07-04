#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6

# replica_exchange.py
#
# Copyright (C) 2011- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#
# This program implements elastic replica exchange using
# the cctools work queue framework and the Protomol molecular
# dynamics package, as described in the following paper:
#
# Dinesh Rajan, Anthony Canino, Jesus A Izaguirre, and Douglas Thain,
# "Converting a High Performance Application to an Elastic Cloud Application",
# The 3rd IEEE International Conference on Cloud Computing Technology
# and Science", November 2011.



# Get ProtoMol related bindings from protomol_functions.
import  pywren_protomol_functions as pywren_protomol


# All other dependencies are standard python.
import time
import math
import random
import sys
import os
import yaml
import shutil
import pywren_ibm_cloud as pywren
import logging
import getopt
import protomol_template_service as template_service

logging.basicConfig(level=logging.DEBUG)

if sys.version_info[0] >= 3:
    unicode = str

import cos_utils as cos

#-------------------------------Global Variables----------------------------
protomol_local_install = False
use_barrier = True #False
generate_xyz = False
generate_dcd = False
debug_mode = True
quart_temp_split = False
mc_step_times = []

#------------------------------Global Data--------------------------------------
replica_id = None
proj_name = None
replicas_running = 0

mac_temp_dir = '/Users/gilv/Dev/tmp/exec'
cf_temp_dir = '/tmp'

local_temp_dir = cf_temp_dir  
#------------------------Stat Collection Variables------------------------------
num_replica_exchanges = 0
total_monte_carlo_step_time = 0
num_task_resubmissions = 0
replica_temp_execution_list = []
replica_exch_list = []
step_time = 0
total_functions_executed = 0
execution_time_per_function = []
#--------------------------------Program Meat-----------------------------------

#Function to drop repetetive values and gather just the unique ones.
def unique(inlist, keep_val=True):
    typ = type(inlist)
    if not typ == list:
        inlist = list(inlist)
    i = 0
    while i < len(inlist):
        try:
            del inlist[inlist.index(inlist[i], i + 1)]
        except:
            i += 1
    if not typ in (str, unicode):
        inlist = type(inlist)
    else:
        if keep_val:
            inlist = ''.join(inlist)
    return inlist


#Function to check if a given executable exists in PATH.
def locate(executable):
    def check_executable(prog):
        return os.path.exists(prog) and os.access(prog, os.X_OK)

    for path in os.environ["PATH"].split(os.pathsep):
        exe = os.path.join(path, executable)
        if check_executable(exe):
            return exe

    return None


#Function to generate execution script that runs simulations of a replica
# over given number of steps
def generate_execn_script(replica_obj, replica_next_starting_step, replica_next_ending_step):
    #assign script file name based on the replica id.
    execn_script_name = "%s/%s/exec-%d.sh" % (pywren_protomol.output_path, "simfiles", replica_obj.id)

    #execn_script_stream = open(execn_script_name, "w")

    #check if protomol comes installed on the remote worker site.
    if protomol_local_install:
        execn_string = "%s" % pywren_protomol.EXECUTABLE
    else:
        execn_string = "./%s" % pywren_protomol.EXECUTABLE
    #initialize string that will hold the file strings.
    write_str = ""

    #write protomol execution commands for steps to be run for this replica.
    write_str += "#!/bin/sh\n\n"
    for i in range(replica_next_starting_step, replica_next_ending_step+1):
        write_str += "%s %d-%d.cfg\n" % (execn_string, replica_obj.id, i)

    #Write to file
    #execn_script_stream.write(write_str)
    #execn_script_stream.close()

    #Make file executable
    #os.chmod(execn_script_name, 0o755)
    #print("write_str {}".format(write_str))
    #print("execn_script_name {}".format(execn_script_name))

    return [execn_script_name, write_str]



#Create a new WorkQueue pywren_task.

def create_wq_task(replica_id, temp_dir, bucket):
    #Task string will be running the execution script.
    task_str = "./exec-%d.sh" % replica_id
    #Create a pywren_task using given pywren_task string for remote worker to execute.
    task = pywren_protomol.Task(task_str, temp_dir, bucket)
    task.specify_tag('%s' % replica_id)
    print ("Generate new task id {}".format(task.tag))

    return task

#Assign all the input files for the pywren_task (replica).
def assign_task_output_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step):
    #Find pdb file for current replica
    replica_pdb = "%s.%d" % (pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)), replica_id)

    #Assign local and remote xyz output files.
    if generate_xyz:
        local_xyz_output_file = "%s/simfiles/%s/%s.%d-%d.xyz" % (pywren_protomol.output_path, replica_list[replica_id].temp, pywren_protomol.xyz_file_name, replica_id, replica_next_ending_step)
        remote_xyz_output_file = "%d.xyz" % (replica_id)
        task.specify_output_file_xyz(remote_xyz_output_file, local_xyz_output_file)

    #Assign local and remote dcd output files.
    if generate_dcd:
        local_dcd_output_file = "%s/simfiles/%s/%s.%d-%d.dcd" % (pywren_protomol.output_path, replica_list[replica_id].temp, pywren_protomol.dcd_file_name, replica_id, replica_next_ending_step)
        remote_dcd_output_file = "%d.dcd" % (replica_id)
        task.specify_output_file_dcd(remote_dcd_output_file, local_dcd_output_file)

    #Assign local and remote (output) energies files.
    local_energies_file = "%s/simfiles/eng/%d/%d.eng" % (pywren_protomol.output_path, replica_id, replica_id)
    remote_energies_file = "%d.eng" % replica_id
    task.specify_output_file_energy(local_energies_file, remote_energies_file, cache=False)

    #Assign local and remote velocity output files.
    local_velocity_output_file = "%s/simfiles/%s/%s-%d.vel" % (pywren_protomol.output_path, replica_list[replica_id].temp, replica_pdb, replica_next_ending_step+1)
    remote_velocity_output_file = "%s-%d.vel" % (replica_pdb, replica_next_ending_step+1)
    task.specify_output_file_velocity(local_velocity_output_file, remote_velocity_output_file, cache=False)

    pdb_output_file = "%s/simfiles/%s/%s-%d.pdb" % (pywren_protomol.output_path, replica_list[replica_id].temp, replica_pdb, replica_next_ending_step+1)
    task.specify_output_file_pdb(pdb_output_file, pywren_protomol.parse_file_name(pdb_output_file), cache=False)


#Assign all the output files for the pywren_task (replica).
def assign_task_input_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step):
    #Find pdb file for current replica
    replica_pdb = "%s.%d" % (pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)), replica_id)

    #Find pdb file for replica that exchanged with current replica in last step
    if (replica_list[replica_id].exchgd_replica_id > -1):
        exchgd_replica_pdb = "%s.%d" % (pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)), replica_list[replica_id].exchgd_replica_id)
    else:
        exchgd_replica_pdb = "%s.%d" % (pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)), replica_id)

    '''Local_file: name for file brought back and stored on local site where this is run.
       Remote_file: name for file sent to remote worker and used in execution there.'''
    #Assign local and remote execution scripts
    local_execn_file = "%s/simfiles/%s/exec-%d.sh" % (pywren_protomol.output_path, "runs", replica_id)
    remote_execn_file = "exec-%d.sh" % (replica_id)
    task.specify_input_local_execn_file(local_execn_file, remote_execn_file, cache=False)

    #Assign local and remote pdb inputs
    local_pdb_input_file = "/simfiles/%s/%s.pdb" % (pywren_protomol.output_path, pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)))
    remote_pdb_input_file = "%s-%d.pdb" % (replica_pdb, replica_next_starting_step)
    task.specify_input_pdb_file(local_pdb_input_file, remote_pdb_input_file, cache=False)

    #Velocity input only required after first step since it is output
    #of first step.
    if (replica_next_starting_step > 0):
        #Assign local and remote velocity input files.
        local_velocity_input_file = "%s/simfiles/%s/%s-%d.vel" % (pywren_protomol.output_path, replica_list[replica_id].temp, exchgd_replica_pdb, replica_next_starting_step)
        remote_velocity_input_file = "%s-%d.vel" % (replica_pdb, replica_next_starting_step)
        task.specify_input_file_velocity(local_velocity_input_file, remote_velocity_input_file, cache=False)

    for i in range(replica_next_starting_step, replica_next_ending_step+1):
        #Assign local and remote config files.
        local_config_file = "%s/simfiles/config/%d/%d-%d.cfg" % (pywren_protomol.output_path, replica_id, replica_id, i)
        remote_config_file = "%d-%d.cfg" % (replica_id, i)
        task.specify_input_file(i, local_config_file, remote_config_file, cache=False)

    #Call function to generate execution script.
    [execn_script_name, execn_script] = generate_execn_script(replica_list[replica_id], replica_next_starting_step, replica_next_ending_step)
    print (execn_script)
    task.execn_script = execn_script
    '''
    if upload_data:
        target_key = "%s/simfiles/runs/%s" % (pywren_protomol.output_path, pywren_protomol.remove_first_dots(pywren_protomol.parse_file_name(execn_script_name)))
        cos.upload_bytes_to_cos(ibm_cos, str.encode(execn_script), input_config['ibm_cos']['bucket'], target_key)
    '''        

    #Assign executable that will be run on remote worker to pywren_task string.
    if not protomol_local_install:
        local_executable = "%s" % (pywren_protomol.EXECUTABLE)
        task.specify_executable(local_executable)


# Major replica exchange and scheduling is handled here
def cf_main(ibm_cos, bucket, replica_list, replicas_to_run):

    #Stat collection variables
    global replicas_running
    global step_time
    global total_functions_executed

    #Variable that tracks replicas which completed simulations over all MC steps
    num_replicas_completed = 0

    #-------Perform computation for each replica at current monte carlo step--------
    '''Each computation is a pywren_task in work queue.
       Each pywren_task will be run on one of the connected workers.'''
        #Assign local and remote psf and par inputs
    target_psf_file = "%s/simfiles/input_data/ww_exteq_nowater1.psf"%(pywren_protomol.output_path)
    if upload_data:
        cos.upload_to_cos(ibm_cos, psf_file, input_config['ibm_cos']['bucket'], target_psf_file)
    
    target_par_file = "%s/simfiles/input_data/par_all27_prot_lipid.inp"%(pywren_protomol.output_path)
    if upload_data:
        cos.upload_to_cos(ibm_cos, par_file, input_config['ibm_cos']['bucket'], target_par_file)


    while num_replicas_completed < len(replica_list):

        pw = pywren.ibm_cf_executor(runtime='cactusone/pywren-protomol:3.6.14', runtime_memory=2048)
        #pw = pywren.local_executor()
        print ("num_replicas_completed: {}".format(num_replicas_completed))
        print ("len(replica_list): {}".format(len(replica_list)))

        #Iterate through the given set of replicas and start their
        #         computation for the current monte carlo step.
        activation_list = []
        task_list_iterdata = []
        for j in replicas_to_run:
            if not replica_list[j].running:
                #Initialize step time.
                step_time = time.time()

                replica_id = replica_list[j].id
                #Each replica does computation at its current temperature
                replica_temperature = replica_list[j].temp

                '''Get the last seen step of replica. The last_seen_step
                is the step at which this replica was brought back and
                attempted for an exchange.'''
                replica_last_seen_step = replica_list[j].last_seen_step

                #record the starting, ending steps for current iteration of
                #this replica.
                replica_next_starting_step = replica_last_seen_step + 1
                if replica_next_starting_step >= pywren_protomol.monte_carlo_steps:
                    break

                if use_barrier:
                    #Barrier version, so run one step at a time.
                    replica_next_ending_step = replica_next_starting_step
                else:
                    #Run all steps until the step where the replica will be
                    #chosen to attempt an exchange.
                    if len(replica_list[j].exch_steps) > 0:
                        replica_next_ending_step = replica_list[j].exch_steps[0]
                    #If there are no more exchange steps for this replica, run the
                    #remainder of monte carlo steps.
                    else:
                        replica_next_ending_step = pywren_protomol.monte_carlo_steps-1

                #Set the last_seen_step to the next exchange step at which the
                #replica (its output) will be brought back.
                replica_list[j].last_seen_step = replica_next_ending_step

                task = create_wq_task(replica_id, local_temp_dir, bucket)
                task.specify_input_psf_file(target_psf_file, pywren_protomol.parse_file_name(psf_file))
                task.specify_input_par_file(target_par_file, pywren_protomol.parse_file_name(par_file))
                
                assign_task_input_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step)
                assign_task_output_files(task, replica_list, replica_id, replica_next_starting_step, replica_next_ending_step)

                #Keep count of replicas that iterated through all MC steps.
                if (replica_next_ending_step == pywren_protomol.monte_carlo_steps-1):
                    num_replicas_completed += 1

                #Submit the pywren_task to WorkQueue for execution at remote worker.
                print ("wq.submit(pywren_task): {}".format(task))
                task_dictionary = {}
                task_dictionary['task'] = task
                task_dictionary['protomol_file_template_key'] = task.input_conf_file[0][1]
                task_dictionary['time_per_function'] = 0
                task_list_iterdata.append(task_dictionary)

                #Submitted for execution. So mark this replica as running.
                replica_list[j].running = 1
                replicas_running += 1

        #Wait for tasks to complete.
        total_functions_executed+=len(task_list_iterdata)
        pw.map(serverless_task_process, task_list_iterdata)
        activation_list = pw.get_result()
        for j in range(len(activation_list)):
            replica_list[j].running = 0
            replicas_running -=1
            execution_time_per_function.append(activation_list[j].function_time)


        '''
        for task in task_list_iterdata:
            res = serverless_task_process(task, ibm_cos)
            activation_list.append(res)
        '''    
        if use_barrier:
            replicas_to_run=wq_wait_barrier(activation_list, replica_list, bucket, replica_next_starting_step, 5)
        else:
            replicas_to_run=wq_wait_nobarrier(activation_list, bucket, replica_list, 5)
        activation_list = []

'''The barrier version where it waits for all replicas to finish a given MC step.
   Returns all the replicas that it waited for.'''
def wq_wait_barrier(activation_list, replica_list, bucket, monte_carlo_step, timeout):

    #Stat collection variables
    print ("wq_wait_barrier")
    global step_time
    global num_task_resubmissions
    global total_monte_carlo_step_time
    global replicas_running

    #Initialize list that contains replicas that will be returned to run next.
    replica_to_run = []

    #Wait for all replicas to finish execution,
    for task in activation_list:
        #Get replica id from finished pywren_task.
        replica_id = int(task.tag)

        # Check if pywren_task (replica) failed. If so, resubmit.
        if task.result != 0:
            num_task_resubmissions += 1
            print ("Replica failed!")
            time.sleep(3)
            #Resubmit the pywren_task.
            continue

        #Task was succesful. Update run information.
        #replicas_running -= 1
        #replica_list[replica_id].running = 0

        #Get potential energy value of the completed replica run.
        #energies_file =  "%s/simfiles/eng/%d/%d.eng" % (pywren_protomol.output_path, replica_id, replica_id)
        #energies_stream =  ibm_cos.get_object(Bucket = bucket, Key = pywren_protomol.remove_first_dots(energies_file))['Body']._raw_stream 
        #open(energies_file, "r")
        #line = energies_stream.readline()
        line = task.energy_stream
        print (line)
        #print (task.energy_stream)
        #print ("***")
        slist = (line).split()            
        potential_energy = float(slist[1])
        replica_list[replica_id].potential_energy = potential_energy

        #Store temperature and exchanged replica id values from the current run.
        replica_list[replica_id].prev_temp = replica_list[replica_id].temp
        replica_list[replica_id].exchgd_replica_id = replica_id

        #Add this replica to return list.
        replica_to_run.append(replica_id)

    #Get replica exchange pair for current step and attempt exchange.
    cur = replica_exch_list[monte_carlo_step].pop(0)
    next = replica_exch_list[monte_carlo_step].pop(0)
    if debug_mode:
        print ("Replicas {} & {} are attempted for an exchange at step {}".format(cur, next, monte_carlo_step))

    #Attempt exchange between the two.
    attempt_replica_exch(replica_list, cur, next)

    #Update time stats for this MC step.
    step_time = time.time() - step_time
    total_monte_carlo_step_time += step_time
    mc_step_times.append(step_time)

    return replica_to_run


'''The nobarrier version where it receives a finished replica, waits for its
   exchange partner to finish, attempts an exchange between the two, and continues
   waiting for the rest similarly.
   Returns the replica pair that finished and was attempted for an exchange.'''
def wq_wait_nobarrier(activation_list, bucket, replica_list, timeout):

    print ("wq_wait_nobarrier")
    #Stat collection variables
    global num_task_resubmissions
    global replicas_running

    #Wait for a pywren_task to finish execution.
    print ("wq_wait_nobarrier {}".format(len(activation_list)))
    for task in activation_list:
        #pywren_task = res.get_result()
        print ("wq_wait_nobarrier. got task {}".format(task.task_str))
        if (task):
            #Get replica id from finished pywren_task.
            replica_id = int(task.tag)
            print ("wq_wait_nobarrier. task id {}".format(replica_id))

            #Check if pywren_task (replica) failed. If so, resubmit.
            if task.result != 0:
                num_task_resubmissions += 1
                print ("Replica failed!")
                #time.sleep(3)

                #Resubmit the pywren_task.
                #wq.submit(pywren_task)
                continue

            #Task was succesful. Update run information.
            #replicas_running -= 1
            #replica_list[replica_id].running = 0

            #Get potential energy value of the completed replica run.
            energies_file =  "%s/simfiles/eng/%d/%d.eng" % (pywren_protomol.output_path, replica_id, replica_id)
            energies_stream =  ibm_cos.get_object(Bucket = bucket, Key = pywren_protomol.remove_first_dots(energies_file))['Body']._raw_stream 
            #open(energies_file, "r")
            line = energies_stream.readline()
            print (line)
            slist = (line).split()
            potential_energy = float(slist[1])
            replica_list[replica_id].potential_energy = potential_energy

            #Store temperature and exchanged replica id values from the current run.
            replica_list[replica_id].prev_temp = replica_list[replica_id].temp
            replica_list[replica_id].exchgd_replica_id = replica_id

            #Replica should be currently at this step which is its exchange step.
            if len(replica_list[replica_id].exch_steps) > 0:
                replica_exch_step = replica_list[replica_id].exch_steps.pop(0)
            #Else replica is at the last MC step of this run.
            else:
                replica_exch_step = pywren_protomol.monte_carlo_steps - 1

            #Find the exchange partner of this replica.
            if (replica_id == replica_exch_list[replica_exch_step][0]):
                replica_exch_partner = replica_exch_list[replica_exch_step][1]
            elif (replica_id == replica_exch_list[replica_exch_step][1]):
                replica_exch_partner = replica_exch_list[replica_exch_step][0]
            else:
                if (replica_exch_step != (pywren_protomol.monte_carlo_steps-1)):
                    #If this replica is not part of the exchange pair for this
                    #step and is not at the last MC step of the run, something
                    #is amiss..
                    print ("Replica {} should not be here at step {}".format(replica_id, replica_exch_step))
                    sys.exit(1)
                else:
                    #If all replicas have completed last MC step, return.
                    if replicas_running == 0:
                        print ("all replicas running 0. return")
                        return
                    #If not, loop back to receive other replicas.
                    else:
                        print ("loop to get more replicas")
                        continue

            #If exchange partner is still running, go back to get other tasks.
            if replica_list[replica_exch_partner].running:
                print ("Go back to get other tasks")
                continue
            #Otherwise check if partner has finished the current exchange step.
            else:
                if (replica_list[replica_exch_partner].last_seen_step < replica_exch_step):
                    #Exchange partner is currently behind the exchange step of
                    #this replica. So loop back to get other tasks.
                    print("here")
                    continue
                elif (replica_list[replica_exch_partner].last_seen_step > replica_exch_step):
                    #Should never get here. Something went wrong.
                    print ("Partner of replica {} - replica %d is currently at step {} which is beyond step {}".format(replica_id, replica_exch_partner, replica_list[replica_exch_partner].exch_steps[0], replica_exch_step))
                    sys.exit(1)
                else:
                    #Make sure the replicas are checked in the same order they were chosen at the start.
                    if (replica_exch_partner == replica_exch_list[replica_exch_step][0]):
                        replica_1 = replica_exch_partner
                        replica_2 = replica_id
                    else:
                        replica_1 = replica_id
                        replica_2 = replica_exch_partner

                    if debug_mode:
                        print ("Replicas {} & {} are attempted for an exchange at step {}".format(replica_1, replica_2, replica_exch_step))

                    #Attempt exchange between the two.
                    attempt_replica_exch(replica_list, replica_1, replica_2)

                    #Add these two replicas to return list.
                    replicas_to_run = [replica_1, replica_2]
                    print (replicas_to_run)

                    return replicas_to_run


#Check if two replicas satisfy the criteria to undergo an exchange.
def attempt_replica_exch(replica_list, replica1, replica2):
    global num_replica_exchanges

    #Check for metropolis criteria.
    if (pywren_protomol.metropolis(replica_list[replica1].potential_energy, replica_list[replica2].potential_energy, replica_list[replica1].temp, replica_list[replica2].temp)):
        #Swap fields of the two replicas being exchanged.
        T = replica_list[replica2].temp
        replica_list[replica2].temp = replica_list[replica1].temp
        replica_list[replica1].temp = T

        replica_list[replica1].exchgd_replica_id = replica_list[replica2].id
        replica_list[replica2].exchgd_replica_id = replica_list[replica1].id

        replica_temp_execution_list[replica1].append(replica_list[replica1].temp)
        replica_temp_execution_list[replica2].append(replica_list[replica2].temp)

        if debug_mode:
            print ("Replicas {} and {} exchanged".format(replica1, replica2))

        #Keep count of exchanges.
        num_replica_exchanges += 1


#Function to create directories to hold files from the simulations.
def make_directories(ibm_cos, bucket, output_path, temp_list, num_replicas):
    count = 0
    for i in temp_list:

        target_key = "%s/simfiles/%s/%s.%d-%d.pdb" % (output_path, i, pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)), count, 0)
        cos.upload_to_cos(ibm_cos, pdb_file, bucket, target_key)
        
        count += 1


#Function to determine the replica exchange pairs that will be attempted for an
#exchange at each MC step.
def create_replica_exch_pairs(replica_list, num_replicas):
    #Compute random pair (replica, neighbor) for each step to attempt exchange.
    for i in range(pywren_protomol.monte_carlo_steps):
        replica_1 = random.randint(0, num_replicas-1)
        replica_2 = replica_1 + 1

        if (replica_2 == num_replicas):
            replica_2 = replica_1 - 1

        #List that stores replicas attempted for exchange at each step.
        replica_exch_list.append([])
        replica_exch_list[i].append(replica_1)
        replica_exch_list[i].append(replica_2)

        #Store the steps at which each replica will be attempted for exchange.
        replica_list[replica_1].exch_steps.append(i)
        replica_list[replica_2].exch_steps.append(i)

        if debug_mode:
            print ("For step {}, exchange will be attempted for replica {} and {}.".format(i, replica_1, replica_2))

def serverless_task_process(task, protomol_file_template_key,time_per_function,ibm_cos):
    # get input files
    # execute local script
    # upload result files
    # return data
    #temp_dir = '/Users/gilv/Dev/tmp/exec' + '/' + pywren_task.tag
    time_per_function = time.time()
    temp_dir = task.temp_dir
    bucket = task.bucket
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)

    protomol_file = template_service.get_protomol_template_as_file(protomol_file_template_key)
    #exists = os.path.isfile(temp_dir + '/' +task.input_remote_execn_file)
    #if not exists:
    input_local_execn_file = pywren_protomol.remove_first_dots(task.input_local_execn_file)
    #res = ibm_cos.get_object(Bucket = bucket, Key = input_local_execn_file)
    with open(temp_dir + '/' +task.input_remote_execn_file, 'w') as localfile:
        localfile.write(task.execn_script)
    localfile.close()
        #shutil.copyfileobj(res['Body'], localfile)
    print ("local exec file is {}, remote {}".format(input_local_execn_file, task.input_remote_execn_file))
    
    input_local_file_pdb = pywren_protomol.remove_first_dots(task.input_local_file_pdb)
    res = ibm_cos.get_object(Bucket = bucket, Key = input_local_file_pdb)
    with open(temp_dir + '/' +task.input_remote_file_pdb, 'wb') as localfile:
        shutil.copyfileobj(res['Body'], localfile)
    
    input_par_file = pywren_protomol.remove_first_dots(task.input_par_file)
    res = ibm_cos.get_object(Bucket = bucket, Key = input_par_file)
    with open(temp_dir + '/' +task.input_par_file_name, 'wb') as localfile:
        shutil.copyfileobj(res['Body'], localfile)
    
    input_psf_file = pywren_protomol.remove_first_dots(task.input_psf_file)
    res = ibm_cos.get_object(Bucket = bucket, Key = input_psf_file)
    with open(temp_dir + '/' +task.input_psf_file_name, 'wb') as localfile:
        shutil.copyfileobj(res['Body'], localfile)

    if (task.input_local_file_velocity is not None):
        input_vel_file = pywren_protomol.remove_first_dots(task.input_local_file_velocity)
        print (input_vel_file)
        print (task.input_remote_file_velocity)
        res = ibm_cos.get_object(Bucket = bucket, Key = input_vel_file)
        with open(temp_dir + '/' +task.input_remote_file_velocity, 'wb') as localfile:
            shutil.copyfileobj(res['Body'], localfile)
    
    #bring all config files
    for conf_entry in task.input_conf_file:
        ind = conf_entry[0]
        remote_config = pywren_protomol.remove_first_dots(conf_entry[1])
        local_config = conf_entry[2]
        cached = conf_entry[3]
        with open(temp_dir + '/' + local_config, 'wb') as localfile:
            localfile.write(protomol_file.encode())

    import stat
    os.chmod(temp_dir + '/' + task.input_remote_execn_file, stat.S_IRUSR |
        stat.S_IWUSR |
        stat.S_IRWXG |
        stat.S_IRWXO |
        stat.S_IEXEC
        )
    os.chdir(temp_dir)
    arr = os.listdir()
    print(arr)
    cmd = "./" + task.input_remote_execn_file
    
    import subprocess
    subprocess.call(cmd, shell = True)
    
    #str: ./simfiles/eng/1/1.eng
    output_file_local_energy = task.output_file_local_energy
    #str: 1.eng
    output_file_remote_energy = task.output_file_remote_energy
    #cos.upload_to_cos(ibm_cos, temp_dir + '/' + output_file_remote_energy, 
    #              input_config['ibm_cos']['bucket'], pywren_protomol.remove_first_dots(output_file_local_energy))
    #read local energy file and update task
    energies_file =  temp_dir + '/' + output_file_remote_energy
    energies_stream =  open(energies_file, "r") 
    line = energies_stream.readline()
    task.update_energy(line)
    
    #str: ./simfiles/350.0/ww_exteq_nowater1.1-1.vel
    output_file_local_velocity = task.output_file_local_velocity
    #str: ww_exteq_nowater1.1-1.vel
    output_file_remote_velocity = task.output_file_remote_velocity
    cos.upload_to_cos(ibm_cos, temp_dir + '/' + output_file_remote_velocity, 
                  input_config['ibm_cos']['bucket'], pywren_protomol.remove_first_dots(output_file_local_velocity))
    
    #str: ./simfiles/350.0/ww_exteq_nowater1.1-1.pdb
    output_file_pdb = task.output_file_pdb
    #str: ww_exteq_nowater1.1-1.pdb
    output_file_pdb_name = task.output_file_pdb_name
    cos.upload_to_cos(ibm_cos, temp_dir + '/' + output_file_pdb_name, 
                  input_config['ibm_cos']['bucket'], pywren_protomol.remove_first_dots(output_file_pdb))
    task.result = 0
    time_per_function = time.time() - time_per_function
    task.specify_function_time(time_per_function)
    return task

#Main function.
if __name__ == "__main__":

    # Create help string for user.
    usage_str = "Usage: %s <PDB_FILE> <PSF_FILE> <PAR_FILE> <MIN_TEMP> <MAX_TEMP> <NUM_OF_REPLICAS>" % sys.argv[0]
    help_str = "-N		-	specify a project name for WQ master\n"
    help_str += "-x		-	specify the name of the xyz file for output\n"
    help_str += "-d		-	specify the name of the dcd file for output\n"
    help_str += "-m		-	specify the number of monte carlo steps\n"
    help_str += "-s		-	specify the number of mdsteps\n"
    help_str += "-p		-	specify the path for storing the output files\n"
    help_str += "-q		-	assign closer temperature values to the first and last quartile of the replicas.\n"
    help_str += "-i		-	assume ProtoMol is installed and available in PATH on worker site\n"
    help_str += "-b		-	use barrier in waiting for all replicas to finish their steps before attempting exchange.\n"
    help_str += "-l		-	print debuging information\n"
    help_str += "-h		-	help"

    # Check to see if there is error in the given command line arguments.
    try:
        opts, args = getopt.getopt(sys.argv[1:], "N:x:d:m:s:p:qiblh", ["help"])
    except getopt.GetoptError as err:
        print(str(err))
        print(usage_str)
        sys.exit(1)

    # Parse command line arguments.
    for o, a in opts:
        if o in ("-h", "--help"):
            print(usage_str)
            print(help_str)
            sys.exit(0)
        elif o == "-l":
            debug_mode = True
        elif o in ("-x"):
            generate_xyz = True
            xyz_file_name = a
        elif o in ("-d"):
            generate_dcd = True
            dcd_file_name = a
        elif o in ("-N"):
            proj_name = a
        elif o in ("-p"):
            output_path = a
        elif o in ("-m"):
            monte_carlo_steps = int(a)
        elif o in ("-s"):
            md_steps = int(a)
        elif o == "-q":
            quart_temp_split = True
        elif o == "-i":
            protomol_local_install = True
        elif o == "-b":
            use_barrier = True

    # Check for the 6 mandatory arguments.
    if len(args) != 6:
        print(usage_str)
        sys.exit(1)
    pdb_file = "./../resources/%s"% args[0]
    psf_file = "./../resources/%s"% args[1]
    par_file = "./../resources/%s"% args[2]
    min_temp = int(args[3])
    max_temp = int(args[4])
    num_replicas = int(args[5])

    upload_data = True
    os.environ['PYWREN_CONFIG_FILE'] = './../resources/default_config.yml'
    with open(os.environ['PYWREN_CONFIG_FILE']) as file:
        input_config = yaml.full_load(file)

    ibm_cos = cos.get_ibm_cos_client(input_config)
    print("Clean old data from COS - start")
    cos.clean_from_cos(input_config, input_config['ibm_cos']['bucket'], 'simfiles')
    print("Clean previous data from COS - completed")

    bucket = input_config['ibm_cos']['bucket']

    monte_carlo_steps = pywren_protomol.DEFAULT_MONTE_CARLO_STEPS

    total_run_time = time.time()
    # Split up the temperature range for assigning to each replica.
    inc = float((max_temp - min_temp)) / float(num_replicas - 1)
    print ("number of replicas {}".format(num_replicas))
    replica_list = []
    temp_list = []
    #Assign temperature to each replica.
    for x in range(num_replicas):
        #Quart split assigns closer temperature values
        #    to the top and bottom 25% of replicas.
        if quart_temp_split:
            if x < math.ceil(0.25 * num_replicas):
                replica_temp =  min_temp + (x * inc / 3)

            elif x >= math.ceil(0.75 * num_replicas):
                replica_temp =  max_temp - (((num_replicas-1) - x) * inc / 3)

            else:
                replica_temp =  min_temp + (x * inc)

        #If not quart split, split temperature range uniformly
        #                    among all replicas.
        else:
            replica_temp =  min_temp + (x * inc)

        #Store the temperature values and replicas.
        temp_list.append(str(replica_temp))
        replica_list.append(pywren_protomol.Replica(x, replica_temp))

        #Initialize list for maintaining replica exchange matrix.
        replica_temp_execution_list.append([])
        replica_temp_execution_list[x].append(replica_list[x].temp)



    #Create directories for storing data from the run.
    if upload_data:
        # We upload just once the pdb_file, because hasn't sense to upload it for each temperature configuration
        target_key = "/simfiles/%s/%s.pdb" % (pywren_protomol.output_path, pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)))
        cos.upload_to_cos(ibm_cos, pdb_file, bucket, target_key)

    #Create random replica pairs to check for exchange at each step.

    create_replica_exch_pairs(replica_list, num_replicas)

    #create config files here.
    for i in range(pywren_protomol.monte_carlo_steps):
        for j in range(num_replicas):
            config_path = template_service.save_protomol_template(pywren_protomol.output_path, pdb_file, psf_file, par_file, i, pywren_protomol.md_steps, pywren_protomol.output_freq, replica_list[j])
    replicas_to_run = []
    for i in range(num_replicas):
        replicas_to_run.append(i)


    #Start the run.
    #wq = queue.Queue()
    #Begin timing after parsing command line arguments.

    cf_main(ibm_cos, bucket, replica_list, replicas_to_run)


    #Track total run time.
    total_run_time = (time.time() - total_run_time)

    #Print stats on completion.
    print ("Total Run Time:                  {}".format(total_run_time))
    print("Total Monte Carlo Step Time:     {}".format(total_monte_carlo_step_time))
    print("Average Monte Carlo Step Time:   {}".format(total_monte_carlo_step_time / pywren_protomol.monte_carlo_steps))
    print ("Number of failures:              {}".format(num_task_resubmissions))
    print ("Replica Exchanges:               {}".format(num_replica_exchanges))
    print ("Acceptance Rate:                 {}".format((num_replica_exchanges * 100) / pywren_protomol.monte_carlo_steps))
    print("Total functions executed :        {}".format(total_functions_executed))

    total_function_time = 0
    for value in execution_time_per_function:
        total_function_time+=value

    print("Average per function :            {}".format(total_function_time/len(execution_time_per_function)))

    #Write stats to a stats file
    stat_file_name = "%s/%s.stat" % (local_temp_dir,  pywren_protomol.remove_trailing_dots(pywren_protomol.parse_file_name(pdb_file)))
    stat_file_stream = open(stat_file_name, "w")

    stat_file_stream.write("%s\n" % "Printing replica temperature execution matrix:")
    #Sort and format the replica exchange matrix.
    for itr in range(num_replicas):
        replica_temp_execution_list[itr].sort()
        unique(replica_temp_execution_list[itr])
        stat_file_stream.write("Replica %d: %s\n" % (itr, replica_temp_execution_list[itr]))

    #If barrier version was used, write the MC step times to stats file.
    if use_barrier:
        #Write run time for each step to stats file
        stat_file_stream.write("\n\n%s\n" % "Printing run times for each monte carlo step:")
        count = 1
        for i in mc_step_times:
            stat_file_stream.write("%d %f\n" % (count, i))
            count += 1

    stat_file_stream.close()
    exit_file = open('with_redis_output.txt', 'a')
    exit_file.write('\n')
    exit_file.write(str(num_replicas))
    exit_file.write(',')
    exit_file.write(str(total_monte_carlo_step_time / monte_carlo_steps))
    exit_file.write(',')
    exit_file.write(str(total_run_time))
    exit_file.write(',')
    exit_file.write(str(num_task_resubmissions))
    exit_file.write(',')
    exit_file.write(str(num_replica_exchanges))
    exit_file.write(',')
    exit_file.write(str((num_replica_exchanges * 100) / monte_carlo_steps))
    sys.exit(0)