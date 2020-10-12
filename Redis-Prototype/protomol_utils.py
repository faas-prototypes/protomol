#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6
import random
import math
import os

#-------------------------------Constants-----------------------------------
DEFAULT_MONTE_CARLO_STEPS = 5
DEFAULT_OUTPUT_PATH = os.getcwd()
DEFAULT_MDSTEPS = 100
DEFAULT_BOUNDARY_CONDITIONS = "Vacuum"
DEFAULT_OUTPUT_FREQ = 100
DEFAULT_PHYSICAL_TEMP = 300

EXECUTABLE = "$PROTOMOL"

#-----------------------------Global Data-------------------------------------
pdb_file = ""
psf_file = ""
par_file = ""
xyz_file_name = ""
dcd_file_name = ""
boundary_conditions = DEFAULT_BOUNDARY_CONDITIONS
monte_carlo_steps = DEFAULT_MONTE_CARLO_STEPS
md_steps = DEFAULT_MDSTEPS
output_freq = DEFAULT_OUTPUT_FREQ
output_path = DEFAULT_OUTPUT_PATH

replica_list = []

#------------------------Initialize random generator----------------------------
random.seed()


#-------------------------Global functions---------------------------------
#Function to parse the file name from a string holding its location.
def parse_file_name(file_name):
    split_name = file_name.split('/')
    return split_name[len(split_name)-1]


#Function to parse the file name and leave out its extension.
def remove_trailing_dots(file_name):
    split_name = file_name.split('.')
    return split_name[0]

#Function to parse the file name and leave out its extension.
def remove_first_dots(file_name):
    if file_name.startswith("./"):
        return file_name[2:]
    return file_name

#-------------------------Define Replica Object---------------------------------
class Replica(object):
    def __init__(self, id, temp):
        self.id    = id
        self.temp = temp
        self.exchgd_replica_id = -1
        self.potential_energy = None
        self.prev_temp = None
        self.exch_steps = []
        self.running = 0
        self.last_seen_step = -1

    def __str__(self):
        return "Replica %d at temp %f" % (self.id, self.temp)

#Function that to perform metropolis criteria check for two replicas.
def metropolis( u_i, u_j, t_i, t_j ):
    # Metropolis for replica i with potential energy u_i, temp t_i
    #            and replica j with potential energy u_j, temp t_j
    K_b = 0.001987191 #Constants.boltzmann()
    deltaE = (1 / (K_b * t_i) - 1/ (K_b * t_j) ) - (u_j - u_i)

    if( deltaE < 0 ):
        return True

    acceptProb = math.exp(-deltaE)
    randNum = random.random()

    if( randNum < acceptProb ):
        return True
    else:
        return False
    
class Task(object):
    def __init__(self, task_str, temp_dir):
        self.function_time= None
        self.task_str = task_str
        self.input_conf_file = []
        self.result = 1
        self.temp_dir = temp_dir
        self.input_local_file_velocity = None
        self.energy_stream = None
        self.execn_script = None

    def specify_function_time(self,time):
        self.function_time = time

    def specify_output_file_xyz(self,remote, local, cache = True):
        self.output_file_local_xyz = local
        self.output_file_remote_xyz = remote

    def specify_output_file_dcd(self,remote, local, cache = True):
        self.output_file_local_dcd = local
        self.output_file_remote_dcd = remote

    def specify_output_file_energy(self,local, remote, cache = True):
        self.output_file_local_energy = local
        self.output_file_remote_energy = remote

    def specify_output_file_velocity(self,local, remote, cache = True):
        self.output_file_local_velocity = local
        self.output_file_remote_velocity = remote

    def specify_output_file_pdb(self,pdb_file, pdb_file_name, cache = True):
        self.output_file_pdb = pdb_file
        self.output_file_pdb_name = pdb_file_name
    
    def specify_tag(self, tag):
        self.tag = tag

    def specify_input_local_execn_file(self, local_execn_file, remote_execn_file, cache=False):
        self.input_local_execn_file = local_execn_file
        self.input_remote_execn_file = remote_execn_file
    
    def specify_input_psf_file(self, psf_file, file_name):
        self.input_psf_file = psf_file
        self.input_psf_file_name = file_name

    def specify_input_par_file(self, par_file, file_name):
        self.input_par_file = par_file
        self.input_par_file_name = file_name
        
    def specify_input_pdb_file(self,pdb_local_file, pdb_remote_file, cache = True):
        self.input_local_file_pdb = pdb_local_file
        self.input_remote_file_pdb = pdb_remote_file
            
    def specify_input_file_velocity(self,velocity_local_file, velocity_remote_file, cache = True):
        self.input_local_file_velocity = velocity_local_file
        self.input_remote_file_velocity = velocity_remote_file
        
    def specify_input_file(self,ind, local_config_file, remote_config_file, cache=False):
        self.input_conf_file.append([ind,local_config_file, remote_config_file, cache])
        
    def specify_executable(self, exec_file):
        self.exec_file = exec_file
        
    def update_energy(self, energy):
        self.energy_stream = energy    
        
                
    def __str__(self):
        return "task_str {}".format(self.task_str)
