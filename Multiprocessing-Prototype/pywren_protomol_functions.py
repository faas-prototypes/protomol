#!/usr/bin/env cctools_python
# CCTOOLS_PYTHON_VERSION 2.7 2.6
import random
import math
import os


#-------------------------------Constants-----------------------------------
DEFAULT_MONTE_CARLO_STEPS = 10
DEFAULT_OUTPUT_PATH = os.getcwd()
DEFAULT_MDSTEPS = 100
DEFAULT_BOUNDARY_CONDITIONS = "Vacuum"
DEFAULT_OUTPUT_FREQ = 100
DEFAULT_PHYSICAL_TEMP = 300

EXECUTABLE = "ProtoMol"
#EXECUTABLE = "$PROTOMOL"

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


#Function to generate a config file to send to workqueue. It returns the generated config file name.
def generate_config(output_path, pdb_file, psf_file, par_file, monte_carlo_step, md_steps, output_freq, replica_obj, generate_xyz = False, generate_dcd = False):

    #initialize the config file name based on the replica id.
    print (os.getcwd())
    dirName = "%s/%s/%s/%d" % ( output_path, "simfiles", "config", replica_obj.id)
    if not os.path.exists(dirName):
        os.makedirs(dirName, exist_ok=True) 
        print("Directory " , dirName ,  " Created ")
    cfg_file_name = "%s/%s/%s/%d/%d-%d.cfg" % ( output_path, "simfiles", "config", replica_obj.id, replica_obj.id, monte_carlo_step)
    cfg_file_stream = open(cfg_file_name, "w")

    #initialize string that will hold the config file values
    write_str = ""

    #Parse supplied files so only actual file name is passed, not full path of the file name
    input_pdb = "%s.%d-%d.pdb" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step)
    parsed_psf_file = parse_file_name(psf_file)
    parsed_par_file = parse_file_name(par_file)

    #Start writing the config file parameters and values
    write_str += "randomtype 1\n"
    write_str += "numsteps %d\n" % md_steps
    write_str += "outputfreq %d\n" % output_freq

    write_str += "posfile %s\n" % input_pdb
    write_str += "psffile %s\n" % parsed_psf_file
    write_str += "parfile %s\n" % parsed_par_file

    if monte_carlo_step > 0:
        write_str += "velfile %s.%d-%d.vel\n" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step)

    write_str += "dofinPDBPosFile true\n"
    write_str += "finPDBPosFile %s.%d-%d.pdb\n" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step+1)
    write_str += "finXYZVelFile %s.%d-%d.vel\n" % (remove_trailing_dots(parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step+1)

    write_str += "temperature %f\n" % replica_obj.temp
    write_str += "boundaryConditions %s\n" % boundary_conditions
    write_str += "cellManager Cubic\n"
    write_str += "cellsize 69\n"

    if generate_xyz:
        write_str += "XYZPosFile %d.xyz\n" % replica_obj.id
        write_str += "XYZPosFileOutputFreq %d\n" % md_steps
    if generate_dcd:
        write_str += "DCDFile %d.dcd\n" % replica_obj.id
        write_str += "DCDFileOutputFreq %d\n" % output_freq

    write_str += "allEnergiesFile %d.eng\n" % replica_obj.id
    write_str += "allEnergiesFileOutputFreq %d\n" % output_freq

    write_str += "seed %d\n" % random.randint(1, 1000000)
    write_str += "shake on\n"

    write_str += "integrator {\n"
    write_str += "level 0 langevinImpulse {\n"
    write_str += "temperature %f\n" % replica_obj.temp
    write_str += "gamma 5\n"
    write_str += "timestep 2\n"
    write_str += "force bond\n"
    write_str += "force angle\n"
    write_str += "force dihedral\n"
    write_str += "force improper\n"
    write_str += "force LennardJones Coulomb\n"
    write_str += " -switchingFunction C2 -switchingFunction C1 -algorithm NonbondedCutoff\n"
    write_str += "   -switchon          10\n"
    write_str += "   -cutoff            12\n"
    write_str += "   -cutoff            12\n"
    write_str += "   -cutoff            12\n"
    write_str += "}\n}"

    #Write to the config file
    cfg_file_stream.write(write_str)
    print ("File name is : {}".format(cfg_file_name))
    return cfg_file_name


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
