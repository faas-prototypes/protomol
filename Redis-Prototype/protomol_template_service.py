import redis_connector as redis_connector
from collections import OrderedDict
import protomol_utils as protomol_utils
import random
import json

DEFAULT_BOUNDARY_CONDITIONS = "Vacuum"



def save_protomol_template(output_path, pdb_file, psf_file, par_file, monte_carlo_step, md_steps, output_freq,
                           replica_obj, generate_xyz = False, generate_dcd = False):

    protomol_config_map = OrderedDict()

    # Parse supplied files so only actual file name is passed, not full path of the file name
    input_pdb = "%s.%d-%d.pdb" % (protomol_utils.remove_trailing_dots(protomol_utils.parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step)
    parsed_psf_file = protomol_utils.parse_file_name(psf_file)
    parsed_par_file = protomol_utils.parse_file_name(par_file)
    str_output_freq = str(output_freq)
    str_md_steps = str(md_steps)
    protomol_config_map["randomtype"] = "1"
    protomol_config_map["numsteps"] = str_md_steps
    protomol_config_map["outputfreq"] = str_output_freq
    protomol_config_map["posfile"] = input_pdb
    protomol_config_map["psffile"] = parsed_psf_file
    protomol_config_map["parfile"] = parsed_par_file

    if monte_carlo_step > 0:
        protomol_config_map["velfile"] = "%s.%d-%d.vel" % (
            protomol_utils.remove_trailing_dots(protomol_utils.parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step)

    protomol_config_map["dofinPDBPosFile"] = "true"
    protomol_config_map["finPDBPosFile"] = "%s.%d-%d.pdb" % (
        protomol_utils.remove_trailing_dots(protomol_utils.parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step + 1)
    protomol_config_map["finXYZVelFile"] = "%s.%d-%d.vel" % (
        protomol_utils.remove_trailing_dots(protomol_utils.parse_file_name(pdb_file)), replica_obj.id, monte_carlo_step + 1)

    protomol_config_map["temperature"] = "%f" % replica_obj.temp
    protomol_config_map["boundaryConditions"] = "%s" % DEFAULT_BOUNDARY_CONDITIONS

    protomol_config_map["cellManager"] = "Cubic"
    protomol_config_map["cellsize"] = "69"

    if generate_xyz:
        protomol_config_map["XYZPosFile"] = "%d.xyz" % replica_obj.id
        protomol_config_map["XYZPosFileOutputFreq"] = "%d" % str_md_steps
    if generate_dcd:
        protomol_config_map["DCDFile"] = "%d.dcd" % replica_obj.id
        protomol_config_map["DCDFileOutputFreq"] = str_output_freq

    protomol_config_map["allEnergiesFile"] = "%d.eng" % replica_obj.id
    protomol_config_map["allEnergiesFileOutputFreq"] = str_output_freq

    protomol_config_map["seed"] = str(random.randint(1, 1000000))
    protomol_config_map["shake"] = "on"


    langevin_impulse_dictionary = OrderedDict()
    langevin_impulse_dictionary["temperature"] = str(replica_obj.temp)
    langevin_impulse_dictionary["gamma"] = "5"
    langevin_impulse_dictionary["timestep"] = "2"
    langevin_impulse_dictionary["force"] = ["bond","angle","dihedral","improper","LennardJones Coulomb"]
    langevin_impulse_dictionary["-switchingFunction"] = ["C2","C1","-algorithm NonbondedCutoff"]
    langevin_impulse_dictionary["-switchon"] = "10"
    langevin_impulse_dictionary["-cutoff"] = ["12","12","12"]

    integrator_dictionary = OrderedDict()
    integrator_dictionary["level"] = "0"
    integrator_dictionary["langevinImpulse"] = langevin_impulse_dictionary

    protomol_config_map["integrator"] = integrator_dictionary


    cfg_file_name = "%s/%s/%s/%d/%d-%d.cfg" % (
    output_path, "simfiles", "config", replica_obj.id, replica_obj.id, monte_carlo_step)
    serialized_object = json.dumps(protomol_config_map)
    redis_connector.save_file_to_redis(cfg_file_name, serialized_object)

    return cfg_file_name


def get_protomol_template_as_file(key):

    protomol_template_file = OrderedDict(json.loads(redis_connector.get_value(key)))
    write_str = build_file(protomol_template_file)
    return write_str


def build_file(properties):
    write_str=""
    for property, value in properties.items():

        if (type(value) == dict):
           write_str += property + " "
           write_str += "{\n"
           write_str += build_file(value)
           write_str += "\n}"

        else:
            if(type(value) == list):

                if (property == "force" or "-cutoff" == property):
                    for element in value:
                        write_str += property + " " + element + "\n"
                else:
                    if ("switchingFunction" in  property):
                        for element in value:
                            if element == "-algorithm":
                                write_str += " " + value
                            else:
                                write_str += property + " " + element + " "
                write_str += "\n"
            else:
                write_str += property + " " + value + "\n"

    return  write_str

