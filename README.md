# Replica Exchange with Serverless

## Introduction

This project is a reimplementation of [Replica Exchange with Workqueue](https://github.com/cooperative-computing-lab/cctools/tree/master/apps/wq_replica_exchange) making use of different Serverless frameworks such as [IBM-PyWren](https://github.com/pywren/pywren-ibm-cloud) and [Multiprocessing API](https://github.com/cloudbutton/cloudbutton). In addition to this, we present different prototypes which making use of [IBM-PyWren](https://github.com/pywren/pywren-ibm-cloud) with the goal to analyze the performance and the scalability of each one.

## Environment setup

1.	Download and install [cctools library](https://cctools.readthedocs.io/en/latest/install/) for your platform.
2.  Run ```pip install -r requirements.txt``` to download the required libraries.
3.  Modify the resources/default_config.yml file and change its parameters to point to your IBM-Cloud configuration.
4.  Set the PYWREN_CONFIG_FILE environment variable and point it to  resources/default_config.yml absolute path.
5.  In the folder Redis-Prototype, modify the configuration code block placed  in `redis_connector.py` file and change these parameters to point it to your Redis.

If you want to run these prototypes with the local executor provided by IBM-PyWren, you need to make available the ProtoMol file for the environment making an installing of this or referencing it through an environment variable.
## Run Simulation

**Isolated Execution**

If you want to run an isolated execution of Protein folding code, go to the prototype desired folder and execute `pywren_replica_exchange.py` file as follows:

```
    python3.6 pywren_replica_exchange.py ww_exteq_nowater1.pdb ww_exteq_nowater1.psf par_all27_prot_lipid.inp  <MIN TEMPERATURE> <MAX TEMPERATURE>  <NUMBER OF REPLICAS>

```

**Run a Set of Executions**

If you want to run a set of consecutive executions, you should do the following:

```  
    chmod +x execution.sh 
    ./execution.sh <PROTOTYPE NAME> <ITERATIONS> <MIN TEMPERATURE> <MAX TEMPERATURE>  <NUMBER OF REPLICAS>     
```

**Example**
```

    ./execution.sh LocalDictionary 5 300 400 12
    
```

The simulation output will be written in the corresponding .txt file inside the folder of the prototype selected.
