from subprocess import call
from dataclasses import dataclass
import re
import pandas as pd

SECONDS_IN_A_DAY = 86400
SIM_NUM_DAYS = 15
STATE_SIZE = 16

@dataclass
class Trace:
    p: list[int]
    ptilde: list[int]
    q: list[int]
    r: list[int]

@dataclass
class ExpConfig:
    simulator: str
    plat_file: str
    backfilling_flag: str
    description: str
    estimated: bool

@dataclass
class Policies:
    names: list[str]
    flags: list[str]
    size: int

def read_swf(filepath):
    p = []
    ptilde = []
    q = []
    r = []

    with open(filepath, 'r') as f:
        for line in f:
            row = re.split(" +", line.lstrip(" "))
            if row[0].startswith(';'):
                continue
            if int(row[4]) > 0 and int(row[3]) > 0 and int(row[8]) > 0:
                p.append(int(row[3]))
                ptilde.append(int(row[8]))
                q.append(int(row[4]))
                r.append(int(row[1]))

    trace = Trace(p, ptilde, q, r)
    return trace

def get_exec_command(configs, policy_flag, number_of_tasks):
    simulator = configs.simulator
    plat_file = configs.plat_file
    backfilling = configs.backfilling_flag
    cmd = "./" + simulator + " " + "DATA/xmls/plat_day.xml" + " " + plat_file + " " \
        + backfilling + policy_flag + "-nt" + " " + str(number_of_tasks)
    
    return cmd


def perform_experiments(num_exp, trace, trace_name, policies, conditions):
    """
    Perform scheduling expriments using different policies based on specific a 
    trace and conditions.
    """
    # Extracting the details of the experiment
    config = conditions.description
    use_estimated = conditions.estimated

    # Policy information 
    policies_name = policies.names
    policies_flags = policies.flags
    number_of_policies = policies.size

    print(f'Performing scheduling performance test for the workload trace {trace_name}.\n'+
     f'Configuration: {config}')

    # DataFrame to store all slowdowns from all experiments
    slowdowns = pd.DataFrame(columns=policies_name)

    choose = 0
    for exp in range(num_exp):
        # Initial informations
        task_file = open("initial-simulation-submit.csv", "w+")
        earliest_submit = trace.r[choose]

        # Collect S set
        p_s = []
        ptilde_s = []
        q_s = []
        r_s = []
        for idx in range(STATE_SIZE):
            p_s.append(trace.p[choose + idx])
            q_s.append(trace.q[choose + idx])
            r_s.append(trace.r[choose + idx] - earliest_submit)
            
            if use_estimated:
                ptilde_s.append(trace.ptilde[choose + idx])
                task_file.write(str(p_s[idx])+","+str(q_s[idx])+","+str(r_s[idx])+","+str(ptilde_s[idx])+"\n")
            else:
                task_file.write(str(p_s[idx])+","+str(q_s[idx])+","+str(r_s[idx])+"\n")

        # Collect Q set
        p_q = []
        ptilde_q = []
        q_q = []
        r_q = []
        idx = 0
        while trace.r[choose+STATE_SIZE+idx] - earliest_submit <= SECONDS_IN_A_DAY * SIM_NUM_DAYS:
            p_q.append(trace.p[STATE_SIZE+choose+idx])
            q_q.append(trace.q[STATE_SIZE+choose+idx])
            r_q.append(trace.r[STATE_SIZE+choose+idx] - earliest_submit)
            if use_estimated:
                ptilde_q.append(trace.ptilde[STATE_SIZE+choose+idx])
                task_file.write(str(p_q[idx])+","+str(q_q[idx])+","+str(r_q[idx])+","+str(ptilde_q[idx])+"\n")
            else:
                task_file.write(str(p_q[idx])+","+str(q_q[idx])+","+str(r_q[idx])+"\n")
            idx += 1
        task_file.close()
        choose += STATE_SIZE + idx

        number_of_tasks = int(len(p_s) + len(p_q)) # |M| = |S| + |Q|
        print(f"Performing scheduling experiment {exp}. Number of tasks={number_of_tasks}")

        # Simulator calls
        _buffer = open("plot-temp.dat", "w+")
        for i in range(number_of_policies):
            policy_flag = policies_flags[i]
            command = get_exec_command(conditions, policy_flag, number_of_tasks)
            call([command], shell=True, stdout=_buffer)
        _buffer.close()

        # Temporary DataFrame to store slowdowns from current experiment
        temp_data = pd.DataFrame(columns=policies_name)

        _buffer = open("plot-temp.dat", "r")
        lines = list(_buffer)
        for i in range(number_of_policies):
            policy_name = policies_name[i]
            temp_data[policy_name] = float(lines[i]) # store slowdown for each policy
        slowdowns = pd.concat([slowdowns, temp_data], ignore_index=True)
        _buffer.close()
    return slowdowns


def main():
    # == Possible policies (with flags) ==
    # "FCFS": ""; "SJF": "-spt"; "WFP3": "-wfp3"; "UNICEF": "-unicef";
    # "EASY": "-easy"; "F1": "-f1"; "F2": "-f2"; "F3": "-f3"; "F4": "-f4";
    # "LINEAR": "-linear"; "QUADRATIC": "-quadratic"; "CUBIC": "-cubic";
    # "QUARTIC": "-quartic"; "QUINTIC": "-quintic"; "SEXTIC": "-sextic"
    # "SAF": "-saf"
    #
    # == Possible traces (full path is DATA/swfs/<*.swf>) ==
    # "Lublin_256": "lublin_256.swf"; "estLublin_256": "lublin_256_est.swf";
    # "Lublin_1024": "lublin_1024.swf"; "estLublin_1024": "lublin_1024_est.swf";
    # "ANL": "ANL-Interpid-2009-1.swf"; "Curie": "CEA-Curie-2011-2.1-cln.swf";
    # "CTC_SP2": "CTC-SP2-1996-3.1-cln.swf"; "HPC2N": "HPC2N-2002-2.2-cln.swf";
    # "SDSC_BLUE": "SDSC-BLUE-2000-4.2-cln.swf"; "SDSC_SP2": "SDSC-SP2-1998-4.2-cln.swf"
    #
    # == Possible configuration platforms (full path is DATA/xmls/<*.xml>) ==
    # "deployment_day.xml"; "deployment_day_1024.xml"; "deployment_anl.xml";
    # "deployment_curie.xml"; "deployment_ctcsp2.xml"; "deployment_hpc2n.xml";
    # "deployment_blue.xml"; "deployment_sdscsp2.xml"

    # Trace and Experiments
    filepath = "DATA/swfs/lublin_256.swf"
    trace = read_swf(filepath)
    number_of_experiments = 50
    trace_name = "Lublin_256"
    

    simulator = "sched-simulator-runtime"
    # simulator = "sched-simulator-estimate-backfilling"
    plat_file = "DATA/xmls/deployment_day.xml"
    exp_config = ExpConfig(
        simulator, plat_file,
        backfilling_flag="", description="runtimes", estimated=False
        )
    
    # Policies
    policies_list = [
        "FCFS", "SAF", "LINEAR", "QUADRATIC", "CUBIC",
        "QUARTIC", "QUINTIC", "SEXTIC"
        ]
    policies_flags = [
        "", "-saf ", "-linear ", "-quadratic ", "-cubic ",
        "-quartic ", "-quintic ", "-sextic "
    ]
    size = len(policies_list)

    policies = Policies(policies_list, policies_flags, size)

    slowdowns = perform_experiments(
        number_of_experiments, trace, trace_name, policies, exp_config)
    
    slowdowns.to_csv(trace_name + "_" + exp_config.description + ".csv", index=False)

if __name__ == "__main__":
    main()