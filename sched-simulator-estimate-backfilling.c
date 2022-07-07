/* This program is free software; you can redistribute it and/or modify it
 * under the terms of the license (GNU LGPL) which comes with this package. */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <string.h>
#include "simgrid/msg.h"            /* Yeah! If you want to use msg, you need to include msg/msg.h */
#include "xbt/sysdep.h"         /* calloc, printf */

/* Create a log channel to have nice outputs. */
#include "xbt/log.h"
#include "xbt/asserts.h"
XBT_LOG_NEW_DEFAULT_CATEGORY(msg_test,
                             "Messages specific for this msg example");

void backFill(double* runtimes, int* cores, int* submit, double* req, int* orig_pos, int policy, int queue_num_tasks, int num_tasks_disp);
void sortTasksQueue(double* runtimes, int* cores, int* submit, double* req, int* orig_pos, int policy, int queue_num_tasks, int num_tasks_disp);
const char* getfield(char* line, int num);
void readModelFile(void);
int master(int argc, char *argv[]);
int taskManager(int argc, char *argv[]);
msg_error_t test_all(const char *platform_file,
                     const char *application_file);

#define FINALIZE ((void*)221297)        /* a magic number to tell people to stop working */

#define MAX_TASKS 1024
#define WORKERS_PER_NODE 1
#define MAX_TASK_TYPES 5 
#define TERA 1e12
#define MEGA 1e6
#define TAO 10
#define QUEUE_NUM_TASKS 32
#define NUM_TASKS_STATE 16
#define EPSILON 1e-20

#define FCFS 0
#define SJF 1
#define WFP3 2
#define UNICEF 3
#define EASY 10
#define F4 6
#define F3 7
#define F2 8
#define F1 9
#define LINEAR 11
#define QUADRATIC 12
#define CUBIC 13
#define QUARTIC 14
#define QUINTIC 15
#define SEXTIC 16
#define SAF 17

int BF = 0;

int number_of_tasks = 0;


struct task_t{
int numNodes;
double startTime;
double endTime;
double submitTime;
double *task_comp_size;
double *task_comm_size;
msg_host_t* task_workers;
int *task_allocation;
};

//long task_comp_sizes[MAX_TASK_TYPES];
//long task_comm_sizes[MAX_TASK_TYPES];
//int num_tasks[10];
//int tasks_created[10];
//int seed;
int VERBOSE = 0;
int STATE = 0;

double* all_runtimes;
double* all_req_runtimes;
int* all_submit;
int* all_cores;

int* orig_task_positions;

//double** tasks_comp_sizes;
//double** tasks_comm_sizes;
//msg_host_t** tasks_workers;
//int** tasks_allocation;

double* slowdown;

struct task_t* task_queue = NULL;
msg_process_t p_master;

int chosen_policy = FCFS;
int* busy_workers;
int num_managers;
int number_of_nodes;

double* sched_task_placement;
//int** sched_task_types;
//int* tasks_per_worker;
//int number_of_tasks = QUEUE_NUM_TASKS + NUM_TASKS_STATE;
double t0 = 0.0f;

void backFill(double* runtimes, int* cores, int* submit, double* req, int* orig_pos, int policy, int queue_num_tasks, int num_tasks_disp){
  int i, j;
  int curr_time = MSG_get_clock();
  int num_arrived_tasks = 0;
  for(i = 0; i < queue_num_tasks; i++){
   if(submit[i] <= curr_time){
     num_arrived_tasks++;
   }else{
     break;
   }
  }
  //printf("%d ", num_arrived_tasks);
  if(num_arrived_tasks == 1)
    return;
 
    //double remaining_time[num_tasks_disp];
    double* remaining_time = (double*) calloc(num_tasks_disp, sizeof(double));
    for (i = 0; i < num_tasks_disp; i++) {
      if(task_queue[i].endTime == 0.0f){
        float task_elapsed_time = curr_time - task_queue[i].startTime;
        remaining_time[i] = all_req_runtimes[i] - task_elapsed_time;
      }else{
        remaining_time[i] = -1.0;
      }
    }    
    int available_nodes = 0;
    for (j = 0; j < number_of_nodes; j++) {
      if(busy_workers[j] == 0){
        available_nodes++; 
      }
    }
    int shadow_time = 0;
    int available_nodes_future = 0;
    int extra_nodes = 0;
    int min_remaining = INT_MAX;
    int min_remaining_task = 0;
    if(available_nodes < cores[0]){
    for (i = 0; i < num_tasks_disp; i++) {
      min_remaining = INT_MAX;     
      for (j = 0; j < num_tasks_disp; j++){
        if(remaining_time[j] != -1.0 && remaining_time[j] < min_remaining){
          min_remaining = remaining_time[j];
          min_remaining_task = j;
        }
      }        
        remaining_time[min_remaining_task] = INT_MAX;
        available_nodes_future+= all_cores[min_remaining_task];        
        if(available_nodes + available_nodes_future >= cores[0]){
          shadow_time = curr_time + min_remaining;
          extra_nodes = (available_nodes + available_nodes_future) - cores[0];
          break;
        }
      }
     }        
   for(i = 1; i < num_arrived_tasks;i++){       
     if((cores[i] <= available_nodes && (curr_time + req[i]) <= shadow_time) || (cores[i] <= (available_nodes < extra_nodes ? available_nodes : extra_nodes))){
       if(VERBOSE)
         XBT_INFO("\"Task_%d\" [r=%.1f,c=%d,s=%d,req=%.1f] Backfilled. Shadow Time=%d, Extra Nodes=%d.", orig_pos[i], runtimes[i], cores[i], submit[i], req[i], shadow_time, extra_nodes);
       double r_buffer = runtimes[i];
       int c_buffer = cores[i];
       int s_buffer = submit[i];
       double req_buffer = req[i];
       int p_buffer = orig_pos[i];
       for(j = i; j > 0;j--){
         runtimes[j] = runtimes[j-1];
         cores[j] = cores[j-1];
         submit[j] = submit[j-1];
         req[j] = req[j-1];
         orig_pos[j] = orig_pos[j-1];
       }
       runtimes[0] = r_buffer;
       cores[0] = c_buffer;
       submit[0] = s_buffer;
       req[0] = req_buffer;
       orig_pos[0] = p_buffer;
       break;
     }
   }
}

void sortTasksQueue(double* runtimes, int* cores, int* submit, double* req, int* orig_pos, int policy, int queue_num_tasks, int num_tasks_disp){
  int i, j;
  int curr_time = MSG_get_clock();
  int num_arrived_tasks = 0;
  for(i = 0; i < queue_num_tasks; i++){
   if(submit[i] <= curr_time){
     num_arrived_tasks++;
   }else{
     break;
   }
  }
  //printf("%d ", num_arrived_tasks);
  if(num_arrived_tasks == 1)
    return;
  if(policy == EASY){
    backFill(runtimes, cores, submit, req, orig_pos, policy, queue_num_tasks,  num_tasks_disp);
    return;  
  }
  if(policy == FCFS){
    if(BF)
      backFill(runtimes, cores, submit, req, orig_pos, policy, queue_num_tasks,  num_tasks_disp);
    return;
  }    
  if(policy == SJF){
    double r_buffer;    
    int c_buffer;
    int s_buffer;
    double req_buffer;
    int p_buffer;	
    for(i = 0; i < num_arrived_tasks; i++){
      for(j = 0; j < num_arrived_tasks; j++){
	    if(req[i] < req[j]){
		  r_buffer = runtimes[i];
		  c_buffer = cores[i];
                  s_buffer = submit[i];
                  req_buffer = req[i];
                  p_buffer = orig_pos[i];
		  runtimes[i] = runtimes[j];
		  cores[i] = cores[j];
                  submit[i] = submit[j];
                  req[i] = req[j];
                  orig_pos[i] = orig_pos[j];
		  runtimes[j] = r_buffer;
		  cores[j] = c_buffer;
                  submit[j] = s_buffer;
                  req[j] = req_buffer;
                  orig_pos[j] = p_buffer;
		}
	  }     	  
    }
    if(BF)
      backFill(runtimes, cores, submit, req, orig_pos, policy, queue_num_tasks,  num_tasks_disp);
	return;
  }
  double* h_values = (double*) calloc(num_arrived_tasks, sizeof(double));
  double* r_temp = (double*) calloc(num_arrived_tasks, sizeof(double));
  int* c_temp = (int*) calloc(num_arrived_tasks, sizeof(int));
  int* s_temp = (int*) calloc(num_arrived_tasks, sizeof(int));
  double* req_temp = (double*) calloc(num_arrived_tasks, sizeof(double));
  int* p_temp = (int*) calloc(num_arrived_tasks, sizeof(int));
  int max_arrive = 0;
  for(i = 0; i < num_arrived_tasks;i++){
    if(submit[i] > max_arrive){
      max_arrive = submit[i];
    }
  }  

  //printf("%d\n", max_arrive); 
  int task_age = 0;
  for(i = 0; i < num_arrived_tasks;i++){
    task_age = curr_time - submit[i]; //priority score for the arrival time (bigger = came first) "age" of the task
    double ptil_norm = req[i];
    double r_norm = submit[i];
    switch(policy){
      case 30:
	    h_values[i] = ((float)task_age/(float)req[i]) * cores[i];
            break;
      case WFP3:
	    h_values[i] = pow((float)task_age/(float)req[i], 3) * cores[i];
            break;
      case UNICEF:
	    h_values[i] = (task_age+EPSILON)/(log2((double)cores[i]+EPSILON) * req[i]);
	    break;      
      case F4:
          h_values[i] = (0.0056500287 * req[i]) * (0.0000024814 * sqrt(cores[i])) + (0.0074444355 * log10(submit[i])); //256nodes                   
          break;
      case F3:
           h_values[i] = (-0.2188093701 * req[i]) * (-0.0000000049 * cores[i]) + (0.0073580361 * log10(submit[i])); //256nodes 
          break;
      case F2: 
          h_values[i] = (0.0000342717 * sqrt(req[i])) * (0.0076562110 * cores[i]) + (0.0067364626 * log10(submit[i])); //256nodes
          break;
      case F1:
          h_values[i] = (-0.0155183403 * log10(req[i])) * (-0.0005149209 * cores[i]) + (0.0069596182 * log10(submit[i])); //256nodes
          break; 
      case LINEAR:
          // h_values[i] = 3.16688932E-02 + (1.24214915E-07 * ptil_norm) + (3.10222317E-05 * cores[i]) + (-1.62730301E-07 * r_norm);
          h_values[i] = 0.032247578568287236 \
          // p + q + r
          + (5.24248737e-04 * ptil_norm) + (1.61682411e-05 * cores[i]) + (-4.25473461e-04 * r_norm);
          break;
      case QUADRATIC:
          // h_values[i] = 3.59070690E-02 + (3.57653837E-07 * ptil_norm) + (-3.05321285E-05 * cores[i]) + (-4.32808767E-07 * r_norm) + (-3.68598108E-12 * pow(ptil_norm, 2)) + (1.46361083E-07 * pow(cores[i], 2)) + (2.94999301E-12 * pow(r_norm, 2)) + (4.79106778E-10*ptil_norm * cores[i]);
          h_values[i] = 0.034335696489395516 \
          // p + q + r
          + (5.49192941e-04 * ptil_norm) + (5.52856529e-06 * cores[i]) + (-9.87686123e-04 * r_norm) \
          // p2 + q2 + r2
          + (-9.81183659e-06 * pow(ptil_norm, 2)) + (1.06725323e-08 * pow(cores[i], 2)) + (2.35285729e-05 * pow(r_norm, 2)) \
          // pq
          + (1.70895013e-06 * ptil_norm*cores[i]);
          break;
      case CUBIC:
          // h_values[i] = 5.49939742e-02 + (-1.42358275e-06 * ptil_norm) + (-1.66172060e-04 * cores[i]) + (-7.32530172e-07 * r_norm) + (3.33805977e-11 * pow(ptil_norm, 2)) + (1.43074227e-07 * pow(cores[i], 2)) + (9.32866675e-12 * pow(r_norm, 2)) + (1.77599437e-08*ptil_norm * cores[i]) + (-1.64597448e-16 * pow(ptil_norm, 3)) + (1.01906798e-09 * pow(cores[i], 3)) + (-3.19724119e-17 * pow(r_norm, 3)) + (-2.60511394e-13*pow(ptil_norm, 2) * cores[i]) + (-4.07107551e-11*ptil_norm * pow(cores[i], 2)) + 5.83699122e-16*pow(ptil_norm*cores[i], 2);
          h_values[i] = 0.03781996606037328 \
          // p + q + r
          + (-5.09613078e-05 * ptil_norm) + (-2.34855993e-05 * cores[i]) + (-1.73214474e-03 * r_norm) \
          // p2 + q2 + r2
          + (2.02738340e-05 * pow(ptil_norm, 2)) + (1.08165338e-08 * pow(cores[i], 2)) + (8.48481749e-05 * pow(r_norm, 2)) \
          // pq
          + (1.23822857e-05 * ptil_norm*cores[i]) \
          // p3 + q3 + r3
          + (-2.68940831e-07 * pow(ptil_norm, 3)) + (1.76844551e-10 * pow(cores[i], 3)) + (-1.16392202e-06 * pow(r_norm, 3)) \
          // p2q + pq2
          + (-4.26793941e-07 * pow(ptil_norm, 2)*cores[i]) + (-1.60394373e-08 * ptil_norm*pow(cores[i], 2));
          break;
      case QUARTIC:
          // h_values[i] = 4.60676052e-02 + (-7.76765755e-07 * ptil_norm) + (-1.76313342e-05 * cores[i]) + (-6.47786293e-07 * r_norm) + (3.68129776e-11 * pow(ptil_norm, 2)) + (5.30588161e-07 * pow(cores[i], 2)) + (5.43172102e-12 * pow(r_norm, 2)) + (-2.86914325e-09*ptil_norm * cores[i]) + (-5.23476439e-16 * pow(ptil_norm, 3)) + (-6.68052736e-09 * pow(cores[i], 3)) + (2.74681557e-17 * pow(r_norm, 3)) + (7.14008267e-14*pow(ptil_norm, 2) * cores[i]) + (4.89098847e-11*ptil_norm * pow(cores[i], 2)) + -3.81658916e-16*pow(ptil_norm*cores[i], 2) + (2.29158387e-21 * pow(ptil_norm, 4)) + (1.79103008e-11 * pow(cores[i], 4)) + (-2.48090946e-22 * pow(r_norm, 3)) + (-8.60381994e-19*pow(ptil_norm, 3) * cores[i]) + (-1.02011767e-13*pow(cores[i], 3) * ptil_norm) + (6.21802436e-24*pow(ptil_norm * cores[i], 3));
          h_values[i] = 0.040430109789934825 \
          // p + q + r
          + (-2.10979902e-04 * ptil_norm) + (-1.01034613e-04 * cores[i]) + (-2.31932629e-03 * r_norm) \
          // p2 + q2 + r2
          + (2.04795625e-05 * pow(ptil_norm, 2)) + (1.31113039e-06 * pow(cores[i], 2)) + (1.65841430e-04 * pow(r_norm, 2)) \
          // pq
          + (1.14337285e-05 * ptil_norm*cores[i]) \
          // p3 + q3 + r3
          + (-1.40495599e-06 * pow(ptil_norm, 3)) + (-6.55426185e-09 * pow(cores[i], 3)) + (-4.92021540e-06 * pow(r_norm, 3)) \
          // p2q + pq2
          + (1.09258695e-06 * pow(ptil_norm, 2)*cores[i]) + (-9.41295917e-08 * ptil_norm*pow(cores[i], 2)) \
          // p4 + q4 + r4
          + (4.19440932e-08 * pow(ptil_norm, 4)) + (1.19265500e-11 * pow(cores[i], 4)) + (5.11827953e-08 * pow(r_norm, 4)) \
          // p3q + p2q2 + pq3
          + (-5.09427736e-08 * pow(ptil_norm, 3)*cores[i]) + (-3.25131488e-10 * pow(ptil_norm*cores[i], 2)) + (1.68809105e-10 * pow(cores[i], 3)*ptil_norm);
          break;
      case QUINTIC:
          // h_values[i] = 4.60676052e-02 + (-7.76765755e-07 * ptil_norm) + (-1.76313342e-05 * cores[i]) + (-6.47786293e-07 * r_norm) + (3.68129776e-11 * pow(ptil_norm, 2)) + (5.30588161e-07 * pow(cores[i], 2)) + (5.43172102e-12 * pow(r_norm, 2)) + (-2.86914325e-09*ptil_norm * cores[i]) + (-5.23476439e-16 * pow(ptil_norm, 3)) + (-6.68052736e-09 * pow(cores[i], 3)) + (2.74681557e-17 * pow(r_norm, 3)) + (7.14008267e-14*pow(ptil_norm, 2) * cores[i]) + (4.89098847e-11*ptil_norm * pow(cores[i], 2)) + -3.81658916e-16*pow(ptil_norm*cores[i], 2) + (2.29158387e-21 * pow(ptil_norm, 4)) + (1.79103008e-11 * pow(cores[i], 4)) + (-2.48090946e-22 * pow(r_norm, 3)) + (-8.60381994e-19*pow(ptil_norm, 3) * cores[i]) + (-1.02011767e-13*pow(cores[i], 3) * ptil_norm) + (6.21802436e-24*pow(ptil_norm * cores[i], 3));
          h_values[i] = 0.038988050583477096 \
          // p + q + r
          + (2.73452709e-04 * ptil_norm) + (7.69022102e-05 * cores[i]) + (-3.22249183e-03 * r_norm) \
          // p2 + q2 + r2
          + (6.43770345e-05 * pow(ptil_norm, 2)) + (-1.79747632e-06 * pow(cores[i], 2)) + (3.50127821e-04 * pow(r_norm, 2)) \
          // pq
          + (-1.23817261e-05 * ptil_norm*cores[i]) \
          // p3 + q3 + r3
          + (-2.88424111e-06 * pow(ptil_norm, 3)) + (5.91576673e-09 * pow(cores[i], 3)) + (-1.93318856e-05 * pow(r_norm, 3)) \
          // p2q + pq2
          + (-6.48604231e-06 * pow(ptil_norm, 2)*cores[i]) + (7.98548664e-07 * ptil_norm*pow(cores[i], 2)) \
          // p4 + q4 + r4
          + (-1.87753585e-07 * pow(ptil_norm, 4)) + (2.18593974e-11 * pow(cores[i], 4)) + (5.08486844e-07 * pow(r_norm, 4)) \
          // p3q + p2q2 + pq3
          + (6.42796628e-07 * pow(ptil_norm, 3)*cores[i]) + (1.98766825e-09 * pow(ptil_norm*cores[i], 2)) + (-5.08237988e-09 * pow(cores[i], 3)*ptil_norm)
          // p5 + q5 + r5
          + (7.56223729e-09 * pow(ptil_norm, 5)) + (-8.82511427e-14 * pow(cores[i], 5)) + (-4.90135508e-09 * pow(r_norm, 5)) \
          // p4q + p3q2
          + (-1.54514292e-08 * pow(ptil_norm, 4)*cores[i]) + (-7.38101914e-10 * pow(ptil_norm, 3)*pow(cores[i], 2)) \
          // p2q3 + pq4
          + (2.68544307e-11 * pow(ptil_norm, 2)*pow(cores[i], 3)) + (9.36081253e-12 * ptil_norm*pow(cores[i], 4));
          break;
      case SEXTIC:
          h_values[i] = 0.03716302646065289 \
          // p + q + r
          + (6.41521182e-04 * ptil_norm) + (4.26864527e-04 * cores[i]) + (-4.16425862e-03 * r_norm) \
          // p2 + q2 + r2
          + (1.49566455e-04 * pow(ptil_norm, 2)) + (-1.14347869e-05 * pow(cores[i], 2)) + (6.12884457e-04 * pow(r_norm, 2)) \
          // pq
          + (-9.59479573e-05 * ptil_norm*cores[i]) \
          // p3 + q3 + r3
          + (-3.54045251e-05 * pow(ptil_norm, 3)) + (1.39510813e-07 * pow(cores[i], 3)) + (-4.87573975e-05 * pow(r_norm, 3)) \
          // p2q + pq2
          + (1.13575542e-05 * pow(ptil_norm, 2)*cores[i]) + (1.51210086e-06 * ptil_norm*pow(cores[i], 2)) \
          // p4 + q4 + r4
          + (3.22956196e-06 * pow(ptil_norm, 4)) + (-9.25921216e-10 * pow(cores[i], 4)) + (2.01247903e-06 * pow(r_norm, 4)) \
          // p3q + p2q2 + pq3
          + (-1.22892966e-06 * pow(ptil_norm, 3)*cores[i]) + (-9.70097879e-08 * pow(ptil_norm*cores[i], 2)) + (-7.77024939e-09 * pow(cores[i], 3)*ptil_norm)
          // p5 + q5 + r5
          + (-1.34380105e-07 * pow(ptil_norm, 5)) + (3.16107259e-12 * pow(cores[i], 5)) + (-3.98682447e-08 * pow(r_norm, 5)) \
          // p4q + p3q2
          + (7.20792845e-08 * pow(ptil_norm, 4)*cores[i]) + (6.03178917e-09 * pow(ptil_norm, 3)*pow(cores[i], 2)) \
          // p2q3 + pq4
          + (2.29215937e-10 * pow(ptil_norm, 2)*pow(cores[i], 3)) + (1.41926779e-11 * ptil_norm*pow(cores[i], 4)) \
          // p6 + q6 + r6
          + (2.04292170e-09 * pow(ptil_norm, 6)) + (-4.24431834e-15 * pow(cores[i], 6)) + (2.99071239e-10 * pow(r_norm, 6)) \
          // p5q + p4q2
          + (-1.50403822e-09 * pow(ptil_norm, 5)*cores[i]) + (-1.34608366e-10 * pow(ptil_norm, 4)*pow(cores[i], 2)) \
          // p3q3 + p2q4
          + (-9.15186712e-12 * pow(ptil_norm, 3)*pow(cores[i], 3)) + (-4.41111039e-14 * pow(ptil_norm, 2)*pow(cores[i], 4)) \
          // pq5
          + (-4.79289535e-15 * ptil_norm*pow(cores[i], 5));
          break;
      case SAF:
          h_values[i] = req[i] * cores[i];
          break;
    }
  if(VERBOSE)
    XBT_INFO("Score for \"Task_%d\" [r=%.1f,c=%d,s=%d,est=%.1f]=%.7f", orig_pos[i], runtimes[i], cores[i], submit[i], req[i], h_values[i]);	
  }
if(policy == WFP3 || policy == UNICEF){
  double max_val = 0.0;
  int max_index;
  for(i = 0; i < num_arrived_tasks;i++){
	max_val = -1e20;  
    for(j = 0; j < num_arrived_tasks; j++){		
	  if(h_values[j] > max_val){
		max_val = h_values[j];
        max_index = j;	
	  }
	}
    r_temp[i] = runtimes[max_index];
    c_temp[i] = cores[max_index];
    s_temp[i] = submit[max_index];
    req_temp[i] = req[max_index];
    p_temp[i] = orig_pos[max_index];
    h_values[max_index] = -1e20;	
  }
}else if(policy >= F4){
  double min_val = 1e20;
  int min_index;
  for(i = 0; i < num_arrived_tasks;i++){
	min_val = 1e20;
        min_index = 0;
    for(j = 0; j < num_arrived_tasks; j++){		
	  if(h_values[j] < min_val){
		min_val = h_values[j];
        min_index = j;	
	  }
	}
    r_temp[i] = runtimes[min_index];
    c_temp[i] = cores[min_index];
    s_temp[i] = submit[min_index];
    req_temp[i] = req[min_index];
    p_temp[i] = orig_pos[min_index];
    h_values[min_index] = 1e20;	
  }
}
  for(i = 0; i < num_arrived_tasks;i++){
	runtimes[i] = r_temp[i];
	cores[i] = c_temp[i];
        submit[i] = s_temp[i];
        req[i] = req_temp[i];
        orig_pos[i] = p_temp[i];
  }
  if(BF)
    backFill(runtimes, cores, submit, req, orig_pos, policy, queue_num_tasks,  num_tasks_disp);
  free(r_temp);
  free(c_temp);
  free(s_temp);
  free(req_temp);
  free(p_temp);
  free(h_values);
}

const char* getfield(char* line, int num)
{
    const char* tok;
    for (tok = strtok(line, ",");
            tok && *tok;
            tok = strtok(NULL, ",\n"))
    {
        if (!--num)
            return tok;
    }
    return NULL;
}

void readModelFile(void){    

    all_runtimes = (double*) malloc((number_of_tasks) * sizeof(double));
    all_req_runtimes = (double*) malloc((number_of_tasks) * sizeof(double));
    all_submit = (int*) malloc((number_of_tasks) * sizeof(int));
    all_cores = (int*) malloc((number_of_tasks) * sizeof(int));
    int task_count=0;    
    
    //FILE* stream = fopen("workload-test.csv.old", "r");
    FILE* stream = fopen("initial-simulation-submit.csv", "r");

    char line[1024];
    while (fgets(line, 1024, stream))
    {
        char* tmp = strdup(line);             
        all_runtimes[task_count] = atof(getfield(tmp, 1)); ;
        free(tmp);
        tmp = strdup(line);        
        all_cores[task_count] = atoi(getfield(tmp, 2));;
        free(tmp);
        tmp = strdup(line);        
        all_submit[task_count] = atoi(getfield(tmp, 3));
        free(tmp);
        tmp = strdup(line);        
        all_req_runtimes[task_count] = atof(getfield(tmp, 4));
        free(tmp);
        // NOTE strtok clobbers tmp
        task_count++;      
    }
}

/** Emitter function  */
int master(int argc, char *argv[])
{
  int workers_count = 0;
  number_of_nodes = 0;
  msg_host_t *workers = NULL;
  msg_host_t task_manager = NULL;
  msg_task_t *todo = NULL;  
  //double task_comp_size = 0;
  //double task_comm_size = 0;

  int i;

  int res = sscanf(argv[1], "%d", &workers_count);
  xbt_assert(res,"Invalid argument %s\n", argv[1]);
  res = sscanf(argv[2], "%d", &number_of_nodes);
  xbt_assert(res, "Invalid argument %s\n", argv[2]);
  //res = sscanf(argv[3], "%lg", &task_comm_size);
  //xbt_assert(res, "Invalid argument %s\n", argv[3]);


  //workers_count = argc - 4;

readModelFile();
orig_task_positions = (int*) malloc((number_of_tasks) * sizeof(int));
int c = 0;
for(i = 0; i < number_of_tasks; i++){
  orig_task_positions[c++] = i;
}

/*  criacao das matrizes de saída */
//sched_task_placement = (double*) calloc((MAX_TASKS),sizeof(double));
//sched_task_types = (int**) malloc(workers_count*sizeof(int*));

//srand(seed);

p_master = MSG_process_self();
{                             /* Process organisation (workers) */  
    //int i;
    char sprintf_buffer[64];  
    workers = xbt_new0(msg_host_t, workers_count);

    for (i = 0; i < workers_count; i++) {
      sprintf(sprintf_buffer, "node-%d", (i+WORKERS_PER_NODE) / WORKERS_PER_NODE);
      workers[i] = MSG_get_host_by_name(sprintf_buffer);
      xbt_assert(workers[i] != NULL, "Unknown host %s. Stopping Now! ",
                  sprintf_buffer);
    }
}

{                             /* Process organisation (managers) */
    //num_managers = QUEUE_NUM_TASKS + NUM_TASKS_STATE;
    task_manager = MSG_get_host_by_name("node-0");
    xbt_assert(task_manager != NULL, "Unknown host %s. Stopping Now! ",
                    "node-0");    
}

if(VERBOSE)
XBT_INFO("Got %d processors and %d tasks to process", number_of_nodes, number_of_tasks);

{                             /*  Task creation */
    int j, k;

    todo = xbt_new0(msg_task_t, number_of_tasks);

    busy_workers = (int*) calloc(number_of_nodes, sizeof(int));
    task_queue = (struct task_t*) malloc(number_of_tasks * sizeof(struct task_t)); 
    //tasks_comp_sizes = (double**) malloc(number_of_tasks * sizeof(double*));
    //tasks_comm_sizes = (double**) malloc(number_of_tasks * sizeof(double*));
    //tasks_allocation = (int**) malloc(number_of_tasks * sizeof(int*));  
    //tasks_workers = xbt_new0(msg_host_t**, number_of_tasks);
    
    for (i = 0; i < number_of_tasks; i++) {      
      
      int available_nodes;  
      do{
          if(i >= NUM_TASKS_STATE){       
        sortTasksQueue(&all_runtimes[i], &all_cores[i], &all_submit[i], &all_req_runtimes[i], &orig_task_positions[i], chosen_policy, number_of_tasks - i >= QUEUE_NUM_TASKS ? QUEUE_NUM_TASKS : number_of_tasks - i, i);        
          }
        
        while(MSG_get_clock() < all_submit[i]){//task has not arrived yet 
          MSG_process_sleep(all_submit[i] - MSG_get_clock());
        }
        available_nodes = 0;
        for (j = 0; j < number_of_nodes; j++) {
          if(busy_workers[j] == 0){
            available_nodes++;            
            if(available_nodes == all_cores[i]){
              break;
            }
          }
        }
        //if(VERBOSE)            
        //  XBT_INFO("Available nodes=%d. Nodes needed=%d", available_nodes, all_cores[i]);
        if(available_nodes < all_cores[i]){
          if(VERBOSE)
            XBT_INFO("Insuficient workers for \"Task_%d\" [r=%.1f,c=%d,s=%d,est=%.1f] (%d available workers. need %d). Waiting.", orig_task_positions[i], all_runtimes[i], all_cores[i], all_submit[i], all_req_runtimes[i], available_nodes, all_cores[i]);
          //MSG_process_sleep(1.0f);
          MSG_process_suspend(p_master);     
        }               
      }while(available_nodes < all_cores[i]);      

      task_queue[i].numNodes = all_cores[i];
      task_queue[i].startTime = 0.0f;
      task_queue[i].endTime = 0.0f;
      task_queue[i].submitTime = all_submit[i];
      //task_queue[i].task_comp_size = (double*) malloc(all_cores[i] * sizeof(double));
      //task_queue[i].task_comm_size = (double*) malloc(all_cores[i] * all_cores[i] * sizeof(double));
      task_queue[i].task_allocation = (int*) malloc((all_cores[i]) * sizeof(int));      
      //task_queue[i].task_workers = (msg_host_t*) calloc(all_cores[i], sizeof(msg_host_t));

      //tasks_comp_sizes[i] = (double*) malloc(all_cores[i] * sizeof(double));
      //tasks_comm_sizes[i] = (double*) malloc(all_cores[i] * all_cores[i] * sizeof(double));
      //tasks_allocation[i] = (int*) malloc((all_cores[i] + 1) * sizeof(int));
      //tasks_allocation[i][0] = all_cores[i];
      //tasks_workers[i] = xbt_new0(msg_host_t*, all_cores[i]);

      int count = 0;
      for (j = 0; j < number_of_nodes; j++) {          
          if(busy_workers[j] == 0){
            //task_queue[i].task_workers[count] = workers[j];
            task_queue[i].task_allocation[count] = j;
            busy_workers[j] = 1;
            count++;
          }
          if(count >= all_cores[i]){
            break;
          }
      }

      msg_host_t self = MSG_host_self();
      double speed = MSG_host_get_speed(self);

      double comp_size = all_runtimes[i] * speed;
      double comm_size = 1000.0f;//0.001f * MEGA; 

      /*
      for (j = 0; j < all_cores[i]; j++) {
          task_queue[i].task_comp_size[j] = comp_size;
      }

      for (j = 0; j < all_cores[i]; j++)
          for (k = j + 1; k < all_cores[i]; k++)
            task_queue[i].task_comm_size[j * all_cores[i] + k] = comm_size;
      */

      
      char sprintf_buffer[64];
      if(i < NUM_TASKS_STATE){
        sprintf(sprintf_buffer, "Task_%d", i);
      }else{
        sprintf(sprintf_buffer, "Task_%d", orig_task_positions[i]);
      }
      todo[i] = MSG_task_create(sprintf_buffer, comp_size, comm_size, &task_queue[i]);

      if(VERBOSE)
      XBT_INFO("Dispatching \"%s\" [r=%.1f,c=%d,s=%d,est=%.1f]", todo[i]->name, all_runtimes[i], all_cores[i], all_submit[i], all_req_runtimes[i]);

      MSG_task_send(todo[i], MSG_host_get_name(workers[i]));

      //if(VERBOSE)
      //XBT_INFO("Sent");

      if(i == NUM_TASKS_STATE - 1){
        t0 = MSG_get_clock();        
        
        if(STATE){
          float* elapsed_times = (float*) calloc(number_of_nodes, sizeof(float));
          for (j = 0; j <= i; j++) {
            if(task_queue[j].endTime == 0.0f){
              float task_elapsed_time = MSG_get_clock() - task_queue[j].startTime;
              if(j == i){
                task_elapsed_time = 0.01f;
              }           
              //printf("%d - %f\n", j, task_elapsed_time);
              for (k = 0; k < all_cores[j]; k++) {
                //printf("%d\n", task_queue[j].task_allocation[k]);
                elapsed_times[(task_queue[j].task_allocation[k])] = task_elapsed_time;
              }
            }
          }
          for (j = 0; j < number_of_nodes; j++) {
            if(j < number_of_nodes - 1)
              printf("%f,", elapsed_times[j]);
            else
              printf("%f\n", elapsed_times[j]);          
          }
          break;
        }
      }       
    }

if(STATE){
  if(VERBOSE)
    XBT_INFO("All tasks have been dispatched. Let's tell everybody the computation is over.");
  for (i = NUM_TASKS_STATE; i < num_managers; i++) {
    msg_task_t finalize = MSG_task_create("finalize", 0, 0, FINALIZE);
    MSG_task_send(finalize, MSG_host_get_name(workers[i]));
  }
}

  if(VERBOSE)
  XBT_INFO("Goodbye now!");
  free(workers);
  free(todo);
  return 0;
  }

}                               /* end_of_master */

/** Receiver function  */
int taskManager(int argc, char *argv[])
{
  msg_task_t task = NULL;
  struct task_t* _task = NULL;
  int i;
  int res;
  //while (1) {
    res = MSG_task_receive(&(task),MSG_host_get_name(MSG_host_self()));
    xbt_assert(res == MSG_OK, "MSG_task_receive failed");
    _task = (struct task_t*) MSG_task_get_data(task);

    if(VERBOSE)
    XBT_INFO("Received \"%s\"", MSG_task_get_name(task));
    
    if (!strcmp(MSG_task_get_name(task), "finalize")) {
      MSG_task_destroy(task);
      return 0;
    }
    

    if(VERBOSE)
    XBT_INFO("Processing \"%s\"", MSG_task_get_name(task));
    /*
    XBT_INFO("Executing on hosts: ");    
    for(i=0;i<_task->numNodes;i++){
      XBT_INFO("\"%s\"", MSG_host_get_name(_task->task_workers[i]));
    }
    */
    _task->startTime = MSG_get_clock();
    MSG_task_execute(task);    
    _task->endTime = MSG_get_clock();
    if(VERBOSE)
    XBT_INFO("\"%s\" done", MSG_task_get_name(task));    
    int* allocation = _task->task_allocation;
    int n = _task->numNodes;
    //int i;
    for(i = 0; i < n; i++){
      busy_workers[allocation[i]] = 0;
    } 
    MSG_task_destroy(task);
    task = NULL;
    MSG_process_resume(p_master);
  //}
  //if(VERBOSE)
  //XBT_INFO("I'm done. See you!");
  return 0;
}                               /* end_of_worker */


int taskMonitor(int argc, char *argv[]){
  int i;
  for (i = 0; i < number_of_tasks; i++) {      
    while(MSG_get_clock() < all_submit[i]){//task has not arrived yet 
    MSG_process_sleep(all_submit[i] - MSG_get_clock());
    }
  if(VERBOSE)
    XBT_INFO("\"Task_%d\" [r=%.1f,c=%d,s=%d,req=%.1f] arrived. Waking up master.", i, all_runtimes[i], all_cores[i], all_submit[i], all_req_runtimes[i]);
  MSG_process_resume(p_master);
  }
return 0;
}

/** Test function */
msg_error_t test_all(const char *platform_file,
                     const char *application_file)
{
  msg_error_t res = MSG_OK;
  int i;

  {                             /*  Simulation setting */
    MSG_config("host/model", "default");
    //MSG_set_channel_number(1);
    MSG_create_environment(platform_file);
  }
  {                             /*   Application deployment */
    //int i;
    
    MSG_function_register("master", master);
    MSG_function_register("taskManager", taskManager);
    MSG_function_register("taskMonitor", taskMonitor);
    
    MSG_launch_application(application_file);

    char sprintf_buffer[64];

    for(i = 0; i < num_managers; i++){
      sprintf(sprintf_buffer, "node-%d", i+1);
      MSG_process_create("taskManager", taskManager, NULL, MSG_get_host_by_name(sprintf_buffer));
    }
    
    sprintf(sprintf_buffer, "node-%d", number_of_tasks + 1);
    MSG_process_create("taskMonitor", taskMonitor, NULL, MSG_get_host_by_name(sprintf_buffer));
   

  }
  res = MSG_main();
  
  double sumSlowdown = 0.0f;
  slowdown = (double* ) calloc(number_of_tasks - NUM_TASKS_STATE, sizeof(double));
  int _count = 0;
  for (i = NUM_TASKS_STATE; i < number_of_tasks; i++) {
    double waitTime = task_queue[i].startTime - task_queue[i].submitTime;    
    double runTime = task_queue[i].endTime - task_queue[i].startTime;
    double quocient = runTime >= TAO ? runTime : TAO;
    double slow = (waitTime + runTime) / quocient;  
    slowdown[_count] = slow >= 1.0f ? slow : 1.0f;
    sumSlowdown += slowdown[_count];
    //printf("%.2f\n", waitTime);
    //printf("[%.2f %.2f %.2f %d] ", waitTime, slowdown[_count], runTime, task_queue[i].numNodes);
    if(VERBOSE)
      XBT_INFO("Execution Stats for \"Task_%d\" [r=%.1f,c=%d,s=%d,est=%.1f]: Wait Time=%.2f, Slowdown=%.2f, Simulated Runtime=%.2f.", orig_task_positions[i], all_runtimes[i], all_cores[i], all_submit[i], all_req_runtimes[i], waitTime, slowdown[_count], runTime);
    _count++;
  }

  double AVGSlowdown = sumSlowdown / (number_of_tasks - NUM_TASKS_STATE);
  
  if(VERBOSE){
    XBT_INFO("Average bounded slowdown: %f", AVGSlowdown);
    XBT_INFO("Simulation time %g", MSG_get_clock());
  }else if(!STATE){
    printf("%f\n", AVGSlowdown);
  }
//int i,j;

/*
for(i=0;i<MAX_TASKS; i++){
printf("%f,", sched_task_placement[i]);
} 
printf("%g\n", MSG_get_clock());
*/


  return res;
}                               /* end_of_test_all */


/** Main function */
int main(int argc, char *argv[])
{
  msg_error_t res = MSG_OK;

  int i;

  MSG_init(&argc, argv);
  if (argc < 3) {
    printf("Usage: %s platform_file deployment_file [-verbose]\n", argv[0]);
    printf("example: %s msg_platform.xml msg_deployment.xml -verbose\n", argv[0]);
    exit(1);
  }
  //seed = atoi(argv[3]);
  if(argc >= 4){
    for(i = 3;i < argc; i++){
      if (strcmp(argv[i], "-verbose") == 0){
        VERBOSE = 1;
      }
      if (strcmp(argv[i], "-state") == 0){
        STATE = 1;
      }
	  if (strcmp(argv[i], "-spt") == 0){
        chosen_policy = SJF;
      }
	  if (strcmp(argv[i], "-wfp3") == 0){
        chosen_policy = WFP3;
      }
	  if (strcmp(argv[i], "-unicef") == 0){
        chosen_policy = UNICEF;
      }
      if (strcmp(argv[i], "-easy") == 0){
        chosen_policy = EASY;
      }
      if (strcmp(argv[i], "-f4") == 0){
        chosen_policy = F4;
      }
      if (strcmp(argv[i], "-f3") == 0){
        chosen_policy = F3;
      }
      if (strcmp(argv[i], "-f2") == 0){
        chosen_policy = F2;
      }
      if (strcmp(argv[i], "-f1") == 0){
        chosen_policy = F1;
      }
      if (strcmp(argv[i], "-linear") == 0){
        chosen_policy = LINEAR;
      }
      if (strcmp(argv[i], "-quadratic") == 0){
        chosen_policy = QUADRATIC;
      }
      if (strcmp(argv[i], "-cubic") == 0){
        chosen_policy = CUBIC;
      }
      if (strcmp(argv[i], "-quartic") == 0){
        chosen_policy = QUARTIC;
      }
      if (strcmp(argv[i], "-quintic") == 0){
        chosen_policy = QUINTIC;
      }
      if (strcmp(argv[i], "-sextic") == 0){
        chosen_policy = SEXTIC;
      }
      if (strcmp(argv[i], "-saf") == 0){
        chosen_policy = SAF;
      }
      if (strcmp(argv[i], "-nt") == 0){
        number_of_tasks = atoi(argv[i+1]);
        num_managers = number_of_tasks;
      }
      if (strcmp(argv[i], "-bf") == 0){
        BF = 1;
      }
    }
  }
  if(number_of_tasks == 0){
    printf("Invalid number_of_tasks parameter. Please set -nt parameter in runtime.\n");
    exit(1);
  }
  res = test_all(argv[1], argv[2]);

  if (res == MSG_OK)
    return 0;
  else
    return 1;
}                               /* end_of_main */
