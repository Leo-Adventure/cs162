/**
 * The MapReduce coordinator.
 */

#include "coordinator.h"

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* Global coordinator state. */
coordinator* state;
GHashTable* all_jobs;

extern void coordinator_1(struct svc_req*, struct SVCXPRT*);

/* Set up and run RPC server. */
int main(int argc, char** argv) {
  register SVCXPRT* transp;

  pmap_unset(COORDINATOR, COORDINATOR_V1);

  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, udp).");
    exit(1);
  }

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, tcp).");
    exit(1);
  }

  coordinator_init(&state);

  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/* EXAMPLE RPC implementation. */
int* example_1_svc(int* argp, struct svc_req* rqstp) {
  static int result;

  result = *argp + 1;

  return &result;
}

/* SUBMIT_JOB RPC implementation. */
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;
  struct job* new_job;

  printf("Received submit job request\n");

  /* Assign a unique job ID, starting from 0. */
  result = state->next_id++;
  
  /* Malloc a new job and initialize. */
  new_job = malloc(sizeof(struct job));
  if (new_job == NULL) {
    return NULL;
  }
  new_job->job_id = result;

  /* Duplicate files. */
  new_job->files = malloc(sizeof(char*) * argp->files.files_len);
  for (int i = 0; i < argp->files.files_len; i++) {
    new_job->files[i] = strdup(argp->files.files_val[i]);
    if (new_job->files[i] == NULL) {
      return NULL;
    }
  }

  /* Output directory. */
  new_job->output_dir = strdup(argp->output_dir);
  
  /* Application. */
  if (get_app(argp->app).name != NULL) {
    new_job->app = strdup(argp->app);
  } else {
    result = -1;
    return &result;
  }

  /* Map tasks' information. */
  new_job->n_map = argp->files.files_len;
  new_job->map_finished = 0;
  new_job->map_success = malloc(sizeof(bool) * new_job->n_map);
  if (new_job->map_success == NULL) {
    return NULL;
  }
  for (int i = 0; i < new_job->n_map; i++) {
    new_job->map_success[i] = false;
  }

  new_job->map_time = malloc(sizeof(time_t) * new_job->n_map);
  if (new_job->map_time == NULL) {
    return NULL;
  }
  for (int i = 0; i < new_job->n_map; i++) {
    new_job->map_time[i] = (time_t)0;
  }

  /* Reduce tasks' information. */
  new_job->n_reduce = argp->n_reduce;
  new_job->reduce_finished = 0;
  new_job->reduce_success = malloc(sizeof(bool) * new_job->n_reduce);
  if (new_job->reduce_success == NULL) {
    return NULL;
  }
  for (int i = 0; i < new_job->n_reduce; i++) {
    new_job->reduce_success[i] = false;
  }

  new_job->reduce_time = malloc(sizeof(time_t) * new_job->n_reduce);
  if (new_job->reduce_time == NULL) {
    return NULL;
  }
  for (int i = 0; i < new_job->n_reduce; i++) {
    new_job->reduce_time[i] = (time_t)0;
  }

  /* Auxiliary auguments. */
  if (argp->args.args_val == NULL || strlen(argp->args.args_val) == 0) {
    new_job->args = NULL;
  } else {
    new_job->args = strdup(argp->args.args_val);
  }

  /* Information. */
  new_job->done = false;
  new_job->failed = false;

  /* Insert new_job into hash table and waiting queue. */
  int tmp = new_job->job_id;
  g_hash_table_insert(all_jobs, GINT_TO_POINTER(tmp), new_job);
  state->waiting_queue = g_list_append(state->waiting_queue, GINT_TO_POINTER(new_job->job_id));

  /* Do not modify the following code. */
  /* BEGIN */
  struct stat st;
  if (stat(argp->output_dir, &st) == -1) {
    mkdirp(argp->output_dir);
  }

  return &result;
  /* END */
}

/* POLL_JOB RPC implementation. */
poll_job_reply* poll_job_1_svc(int* argp, struct svc_req* rqstp) {
  static poll_job_reply result;
  static int job_id;
  struct job* job;

  printf("Received poll job request\n");

  job_id = *argp;
  /* Get job by searching all_jobs. If all_job is empty, report invalid job id*/
  if (g_hash_table_size(all_jobs) == 0) {
    result.invalid_job_id = true;
    return &result;
  }
  job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(job_id));

  /* Update job reply information. */
  if (job == NULL) {
    result.invalid_job_id = true;
  } else {
    result.done = job->done;
    result.failed = job->failed;
    result.invalid_job_id = false;
  }

  return &result;
}

/* GET_TASK RPC implementation. */
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;
  GList* elem;
  static int lookup_id;
  static struct job* job;

  printf("Received get task request\n");
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0;

  /* If the queue is empty. */
  if (g_list_length(state->waiting_queue) == 0) {
    return &result;
  }

  /* If worker crashes, reassign task. */
  for (elem = state->waiting_queue; elem; elem = elem->next) {
    lookup_id = GPOINTER_TO_INT(elem->data); // Cast back to an integer.
    job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(lookup_id));

    /* Find assigned map task that hasn't been finished and that has expired. */
    for (int i = 0; i < job->n_map; i++) {
      if (job->map_time[i] != (time_t)0 && 
         ((time(NULL) - job->map_time[i]) >= TASK_TIMEOUT_SECS) && 
         !job->map_success[i]) {
        init_task_reply(&result, job, i, false);
        job->map_time[i] = time(NULL);
        return &result;
      }
    }

    /* If not all map tasks are finished, continue searching for next job. */
    if (job->map_finished < job->n_map) {
      continue;
    }

    /* Find assigned reduce task that hasn't been finished and that has expired. */
    for (int i = 0; i < job->n_reduce; i++) {
      if (job->reduce_time[i] != (time_t)0 && 
         ((time(NULL) - job->reduce_time[i]) >= TASK_TIMEOUT_SECS) && 
         !job->reduce_success[i]) {
        init_task_reply(&result, job, i, true);
        job->reduce_time[i] = time(NULL);
        return &result;
      }
    }
  }

  /* If there is map task that hasn't been assigned. */
  for (elem = state->waiting_queue; elem; elem = elem->next) {
    lookup_id = GPOINTER_TO_INT(elem->data); // Cast back to an integer.
    job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(lookup_id));
    /* Searching for unassigned map task. */
    for (int i = 0; i < job->n_map; i++) {
      if (job->map_time[i] == (time_t)0) {
        init_task_reply(&result, job, i, false);
        job->map_time[i] = time(NULL);
        return &result;
      }
    }

    /* 
     * There exites a map task that has been assigned but not finished. 
     * Then we don't assign its corresponding reduce task, and continue 
     * searching the next one.
     */
    if (job->map_finished < job->n_map) {
      continue;
    }

    /* All map tasks are finished, then assign reduce task. */
    for (int i = 0; i < job->n_reduce; i++) {
      if (job->reduce_time[i] == (time_t)0) {
        init_task_reply(&result, job, i, true);
        job->reduce_time[i] = time(NULL);
        return &result;
      }
    }
  }

  return &result;
}

/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;
  static int job_id;
  static int task;
  static bool reduce;
  static bool success;
  static GList* lookup_res;
  static struct job* job;

  printf("Received finish task request\n");

  job_id = argp->job_id;
  task = argp->task;
  reduce = argp->reduce;
  success = argp->success;

  /* If waiting queue is empty. */
  if (g_list_length(state->waiting_queue) == 0) {
    return (void*)&result;
  }

  /* 
   * Get desired job in the waiting queue by searching job_id. 
   * The job may not be in the waiting queue, since other worker
   * may fail its own task belonging to this job and remove the job
   * from waiting queue before current task is finished.
   */
  lookup_res = g_list_find(state->waiting_queue, GINT_TO_POINTER(job_id));
  if (lookup_res == NULL) {
    return (void*)&result;
  }
  job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(job_id));

  /* Update job information. */
  if (success == false) {
    /* Worker fails to finish a task. */
    job->done = true;
    job->failed = true;
    state->waiting_queue = g_list_remove(state->waiting_queue, GINT_TO_POINTER(job_id));
    free_job_memory(job);

  } else {
    /* Worker finishes a task successfully. */
    if (argp->reduce) {
      job->reduce_finished++;
      job->reduce_success[argp->task] = true;
      /* Job is finished successfully. */
      if (job->reduce_finished == job->n_reduce) {
        job->done = true;
        job->failed = false;
        state->waiting_queue = g_list_remove(state->waiting_queue, GINT_TO_POINTER(job_id));
        free_job_memory(job);
      }
    } else {
      job->map_finished++;
      job->map_success[argp->task] = true;
    }
  }

  return (void*)&result;
}

/* Initialize coordinator state. */
void coordinator_init(coordinator** coord_ptr) {
  *coord_ptr = malloc(sizeof(coordinator));

  coordinator* coord = *coord_ptr;

  coord->next_id = 0;
  coord->waiting_queue = NULL;
  all_jobs = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
}

/* Initialize a task reply information. */
void init_task_reply(get_task_reply* reply, struct job* job, int task, bool reduce) {
  reply->job_id = job->job_id;
  reply->task = task;
  if (!reduce) {
    reply->file = strdup(job->files[task]);
  }
  reply->output_dir = strdup(job->output_dir);
  reply->app = strdup(job->app);
  reply->n_reduce = job->n_reduce;
  reply->n_map = job->n_map;
  reply->reduce = reduce;
  reply->wait = false;
  if (job->args == NULL) {
    reply->args.args_len = 0;
  } else {
    reply->args.args_len = strlen(job->args);
    reply->args.args_val = strdup(job->args);
  }
}

/* 
 * After a job is done, files and task information
 * are useless. We free them to save memory.
 */
void free_job_memory(struct job* job) {
  /* Files. */
  for (int i = 0; i < job->n_map; i++) {
    free(job->files[i]);
  }
  free(job->files);

  /* Map tasks' information. */
  free(job->map_success);
  free(job->map_time);

  /* Reduce tasks' information. */
  free(job->reduce_success);
  free(job->reduce_time);
}
