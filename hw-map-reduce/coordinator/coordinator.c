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

/* Debug helper function. */
void print_job_info(struct job* job) {
  printf("Printing job infomation...\n");
  printf("The job's id is %d\n", job->job_id);
  printf("It has %d files:\n", job->n_map);
  for (int i = 0; i < job->n_map; i++) {
    printf("file %d\t%s\n", i, job->files[i]);
  }
  printf("We'll out put the result to %s using %s\n", job->output_dir, job->app);

  printf("Current job has %d map tasks, %d of them has finished\n", job->n_map, job->map_finished);
  printf("Every map task's current state is:\n");
  for (int i = 0; i < job->n_map; i++) {
    printf("task %d\tassign time: %ld, success: %d\n", i, job->map_time[i], job->map_success[i]);
  }

  printf("Current job has %d reduce tasks, %d of them has finished\n", job->n_reduce, job->reduce_finished);
  printf("Every reduce task's current state is:\n");
  for (int i = 0; i < job->n_reduce; i++) {
    printf("task %d\tassign time: %ld, success: %d\n", i, job->reduce_time[i], job->reduce_success[i]);
  }

  printf("The job's auxilliary arguments are: %s\n", job->args);

  printf("Job done: %d, job failed: %d\n", job->done, job->failed);
}

/* SUBMIT_JOB RPC implementation. */
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;

  printf("Received submit job request\n");

  /* Assign a job ID. */
  result = state->next_id++;
  
  /* Malloc a new job and initialize. */
  struct job* new_job = malloc(sizeof(struct job));
  if (new_job == NULL) {
    printf("In submit_job_1_svc: malloc new_job failed\n");
    return NULL;
  }

  printf("In submit_job_1_svc: malloc new_job success\n");

  new_job->job_id = result;
  printf("new job_is is %d\n", new_job->job_id);
  /* Duplicate files. */
  new_job->files = malloc(sizeof(char*) * argp->files.files_len);
  for (int i = 0; i < argp->files.files_len; i++) {
    new_job->files[i] = strdup(argp->files.files_val[i]);
    printf("file[%d]: %s\n", i, new_job->files[i]);
    if (new_job->files[i] == NULL) {
      printf("In submit_job_1_svc: malloc file %d failed\n", i);
      return NULL;
    }
  }

  new_job->output_dir = strdup(argp->output_dir);
  printf("output_dir: %s\n", new_job->output_dir);
  
  if (get_app(argp->app).name != NULL) {
    new_job->app = strdup(argp->app);
    printf("app: %s\n", new_job->app);
  } else {
    printf("In submit_job_1_svc: provided app doesn't exist, return -1\n");
    result = -1;
    return &result;
  }

  new_job->n_map = argp->files.files_len;
  printf("files_len: %d\n", new_job->n_map);
  new_job->map_finished = 0;
  new_job->map_success = malloc(sizeof(bool) * new_job->n_map);
  if (new_job->map_success == NULL) {
    printf("In submit_job_1_svc: malloc map_success failed\n");
    return NULL;
  }
  for (int i = 0; i < new_job->n_map; i++) {
    new_job->map_success[i] = false;
  }

  new_job->map_time = malloc(sizeof(time_t) * new_job->n_map);
  if (new_job->map_time == NULL) {
    printf("In submit_job_1_svc: malloc map_time failed\n");
    return NULL;
  }
  for (int i = 0; i < new_job->n_map; i++) {
    new_job->map_time[i] = (time_t)0;
  }

  new_job->n_reduce = argp->n_reduce;
  new_job->reduce_finished = 0;
  new_job->reduce_success = malloc(sizeof(bool) * new_job->n_reduce);
  if (new_job->reduce_success == NULL) {
    printf("In submit_job_1_svc: malloc reduce_success failed\n");
    return NULL;
  }
  for (int i = 0; i < new_job->n_reduce; i++) {
    new_job->reduce_success[i] = false;
  }

  new_job->reduce_time = malloc(sizeof(time_t) * new_job->n_reduce);
  if (new_job->reduce_time == NULL) {
    printf("In submit_job_1_svc: malloc reduce_time failed\n");
    return NULL;
  }
  for (int i = 0; i < new_job->n_reduce; i++) {
    new_job->reduce_time[i] = false;
  }


  printf("args: %s\n", argp->args.args_val);
  if (argp->args.args_val == NULL || strlen(argp->args.args_val) == 0) {
    new_job->args = NULL;
  } else {
    new_job->args = strdup(argp->args.args_val);
  }

  new_job->done = false;
  new_job->failed = false;

  /* Insert new_job into hash table and waiting queue. */
  printf("Job_id is: %d\n", new_job->job_id);
  int tmp = new_job->job_id;
  g_hash_table_insert(all_jobs, GINT_TO_POINTER(tmp), new_job);
  printf("Insert success\n");
  state->waiting_queue = g_list_append(state->waiting_queue, GINT_TO_POINTER(new_job->job_id));
  printf("Append success. Current length = %d\n", g_list_length(state->waiting_queue));

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

  printf("Received poll job request\n");

  static int job_id;
  job_id = *argp;

  if (g_hash_table_size(all_jobs) == 0) {
    printf("The hash table is empty\n");
    result.invalid_job_id = true;
    return &result;
  }

  struct job* job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(job_id));

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

  /* If worker crashes. */
  // TODO

  GList* elem = NULL;
  static int lookup_id;
  static struct job* job;
  bool assigned_not_finished_map_task = false; // If there is a map task that has been assigned but not been finished.
  /* If there are map task that hasn't been assigned. */
  for (elem = state->waiting_queue; elem; elem = elem->next) {
    lookup_id = GPOINTER_TO_INT(elem->data); // Cast back to an integer.
    printf("Finding job %d for map task\n", lookup_id);
    job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(lookup_id));
    if (job->map_finished == job->n_map) {
      continue;
    }
    for (int i = 0; i < job->n_map; i++) {
      if (job->map_time[i] == (time_t)0) {
        /* Can write the content below to be a function. */
        result.job_id = job->job_id;
        result.task = i;
        result.file = strdup(job->files[i]);
        result.output_dir = strdup(job->output_dir);
        result.app = strdup(job->app);
        result.n_reduce = job->n_reduce;
        result.n_map = job->n_map;
        result.reduce = false;
        result.wait = false;
        if (job->args == NULL) {
          result.args.args_len = 0;
        } else {
          result.args.args_len = strlen(job->args);
          result.args.args_val = strdup(job->args);
        }
        job->map_time[i] = time(NULL);
        printf("Assign job %d task(map) %d at time %ld\n", job->job_id, i, job->map_time[i]);
        return &result;
      }
    }
    bool all_assigned = true;
    for (int j = 0; j < job->n_map; j++) {
      if (job->map_time[j] == (time_t)0) {
        all_assigned = false;
      }
    }
    if (all_assigned && job->map_finished < job->n_map) {
      assigned_not_finished_map_task = true;
    }
  }

  /* If there is any map task that hasn't been finished, we wait and do nothing. */
  if (assigned_not_finished_map_task) {
    return &result;
  }

  /* If there is reduce task that hasn't been assigned. */
  for (elem = state->waiting_queue; elem; elem = elem->next) {
    lookup_id = GPOINTER_TO_INT(elem->data); // Cast back to an integer.
    printf("Finding job %d for reduce task\n", lookup_id);
    job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(lookup_id));
    for (int i = 0; i < job->n_reduce; i++) {
      if (job->reduce_time[i] == (time_t)0) {
        /* Can write the content below to be a function. */
        result.job_id = job->job_id;
        result.task = i;
        // result.file = job->file;
        result.output_dir = strdup(job->output_dir);
        result.app = strdup(job->app);
        result.n_reduce = job->n_reduce;
        result.n_map = job->n_map;
        result.reduce = true;
        result.wait = false;
        if (job->args == NULL) {
          result.args.args_len = 0;
        } else {
          result.args.args_len = strlen(job->args);
          result.args.args_val = strdup(job->args);
        }
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

  printf("Received finish task request\n");

  static int job_id;
  static int task;
  static bool reduce;
  static bool success;

  job_id = argp->job_id;
  task = argp->task;
  reduce = argp->reduce;
  success = argp->success;

  /* If waiting queue is empty. */
  if (g_list_length(state->waiting_queue) == 0) {
    return (void*)&result;
  }

  /* Find if job id is in the waiting queue. */
  static GList* lookup_res;
  static struct job* job;
  lookup_res = g_list_find(state->waiting_queue, GINT_TO_POINTER(job_id));
  if (lookup_res == NULL) {
    return (void*)&result;
  }

  job = g_hash_table_lookup(all_jobs, GINT_TO_POINTER(job_id));

  if (success == false) {
    job->done = true;
    job->failed = true;
    state->waiting_queue = g_list_remove(state->waiting_queue, GINT_TO_POINTER(job_id));
  } else {
    if (argp->reduce) {
      job->reduce_finished++;
      job->reduce_success[argp->task] = true;
      if (job->reduce_finished == job->n_reduce) {
        job->done = true;
        job->failed = false;
        state->waiting_queue = g_list_remove(state->waiting_queue, GINT_TO_POINTER(job_id));
        printf("Job %d finished\n", job_id);
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
