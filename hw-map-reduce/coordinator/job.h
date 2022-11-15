/**
 * Logic for job and task management.
 *
 * You are not required to modify this file.
 */

#ifndef JOB_H__
#define JOB_H__

/* You may add definitions here */
#include <stdbool.h>
#include <time.h>
#include "../lib/lib.h"

struct job {
  int job_id;
  char** files;
  char* output_dir;
  char* app;
  int n_map;
  int map_finished;
  bool* map_success; /* Size equals 'n_map'. */
  time_t* map_time; /* Size equals 'n_map', recording the assign time. */
  int n_reduce;
  int reduce_finished;
  bool* reduce_success; /* Size equals 'n_reduce', recording the assign time. */
  time_t* reduce_time;
  char* args;
  bool done;
  bool failed;
};

#endif
