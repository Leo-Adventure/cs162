/*
 * Word count application with one thread per input file.
 *
 * You may modify this file in any way you like, and are expected to modify it.
 * Your solution must read each input file from a separate thread. We encourage
 * you to make as few changes as necessary.
 */

/*
 * Copyright © 2021 University of California, Berkeley
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <pthread.h>

#include "word_count.h"
#include "word_helpers.h"

struct thread_args {
  word_count_list_t *word_counts;
  char *filename;
};

// void *thread_words(void* filename) {
//   FILE* infile = fopen(filename, "r");

//   if (infile == NULL) {
//     fprintf(stderr, "File does not exist.\n");
//     exit(1);
//   }

//   count_words(&word_counts, infile);
//   fclose(infile);

//   return NULL;
// }

extern char *new_string(char *);

void *thread_words(void *args) {
  FILE* infile = fopen(((struct thread_args *)args)->filename, "r");

  if (infile == NULL) {
    fprintf(stderr, "File does not exist.\n");
    exit(1);
  }

  count_words(((struct thread_args *)args)->word_counts, infile);
  fclose(infile);

  return NULL;
}

/*
 * main - handle command line, spawning one thread per file.
 */
int main(int argc, char* argv[]) {
  /* Create the empty data structure. */
  int rc;
  word_count_list_t word_counts;
  init_words(&word_counts);

  if (argc <= 1) {
    /* Process stdin in a single thread. */
    count_words(&word_counts, stdin);
  } else {
    pthread_t threads[argc - 1];
    /* 
     * Do remember to declare args to be a array, rather than a variable. Let's consider 
     * args is a variable. Then all thread will share the same args, which means when thread
     * A is running, main thread can change the filename and create thread B... Then the thread
     * will crash.
     */
    struct thread_args args[argc - 1];

    for (int t = 1; t < argc; t++) {
      args[t - 1].word_counts = &word_counts;
      args[t - 1].filename = new_string(argv[t]);
      
      rc = pthread_create(&threads[t - 1], NULL, thread_words, (void *)&args[t - 1]);

      if (rc) {
        printf("ERROR; return code from pthread_create() is %d\n", rc);
        exit(-1);
      }
    }

    for (int t = 1; t < argc; t ++) {
      pthread_join(threads[t - 1], NULL);
    }
  }

  /* Output final result of all threads' work. */
  wordcount_sort(&word_counts, less_count);
  fprint_words(&word_counts, stdout);
  return 0;
}
