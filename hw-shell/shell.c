#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include "tokenizer.h"

/* Convenience macro to silence compiler warnings about unused function parameters. */
#define unused __attribute__((unused))

/* Whether the shell is connected to an actual terminal or not. */
bool shell_is_interactive;

/* File descriptor for the shell input */
int shell_terminal;

/* Terminal mode settings for the shell */
struct termios shell_tmodes;

/* Process group id for the shell */
pid_t shell_pgid;

int cmd_exit(struct tokens* tokens);
int cmd_help(struct tokens* tokens);
int cmd_pwd(struct tokens* tokens);
int cmd_cd(struct tokens* tokens);

/* Built-in command functions take token array (see parse.h) and return int */
typedef int cmd_fun_t(struct tokens* tokens);

/* Built-in command struct and lookup table */
typedef struct fun_desc {
  cmd_fun_t* fun;
  char* cmd;
  char* doc;
} fun_desc_t;

fun_desc_t cmd_table[] = {
    {cmd_help, "?", "show this help menu"},
    {cmd_exit, "exit", "exit the command shell"},
    {cmd_pwd, "pwd", "print the current working directory to stand output"},
    {cmd_cd, "cd", "take an argument and change current working directory to that directory"},
};

/* Prints a helpful description for the given command */
int cmd_help(unused struct tokens* tokens) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    printf("%s - %s\n", cmd_table[i].cmd, cmd_table[i].doc);
  return 1;
}

/* Exits this shell */
int cmd_exit(unused struct tokens* tokens) { exit(0); }

/* Prints the current working directory to stand output */
int cmd_pwd(unused struct tokens* tokens) {
  char* pwd = getcwd(NULL, 0);
  if (pwd == NULL) {
    printf("In cmd_pwd: failed to allocate for pwd\n");
    exit(-1);
  }
  fprintf(stdout, "%s\n", pwd);
  free(pwd);
  return 0;
}

/* Changes current working directory to argument user input */
int cmd_cd(unused struct tokens* tokens) {
  int retval = chdir(tokens->tokens[1]);
  if (retval == -1) {
    printf("In cmd_cd: path doesn't exist\n");
    exit(-1);
  }
  return 0;
}

int execute(struct tokens* tokens) {
  char* filename = tokens_get_token(tokens, 0);  // get filename
  // printf("%s", filename);
  char fullpath[4096];
  if (filename == NULL) {
    printf("In execute: filename should not be empty\n");
    exit(-1);
  }

  int status;
  int pid = fork();

  if (pid == 0) {
    FILE* fp;
    if ((fp = fopen(filename, "r"))) {
      /* If it is full name. */
      fclose(fp);
      memcpy(fullpath, filename, strlen(filename) + 1);
    } else {
      /* If it is not then search for environmental variables. */
      char* envvar = getenv("PATH");
      char* token;
      char* saveptr;
      bool flag = false;
      for (char* str = envvar; ; str = NULL) {
      
        token = strtok_r(str, ":", &saveptr);
        if (token == NULL) {
          break;
        }

        memcpy(fullpath, token, strlen(token) + 1);
        fullpath[strlen(token)] = '/';
        fullpath[strlen(token) + 1] = '\0';
        strcat(fullpath, filename); // contcatenate the path and filename
        if ((fp = fopen(fullpath, "r"))) {
          fclose(fp);
          flag = true;
          break;
        }
      }
      if (!flag) {
        printf("In execution: your program doesn't exist\n");
      }
    }

    printf("%s\n", fullpath);
    int length = tokens_get_length(tokens);
    int argc = 0;
    char* argv[length + 1];  // store the arguments
    bool stdin_redir = false, stdout_redir = false;

    /* Initialize the argv for execv. */
    for (int i = 0; i < length; i++) {
      char* arg = tokens_get_token(tokens, i);
      if (strcmp(arg, "<") == 0) {
        /* If it is an input redirection character. */
        stdin_redir = true;
      } else if (strcmp(arg, ">") == 0) {
        /* If it is an output redirection character. */
        stdout_redir = true;
      } else if (stdin_redir) {
        int fd = open(arg, O_RDONLY);
        dup2(fd, STDIN_FILENO);
        close(fd);
        stdin_redir = false; // prepare for next redirection
      } else if (stdout_redir) {
        int fd = creat(arg, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        dup2(fd, STDOUT_FILENO);
        close(fd);
        stdout_redir = false; // prepare for next redirection
      } else {
        /* When it is a normal parameter. */
        argv[argc] = malloc(strlen(arg) + 1);
        if (argv[argc] == NULL) {
          printf("In execute: malloc failed\n");
          exit(-1);
        }
        memcpy(argv[argc], arg, strlen(arg) + 1);
        argc++;
      }
    }
    argv[argc] = NULL;

    // for (int i = 0; i < length; i++) {
    //   printf("%s\n", argv[i]);
    // }

    int retval = execv(fullpath, argv);
    if (retval == -1) {
      printf("In execute: no such program\n");
      return -1;
    }
  } else {
    waitpid(pid, &status, 0);
  }
  
  return status;
}

/* Looks up the built-in command, if it exists. */
int lookup(char cmd[]) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    if (cmd && (strcmp(cmd_table[i].cmd, cmd) == 0))
      return i;
  return -1;
}

/* Intialization procedures for this shell */
void init_shell() {
  /* Our shell is connected to standard input. */
  shell_terminal = STDIN_FILENO;

  /* Check if we are running interactively */
  shell_is_interactive = isatty(shell_terminal);

  if (shell_is_interactive) {
    /* If the shell is not currently in the foreground, we must pause the shell until it becomes a
     * foreground process. We use SIGTTIN to pause the shell. When the shell gets moved to the
     * foreground, we'll receive a SIGCONT. */
    while (tcgetpgrp(shell_terminal) != (shell_pgid = getpgrp()))
      kill(-shell_pgid, SIGTTIN);

    /* Saves the shell's process id */
    shell_pgid = getpid();

    /* Take control of the terminal */
    tcsetpgrp(shell_terminal, shell_pgid);

    /* Save the current termios to a variable, so it can be restored later. */
    tcgetattr(shell_terminal, &shell_tmodes);
  }
}

int main(unused int argc, unused char* argv[]) {
  init_shell();

  static char line[4096];
  int line_num = 0;

  /* Please only print shell prompts when standard input is not a tty */
  if (shell_is_interactive)
    fprintf(stdout, "%d: ", line_num);

  while (fgets(line, 4096, stdin)) {
    /* Split our line into words. */
    struct tokens* tokens = tokenize(line);

    /* Find which built-in function to run. */
    int fundex = lookup(tokens_get_token(tokens, 0));

    if (fundex >= 0) {
      cmd_table[fundex].fun(tokens);
    } else {
      execute(tokens);
    }

    if (shell_is_interactive)
      /* Please only print shell prompts when standard input is not a tty */
      fprintf(stdout, "%d: ", ++line_num);

    /* Clean up memory */
    tokens_destroy(tokens);
  }

  return 0;
}
