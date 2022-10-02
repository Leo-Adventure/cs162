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

char* find_path(char* path) {
  FILE* fp = fopen(path, "r");
  char* abs_path = malloc(4096);  // store absolute path.
  if (abs_path == NULL) {
    printf("In find_path: malloc failed\n");
    exit(-1);
  }

  if (fp) {
    /* If the file path can be opened successfully, then it is a full path. */
    fclose(fp);
    memcpy(abs_path, path, strlen(path) + 1);
    return abs_path;
  } else {
    /* If it is not then search for environmental variables. */
    char* envvar = getenv("PATH");
    char* token;
    char* saveptr;
    for (char* str = envvar; ; str = NULL) {
    
      token = strtok_r(str, ":", &saveptr);
      if (token == NULL) {
        break;
      }

      memcpy(abs_path, token, strlen(token) + 1);
      abs_path[strlen(token)] = '/';
      abs_path[strlen(token) + 1] = '\0';
      strcat(abs_path, path); // contcatenate the path and filename
      if ((fp = fopen(abs_path, "r"))) {
        fclose(fp);
        return abs_path;
      }
    }
  }
  return NULL;
}

int program(char** argv, int in, int out) {
  pid_t pid = fork();
  int status;

  if (pid == -1) {
    printf("In program: fork() creat child process failed\n");
    exit(-1);
  } else if (pid == 0) {
    /* Child process. */
    if (in != 0) {
      dup2(in, STDIN_FILENO);
    }
    if (out != 1) {
      dup2(out, STDOUT_FILENO);
    }

    
    execv(find_path(argv[0]), argv);
  } else {
    wait(&status);
    return status;
  }
}

int execute(struct tokens* tokens) {
  // char* filename = tokens_get_token(tokens, 0);  // get filename
  // // printf("%s", filename);
  // char fullpath[4096];
  // if (filename == NULL) {
  //   printf("In execute: filename should not be empty\n");
  //   exit(-1);
  // }

  // FILE* fp;
  // if ((fp = fopen(filename, "r"))) {
  //   /* If it is full name. */
  //   fclose(fp);
  //   memcpy(fullpath, filename, strlen(filename) + 1);
  // } else {
  //   /* If it is not then search for environmental variables. */
  //   char* envvar = getenv("PATH");
  //   char* token;
  //   char* saveptr;
  //   bool flag = false;
  //   for (char* str = envvar; ; str = NULL) {
    
  //     token = strtok_r(str, ":", &saveptr);
  //     if (token == NULL) {
  //       break;
  //     }

  //     memcpy(fullpath, token, strlen(token) + 1);
  //     fullpath[strlen(token)] = '/';
  //     fullpath[strlen(token) + 1] = '\0';
  //     strcat(fullpath, filename); // contcatenate the path and filename
  //     if ((fp = fopen(fullpath, "r"))) {
  //       fclose(fp);
  //       flag = true;
  //       break;
  //     }
  //   }
  //   if (!flag) {
  //     printf("In execution: your program doesn't exist\n");
  //   }
  // }

  int length = tokens_get_length(tokens);
  int argc = 0;
  char* argv[length + 1];  // store the arguments
  int in = 0, out = 1;
  int pipes[2];

  /* Initialize the argv for execv. */
  for (int i = 0; i < length; i++) {
    char* arg = tokens_get_token(tokens, i);

    if (strcmp(arg, "|") == 0) {
      argv[argc] = NULL;
      /* Create a pipe. */
      pipe(pipes);
      
      out = pipes[1];
      program(argv, in, out);
      close(out);
      out = 1;

      argc = 0;
      if (in != STDIN_FILENO) {
        close(in);
      }
      in = pipes[0];

    } else if (strcmp(arg, "<") == 0) {
      /* If it is an input redirection character. */
      arg = tokens_get_token(tokens, ++i);
      in = open(arg, O_RDONLY);
      
    } else if (strcmp(arg, ">") == 0) {
      /* If it is an output redirection character. */
      arg = tokens_get_token(tokens, ++i);
      out = creat(arg, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    } else {
      /* When it is a normal parameter. */
      argv[argc] = arg;
      argc++;
    }
  }
  argv[argc] = NULL;

  // // debug
  // for (int i = 0; i < argc; i++) {
  //   printf("%s\n", argv[i]);
  // }

  program(argv, in, out);

  return 0;
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
