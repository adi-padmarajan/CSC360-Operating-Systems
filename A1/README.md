# SSI - A Simple Shell Interpretor

NovaShell lightweight POSIX-compatible command-line interpreter written in C, featuring interactive prompts, built-in commands (cd, bg, bglist), process management, and custom signal handling. Designed for modularity, robustness, and user responsiveness using readline() and fork()/execvp().

---

## Tech Stack

C · POSIX · GNU Readline · Signals · Process Control · Memory Managemen

---

## Feature Test Macros

> These macros enable specific POSIX and GNU extensions required by system-level functions.

```c
#define _POSIX_C_SOURCE 200809L

```
---

## Library Headers and Functions used

| Header                  | Functions Used                                                                   |
| :---------------------- | :------------------------------------------------------------------------------- |
| `<stdio.h>`             | `printf`, `fprintf`, `perror`, `snprintf`                                        |
| `<string.h>`            | `strlen`, `strcmp`, `strtok_r`, `strdup`, `strcat`, `memmove`                    |
| `<stdlib.h>`            | `malloc`, `realloc`, `free`, `exit`, `getenv`                                    |
| `<readline/readline.h>` | `readline` *(requires `-lreadline`)*                                             |
| `<readline/history.h>`  | `using_history`, `add_history`                                                   |
| `<unistd.h>`            | `getlogin`, `gethostname`, `getcwd`, `chdir`, `fork`, `execvp`, `write`, `_exit` |
| `<pwd.h>`               | `struct passwd`, `getpwuid`                                                      |
| `<limits.h>`            | `PATH_MAX`                                                                       |
| `<ctype.h>`             | `isspace` *(used by trimming helper)*                                            |
| `<sys/types.h>`         | `pid_t`                                                                          |
| `<sys/wait.h>`          | `waitpid`, macros like `WNOHANG`                                                 |
| `<errno.h>`             | `errno` *(for error handling)*                                                   |
| `<signal.h>`            | `sigaction`, `SIGINT`, `SIG_IGN`, `SIG_DFL`                                      |


---

## Overview

ssi.c is a simple shell interpreter that interacts directly with the Linux operating system.
It supports execution of external programs, directory management, and background processes — all within an interactive prompt.

---

## Core Functionalities

-  Foreground execution of external programs
-  Directory changes via built-in cd
-  Background execution via built-in bg
-  Listing background jobs with bglist
-  Graceful handling of Ctrl-D (EOF) and Ctrl-C (SIGINT)
  
---

## Features Implemented

### Foreground Execution

- Tokenizes a command line into argv[] and runs it with fork() + execvp().
- Prints <name>: No such file or directory if execvp fails (exit code 127 in child). 
- Parent waits for completion with waitpid. 
(Requirement: execute external programs with arbitrary numbers of args.)

### Changing Directories(cd)

- cd with no argument goes to $HOME. cd ~ or cd ~/path expands via $HOME.  
cd ., cd .., and relative paths are supported with chdir().
- Only the first argument is honored; extras are ignored by the main built-in dispatch. 
(Requirement: support changing directories, special handling for ., .., and ~ and reflect in prompt.)


### Prompt Formatting: (Completed and Working on UVIC Linux Server)

- username@hostname: <absolute cwd> >
- Username is obtained robustly; hostname via gethostname; directory via getcwd. 
(Requirement: display correct prompt format.)

### Background Execution: (Completed and Working on UVIC Linux Server)

- bg <command> [args…] forks and execvps the command in the child; the parent immediately returns to the prompt.
- Each background job (PID + joined command line) is recorded in a singly linked list.
- bglist prints the current list and a total count.
- Finished background children are reaped with waitpid(WNOHANG) and are reported as
PID: <command> has terminated. during the next input cycle. 
(Requirement: run jobs in background, list jobs, and notify on termination.)

### Signal handling & EOF: (Completed and Working on UVIC Linux Server)

- While a foreground child runs, the parent temporarily ignores SIGINT so Ctrl-C reaches the child only; after the child exits, the parent restores its handler.
- At the interactive prompt, a custom SIGINT handler clears the current line and redraws the prompt (using GNU Readline hooks).
- Ctrl-D at empty prompt exits cleanly; otherwise it’s ignored when line buffer is non-empty. 
(Requirement: proper Ctrl-C/Ctrl-D behavior.)

---
## Final Grade: 98.33%
