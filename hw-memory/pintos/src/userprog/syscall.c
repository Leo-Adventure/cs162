#include "userprog/syscall.h"
#include <stdio.h>
#include <string.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "threads/palloc.h"
#include "filesys/filesys.h"
#include "filesys/file.h"
#include "userprog/pagedir.h"

static void syscall_handler(struct intr_frame*);

void syscall_init(void) { intr_register_int(0x30, 3, INTR_ON, syscall_handler, "syscall"); }

void syscall_exit(int status) {
  printf("%s: exit(%d)\n", thread_current()->name, status);
  thread_exit();
}

/*
 * This does not check that the buffer consists of only mapped pages; it merely
 * checks the buffer exists entirely below PHYS_BASE.
 */
static void validate_buffer_in_user_region(const void* buffer, size_t length) {
  uintptr_t delta = PHYS_BASE - buffer;
  if (!is_user_vaddr(buffer) || length > delta)
    syscall_exit(-1);
}

/*
 * This does not check that the string consists of only mapped pages; it merely
 * checks the string exists entirely below PHYS_BASE.
 */
static void validate_string_in_user_region(const char* string) {
  uintptr_t delta = PHYS_BASE - (const void*)string;
  if (!is_user_vaddr(string) || strnlen(string, delta) == delta)
    syscall_exit(-1);
}

static int syscall_open(const char* filename) {
  struct thread* t = thread_current();
  if (t->open_file != NULL)
    return -1;

  t->open_file = filesys_open(filename);
  if (t->open_file == NULL)
    return -1;

  return 2;
}

static int syscall_write(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd == STDOUT_FILENO) {
    putbuf(buffer, size);
    return size;
  } else if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_write(t->open_file, buffer, size);
}

static int syscall_read(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_read(t->open_file, buffer, size);
}

static void syscall_close(int fd) {
  struct thread* t = thread_current();
  if (fd == 2 && t->open_file != NULL) {
    file_close(t->open_file);
    t->open_file = NULL;
  }
}

static void* syscall_sbrk(intptr_t increment) {
  struct thread* t = thread_current();
  intptr_t prevbrk = t->brk;

  if (increment == 0) {
    return t->brk;
  } else if (increment > 0) {
    /* At first, we need to judge whether to allocate new page. */
    bool success = true;
    uint8_t* next_page = pg_round_up(t->brk);
    int to_page_up = next_page - t->brk;
    
    if (increment > to_page_up) {
      /* If exceed user virtual address, then must cause an error. */
      if (prevbrk + increment > PHYS_BASE || prevbrk + increment < 0) {
        return (void *)-1;
      }
      /* Cross the page border, need to allocate new pages. */
      int i;
      int new_pages = ((increment - to_page_up) + PGSIZE - 1) / PGSIZE;
      /* Integer overflow, which means increment is too large. */

      for (i = 0; i < new_pages; i++) {
        uint8_t* new_page = palloc_get_page(PAL_USER | PAL_ZERO);
        if (new_page == NULL) {
          success = false;
          break;
        }

        if (!pagedir_set_page(t->pagedir, next_page, new_page, true)) {
          palloc_free_page(new_page);
          success = false;
          break;
        }

        next_page += PGSIZE;
      }

      if (!success) {
        /* Undo every pages. */
        next_page = pg_round_up(t->brk);

        for (int j = 0; j < i; j++) {
          void* paddr = pagedir_get_page(t->pagedir, next_page);
          pagedir_clear_page(t->pagedir, next_page);
          palloc_free_page(paddr);
          next_page += PGSIZE;
        }

        return (void*) -1;
      } else {
        t->brk += increment;
        return prevbrk;
      }
    } else {
      /* Don't need to allocate new pages. */
      t->brk += increment;
      return prevbrk;
    }
  } else {
    uint8_t* prev_page = pg_round_down(t->brk);
    int to_page_down = prev_page - t->brk;

    if (to_page_down >= increment) {
      /* We need to deallocate pages. */
      int free_pages = ((to_page_down - increment) + PGSIZE - 1) / PGSIZE;
      free_pages = (free_pages == 0)? 1: free_pages;

      for (int i = 0; i < free_pages; i++) {
        void* paddr = pagedir_get_page(t->pagedir, prev_page);

        pagedir_clear_page(t->pagedir, prev_page);
        palloc_free_page(paddr);

        prev_page -= PGSIZE;
      }

      t->brk += increment;
      return prevbrk;
    } else {
      t->brk += increment;
      return prevbrk;
    }
  }
}

static void syscall_handler(struct intr_frame* f) {
  uint32_t* args = (uint32_t*)f->esp;
  struct thread* t = thread_current();
  t->in_syscall = true;

  validate_buffer_in_user_region(args, sizeof(uint32_t));
  switch (args[0]) {
    case SYS_EXIT:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_exit((int)args[1]);
      break;

    case SYS_OPEN:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      validate_string_in_user_region((char*)args[1]);
      f->eax = (uint32_t)syscall_open((char*)args[1]);
      break;

    case SYS_WRITE:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_write((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_READ:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_read((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_CLOSE:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_close((int)args[1]);
      break;

    case SYS_SBRK:
      validate_buffer_in_user_region(&args[1], sizeof(intptr_t));
      f->eax = syscall_sbrk((intptr_t)args[1]);
      break;

    default:
      printf("Unimplemented system call: %d\n", (int)args[0]);
      break;
  }

  t->in_syscall = false;
}
