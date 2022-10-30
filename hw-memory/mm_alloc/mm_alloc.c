/*
 * mm_alloc.c
 */

#include "mm_alloc.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

void* heap_start = NULL;

void* extend_heap(size_t size, struct block* prev) {
  struct block* block = sbrk(size + META_SIZE);
  if ((void*)block == (void*)-1) {
    return NULL;
  }

  if (prev) {
    prev->next = block;
  }
  
  block->prev = prev;
  block->next = NULL;
  block->free = 0;
  block->size = size;
  memset(block->data, 0, size);

  return block;
}

void split_heap(size_t size, struct block* block) {
  if (block->size - size > META_SIZE) {
    /* Get the splited block. */
    struct block* splited = (struct block*)(block->data + size);

    splited->prev = block;
    splited->next = block->next;
    splited->free = 1;
    splited->size = block->size - size - META_SIZE;

    block->next = splited;
    block->free = 0;
    block->size = size;

    memset(block->data, 0, size);
  } else {
    block->free = 0;
    block->size = size;

    memset(block->data, 0, size);
  }
}

void coalesce_heap(struct block* block) {
  if (block->next && block->next->free) {
    block->size = block->size + META_SIZE + block->next->size;
    block->next = block->next->next;
  }
  if (block->prev && block->prev->free) {
    block->prev->size = block->prev->size + META_SIZE + block->size;
    block->prev->next = block->next;
    if (block->next) {
      block->next->prev = block->prev;
    }
  }
}

void* mm_malloc(size_t size) {
  if (size == 0) {
    return NULL;
  }
  if (heap_start == NULL) {
    /* Initailize start of heap. */
    heap_start = sbrk(0);
    /* Get requested size block. */
    struct block* block = extend_heap(size, NULL);
    if (block == NULL) {
      return NULL;
    }

    return block->data;
  } else {
    struct block* prev = heap_start;
    struct block* ptr = prev->next;

    /* Only one block in the heap. */
    if (ptr == NULL) {
      if (prev->free && prev->size >= size) {
        split_heap(size, prev);
        return prev->data;
      }
    }

    /* Iterate the heap to find a free block. */
    while (ptr != NULL) {
      /* If it is a free block. */
      if (prev->free && prev->size >= size) {
        split_heap(size, prev);
        return prev->data;
      }

      prev = ptr;
      ptr = ptr->next;
    }

    /* If there is no free block in the heap. */
    ptr = extend_heap(size, prev);
    if (ptr == NULL) {
      return NULL;
    }
    return ptr->data;
  }
}

void* mm_realloc(void* ptr, size_t size) {
  /* Corner case. */
  if (ptr == NULL) {
    return mm_malloc(size);
  }
  if (size == 0) {
    mm_free(ptr);
    return NULL;
  }

  /* If the new size is smaller than the previous. */
  if (size <= ((struct block*)(ptr - META_SIZE))->size) {
    split_heap(size, ptr - META_SIZE);
    return ptr;
  } else {
    mm_free(ptr);
    void* p = mm_malloc(size);
    memset(p, 0, size);
    memcpy(p, ptr, ((struct block*)(ptr - META_SIZE))->size);
    return p;
  }
}

void mm_free(void* ptr) {
  if (ptr == NULL) {
    return;
  }
  struct block* block = (struct block*)(ptr - META_SIZE);
  block->free = 1;

  coalesce_heap(block);
  return;
}
