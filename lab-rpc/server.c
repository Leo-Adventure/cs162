/**
 * Server binary.
 */

#include "kv_store.h"
#include <glib.h>
#include <memory.h>
#include <netinet/in.h>
#include <rpc/pmap_clnt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* TODO: Add global state. */
GHashTable *ht;

void init() {
  ht = g_hash_table_new(g_bytes_hash, g_bytes_equal);
}

void add(buf *key_, buf *value_) {
  GBytes *key = g_bytes_new(key_->buf_val, key_->buf_len); 
  GBytes *value = g_bytes_new(value_->buf_val, value_->buf_len);
  // GBytes *key = g_bytes_new("key", strlen("key")); 
  // GBytes *value = g_bytes_new("value", strlen("value"));
  g_hash_table_insert(ht, key, value);
}

buf lookup(buf *key_) {
  GBytes *key = g_bytes_new(key_->buf_val, key_->buf_len);
  GBytes *value = g_hash_table_lookup(ht, key);

  g_bytes_unref(key);

  buf *res = malloc(sizeof(buf));

  if (value != NULL) {
    long unsigned int len;
    const char *data = g_bytes_get_data(value, &len);
    printf("%.*s\n", (int) len, data); /* Outputs first `len` characters of `data` ("value"). */

    res->buf_val = strdup(data);
    res->buf_len = len;

    return *res;
  }
  /* If we found nothing, then value is NULL. */
  res->buf_len = 0;

  return *res;
}

extern void kvstore_1(struct svc_req *, struct SVCXPRT *);

/* Set up and run RPC server. */
int main(int argc, char **argv) {
  register SVCXPRT *transp;

  pmap_unset(KVSTORE, KVSTORE_V1);

  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, KVSTORE, KVSTORE_V1, kvstore_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (KVSTORE, KVSTORE_V1, udp).");
    exit(1);
  }

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, KVSTORE, KVSTORE_V1, kvstore_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (KVSTORE, KVSTORE_V1, tcp).");
    exit(1);
  }

  /* TODO: Initialize state. */
  init();

  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/* Example server-side RPC stub. */
int *example_1_svc(int *argp, struct svc_req *rqstp) {
  static int result;

  result = *argp + 1;

  return &result;
}

/* TODO: Add additional RPC stubs. */
char **echo_1_svc(char **argp, struct svc_req *rqstp) {
  static char *result;

  result = *argp;

  return &result;
}

void *put_1_svc(put_request *argp, struct svc_req *rqstp) {
  static void* result;
  static buf *key;
  static buf *value;

  key = argp->key;
  value = argp->value;

  add(key, value);

  return &result;
}

buf *get_1_svc(buf *argp, struct svc_req *rqstp) {
  static buf arg;
  static buf result;

  arg = *argp;

  result = lookup(&arg);

  return &result;
}
