//
// Created by omer.benmoshe on 7/24/24.
//
/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     istribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - istributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - istributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>

#include <infiniband/verbs.h>

#define WC_BATCH (1)

#define SUCCESS 0
#define FAILURE 1
#define PORT_NUM 8598
#define MAX_EAGER_SIZE 4096
#define BUFFER_SIZE 8250
#define MAX_CLIENTS 1
#define NUM_BUFFERS_SEND_AND_RECEIVE 5
#define NUM_BUFFERS (NUM_BUFFERS_SEND_AND_RECEIVE + 1)
#define ACK_MSG_VALUE_SIZE 3
#define SERVER_SEND_BUFFER 5
#define CLIENT_RECEIVE_BUFFER 5
#define ACTIVE_WR 1
#define INACTIVE_WR 0

int all_argc;
char **all_argv;

//-------------- init structs and enums -----------------//
enum {
  PINGPONG_RECV_WRID = 1,
  PINGPONG_SEND_WRID = 2,
};

typedef enum {
  GET = 1,
  SET = 2
} request_type;

typedef enum {
  EAGER = 1,
  RENDEZVOUS = 2
} protocol_type;

typedef enum {
  CLIENT = 1,
  SERVER = 2
} sender_type;

typedef struct Pending_Dereg {
  char key[MAX_EAGER_SIZE];
  int client_id;
  struct ibv_mr *mr;
} Pending_Dereg;

typedef struct Pending_Dereg_List {
  Pending_Dereg **get_dereg_list;
  int size;
  int capacity;
} Pending_Dereg_List;

typedef struct Query {
  char key[MAX_EAGER_SIZE];
  char *value;
  int value_len;
  request_type request;
  protocol_type protocol;
} Query;

typedef struct Queries {
  Query *kv_table;
  int size;
  int capacity;
} Queries;

static int page_size;

struct pingpong_context {
  struct ibv_context *context;
  struct ibv_comp_channel *channel;
  struct ibv_pd *pd;
  struct ibv_mr *mr[NUM_BUFFERS];
  struct ibv_cq *cq;
  struct ibv_qp *qp;
  void *buf[NUM_BUFFERS];
  int cur_buf;
  int cur_active_wrs[NUM_BUFFERS];
  int size;
  int rx_depth;
  struct ibv_port_attr portinfo;
};

typedef struct server_context {
  struct pingpong_context **client_contexts;
  int num_clients;
} server_context;

struct pingpong_dest {
  int lid;
  int qpn;
  int psn;
  union ibv_gid gid;
};

enum ibv_mtu pp_mtu_to_enum(int mtu)
{
  switch (mtu)
    {
      case 256:
        return IBV_MTU_256;
      case 512:
        return IBV_MTU_512;
      case 1024:
        return IBV_MTU_1024;
      case 2048:
        return IBV_MTU_2048;
      case 4096:
        return IBV_MTU_4096;
      default:
        return -1;
    }
}

//-------------- function declarations -----------------//
int server_request_manager(void *kv_handle, int chosen_client, Queries *queries, Queries *pending_queries, Pending_Dereg_List *get_dereg_list);

Queries *initialize_data();

int send_query(struct pingpong_context *ctx, Query *query, sender_type sender);

Query *receive_query(struct pingpong_context *ctx);

Query *parse_query(struct pingpong_context *ctx, int buffer_index, sender_type sender);

int
client_set_rendezvous(struct pingpong_context *ctx, const char *key, const char *value);

int
client_get_rendezvous(struct pingpong_context *ctx, const char *key, char **value, Query *query);

int
server_set_rendezvous(struct pingpong_context *ctx, Query *query, Queries *pending_queries, Pending_Dereg_List *pending_dereg_list, int chosen_client);

int server_get_rendezvous(struct pingpong_context *ctx, Query *query, Pending_Dereg_List *get_dereg_list, int chosen_client);

int
client_set_eager(struct pingpong_context *ctx, const char *key, const char *value);

int server_get_eager(void *key, void *value, Queries *queries);

int insert_query_to_db(Query *query, void *value, Queries *queries);

static int
pp_post_send_with_RDMA(struct pingpong_context *ctx, int opcode, struct ibv_mr *local_mr, uint64_t remote_addr, uint32_t remote_key);

static int
pp_post_send_with_RDMA(struct pingpong_context *ctx, int opcode, struct ibv_mr *local_mr, uint64_t remote_addr, uint32_t remote_key);

void print_queries(Queries *p_queries);

int
pp_wait_completions_server(server_context *contexts, int* chosen_client);

void close_server(server_context *server_context, Queries *queries, Queries *pending_queries, Pending_Dereg_List *get_dereg_list);

void
server_set_rendezvous_second_phase(struct pingpong_context *ctx, Query *query, Queries *queries, Queries *pending_queries);

Pending_Dereg_List *initialize_get_dereg();

void
dereg_pending_request(struct pingpong_context *ctx, Query *query, Pending_Dereg_List *get_dereg_list, int client);

static int create_post_recv(struct pingpong_context *ctx, int i);

//-------------- gil functions -----------------//

uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
  struct ibv_port_attr attr;

  if (ibv_query_port(context, port, &attr))
    return 0;

  return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr)
{
  return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
  char tmp[9];
  uint32_t v32;
  int i;

  for (tmp[8] = 0, i = 0; i < 4; ++i)
    {
      memcpy(tmp, wgid + i * 8, 8);
      sscanf(tmp, "%x", &v32);
      *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
  int i;

  for (i = 0; i < 4; ++i)
    sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *) (gid->raw + i * 4)));
}

static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx)
{
  struct ibv_qp_attr attr = {
      .qp_state        = IBV_QPS_RTR,
      .path_mtu        = mtu,
      .dest_qp_num        = dest->qpn,
      .rq_psn            = dest->psn,
      .max_dest_rd_atomic    = 1,
      .min_rnr_timer        = 12,
      .ah_attr        = {
          .is_global    = 0,
          .dlid        = dest->lid,
          .sl        = sl,
          .src_path_bits    = 0,
          .port_num    = port
      }
  };

  if (dest->gid.global.interface_id)
    {
      attr.ah_attr.is_global = 1;
      attr.ah_attr.grh.hop_limit = 1;
      attr.ah_attr.grh.dgid = dest->gid;
      attr.ah_attr.grh.sgid_index = sgid_idx;
    }
  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE |
                    IBV_QP_AV |
                    IBV_QP_PATH_MTU |
                    IBV_QP_DEST_QPN |
                    IBV_QP_RQ_PSN |
                    IBV_QP_MAX_DEST_RD_ATOMIC |
                    IBV_QP_MIN_RNR_TIMER))
    {
      fprintf(stderr, "Failed to modify QP to RTR\n");
      return 1;
    }

  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = my_psn;
  attr.max_rd_atomic = 1;
  if (ibv_modify_qp(ctx->qp, &attr,
                    IBV_QP_STATE |
                    IBV_QP_TIMEOUT |
                    IBV_QP_RETRY_CNT |
                    IBV_QP_RNR_RETRY |
                    IBV_QP_SQ_PSN |
                    IBV_QP_MAX_QP_RD_ATOMIC))
    {
      fprintf(stderr, "Failed to modify QP to RTS\n");
      return 1;
    }

  return 0;
}

static struct pingpong_dest *
pp_client_exch_dest(const char *servername, int port,
                    const struct pingpong_dest *my_dest)
{
  struct addrinfo *res, *t;
  struct addrinfo hints = {
      .ai_family   = AF_INET,
      .ai_socktype = SOCK_STREAM
  };
  char *service;
  char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
  int n;
  int sockfd = -1;
  struct pingpong_dest *rem_dest = NULL;
  char gid[33];

  if (asprintf(&service, "%d", port) < 0)
    return NULL;

  n = getaddrinfo(servername, service, &hints, &res);

  if (n < 0)
    {
      fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
      free(service);
      return NULL;
    }

  for (t = res; t; t = t->ai_next)
    {
      sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
      if (sockfd >= 0)
        {
          if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
            break;
          close(sockfd);
          sockfd = -1;
        }
    }

  freeaddrinfo(res);
  free(service);

  if (sockfd < 0)
    {
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
      return NULL;
    }

  gid_to_wire_gid(&my_dest->gid, gid);
  sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
  if (write(sockfd, msg, sizeof msg) != sizeof msg)
    {
      fprintf(stderr, "Couldn't send local address\n");
      goto out;
    }

  if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
      perror("client read");
      fprintf(stderr, "Couldn't read remote address\n");
      goto out;
    }

  write(sockfd, "done", sizeof "done");

  rem_dest = malloc(sizeof *rem_dest);
  if (!rem_dest)
    goto out;

  sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
  wire_gid_to_gid(gid, &rem_dest->gid);

  out:
  close(sockfd);
  return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx)
{
  struct addrinfo *res, *t;
  struct addrinfo hints = {
      .ai_flags    = AI_PASSIVE,
      .ai_family   = AF_INET,
      .ai_socktype = SOCK_STREAM
  };
  char *service;
  char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
  int n;
  int sockfd = -1, connfd;
  struct pingpong_dest *rem_dest = NULL;
  char gid[33];

  if (asprintf(&service, "%d", port) < 0)
    return NULL;

  n = getaddrinfo(NULL, service, &hints, &res);

  if (n < 0)
    {
      fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
      free(service);
      return NULL;
    }

  for (t = res; t; t = t->ai_next)
    {
      sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
      if (sockfd >= 0)
        {
          n = 1;

          setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

          if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
            break;
          close(sockfd);
          sockfd = -1;
        }
    }

  freeaddrinfo(res);
  free(service);

  if (sockfd < 0)
    {
      fprintf(stderr, "Couldn't listen to port %d\n", port);
      return NULL;
    }

  listen(sockfd, 1);
  connfd = accept(sockfd, NULL, 0);
  close(sockfd);
  if (connfd < 0)
    {
      fprintf(stderr, "accept() failed\n");
      return NULL;
    }

  n = read(connfd, msg, sizeof msg);
  if (n != sizeof msg)
    {
      perror("server read");
      fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
      goto out;
    }

  rem_dest = malloc(sizeof *rem_dest);
  if (!rem_dest)
    goto out;

  sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
  wire_gid_to_gid(gid, &rem_dest->gid);

  if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx))
    {
      fprintf(stderr, "Couldn't connect to remote QP\n");
      free(rem_dest);
      rem_dest = NULL;
      goto out;
    }

  gid_to_wire_gid(&my_dest->gid, gid);
  sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
  if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
      fprintf(stderr, "Couldn't send local address\n");
      free(rem_dest);
      rem_dest = NULL;
      goto out;
    }

  read(connfd, msg, sizeof msg);

  out:
  close(connfd);
  return rem_dest;
}

static struct pingpong_context *
pp_init_ctx(struct ibv_device *ib_dev, int size,
            int rx_depth, int tx_depth, int port,
            int use_event, int is_server)
{
  struct pingpong_context *ctx;

  ctx = calloc(1, sizeof *ctx);
  if (!ctx)
    return NULL;

  ctx->size = size;
  ctx->rx_depth = rx_depth;


  // added stuff
  ctx->cur_buf = -1;
  for (int i = 0; i < NUM_BUFFERS; i++)
    {
      ctx->buf[i] = malloc(roundup(size, page_size));
      if (!ctx->buf[i])
        {
          fprintf(stderr, "Couldn't allocate work buf.\n");
          return NULL;
        }
//    memset(ctx->buf[i], 0x7b + is_server, size);
      strcpy(ctx->buf[i], "\0");
      ctx->cur_active_wrs[i] = INACTIVE_WR;
    }
//  ctx->buf = malloc(roundup(size, page_size));
//  if (!ctx->buf)
//    {
//      fprintf(stderr, "Couldn't allocate work buf.\n");
//      return NULL;
//    }
//
//  memset(ctx->buf, 0x7b + is_server, size);

  ctx->context = ibv_open_device(ib_dev);
  if (!ctx->context)
    {
      fprintf(stderr, "Couldn't get context for %s\n",
              ibv_get_device_name(ib_dev));
      return NULL;
    }

  if (use_event)
    {
      ctx->channel = ibv_create_comp_channel(ctx->context);
      if (!ctx->channel)
        {
          fprintf(stderr, "Couldn't create completion channel\n");
          return NULL;
        }
    }
  else
    ctx->channel = NULL;

  ctx->pd = ibv_alloc_pd(ctx->context);
  if (!ctx->pd)
    {
      fprintf(stderr, "Couldn't allocate PD\n");
      return NULL;
    }

  // added stuff 2
  for (int i = 0; i < NUM_BUFFERS; i++)
    {
      ctx->mr[i] = ibv_reg_mr(ctx->pd, ctx->buf[i], size,
                              IBV_ACCESS_LOCAL_WRITE
                              | IBV_ACCESS_REMOTE_WRITE);
      if (!ctx->mr[i])
        {
          fprintf(stderr, "Couldn't register MR\n");
          return NULL;
        }
    }

  ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                          ctx->channel, 0);
  if (!ctx->cq)
    {
      fprintf(stderr, "Couldn't create CQ\n");
      return NULL;
    }

  {
    struct ibv_qp_init_attr attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap     = {
            .max_send_wr  = tx_depth,
            .max_recv_wr  = rx_depth,
            .max_send_sge = 1,
            .max_recv_sge = 1
        },
        .qp_type = IBV_QPT_RC
    };

    ctx->qp = ibv_create_qp(ctx->pd, &attr);
    if (!ctx->qp)
      {
        fprintf(stderr, "Couldn't create QP\n");
        return NULL;
      }
  }

  {
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE
    };

    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_PKEY_INDEX |
                      IBV_QP_PORT |
                      IBV_QP_ACCESS_FLAGS))
      {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        return NULL;
      }
  }

  return ctx;
}
int pp_close_ctx(struct pingpong_context *ctx)
{
  if (ibv_destroy_qp(ctx->qp))
    {
      fprintf(stderr, "Couldn't destroy QP\n");
      return 1;
    }

  if (ibv_destroy_cq(ctx->cq))
    {
      fprintf(stderr, "Couldn't destroy CQ\n");
      return 1;
    }

//  if (ibv_dereg_mr(ctx->mr))
//    {
//      fprintf(stderr, "Couldn't deregister MR\n");
//      return 1;
//    }
  // added stuff 3
  for (int i = 0; i < NUM_BUFFERS; i++)
    {
      if (ibv_dereg_mr(ctx->mr[i]))
        {
          fprintf(stderr, "Couldn't deregister MR\n");
          return 1;
        }
    }

  if (ibv_dealloc_pd(ctx->pd))
    {
      fprintf(stderr, "Couldn't deallocate PD\n");
      return 1;
    }

  if (ctx->channel)
    {
      if (ibv_destroy_comp_channel(ctx->channel))
        {
          fprintf(stderr, "Couldn't destroy completion channel\n");
          return 1;
        }
    }

  if (ibv_close_device(ctx->context))
    {
      fprintf(stderr, "Couldn't release context\n");
      return 1;
    }

// added stuff 4
  for (int i = 0; i < NUM_BUFFERS; i++)
    {
      free(ctx->buf[i]);
    }
//  free(ctx->buf);
  free(ctx);

  return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, sender_type poster)
{
  int i;
  if (poster == CLIENT)
    {
      if (ctx->cur_active_wrs[CLIENT_RECEIVE_BUFFER] == ACTIVE_WR) return SUCCESS;
      if (create_post_recv(ctx, CLIENT_RECEIVE_BUFFER) == FAILURE) return FAILURE;
      ctx->cur_active_wrs[CLIENT_RECEIVE_BUFFER] = ACTIVE_WR;
    }
  else
    {
      for (i = 0; i < NUM_BUFFERS_SEND_AND_RECEIVE; ++i)
        {
          // Cycle through the buffers for each post_recv;
          if (ctx->cur_active_wrs[i] == ACTIVE_WR) continue;
          if (create_post_recv(ctx, i) == FAILURE) return FAILURE;
          ctx->cur_active_wrs[i] = ACTIVE_WR;
        }
    }

  return i;
}

static int create_post_recv(struct pingpong_context *ctx, int i)
{
  struct ibv_sge list = {
      .addr    = (uintptr_t) ctx->buf[i],
      .length = ctx->size,
      .lkey    = ctx->mr[i]->lkey
  };
  struct ibv_recv_wr wr = {
      .wr_id      = PINGPONG_RECV_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .next       = NULL
  };
  struct ibv_recv_wr *bad_wr;
  if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
    {
      fprintf(stderr, "-------Couldn't post recv for i = %d ---------\n", i);
      return FAILURE;
    }
  return SUCCESS;
}

static int pp_post_send(struct pingpong_context *ctx, sender_type sender)
{
  int buffer_index;
  if (sender == CLIENT)
    {
      ctx->cur_buf = (ctx->cur_buf + 1) % NUM_BUFFERS_SEND_AND_RECEIVE;
      buffer_index = ctx->cur_buf;
    }
  else
    {
      buffer_index = SERVER_SEND_BUFFER;
    }
  struct ibv_sge list = {
      .addr    = (uintptr_t) ctx->buf[buffer_index],
      .length = ctx->size,
      .lkey    = ctx->mr[buffer_index]->lkey
  };
  struct ibv_send_wr *bad_wr, wr = {
      .wr_id        = PINGPONG_SEND_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED,
      .next       = NULL
  };
  return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

int pp_wait_completions(struct pingpong_context *ctx, int iters)
{
  int rcnt = 0, scnt = 0;
  while (rcnt + scnt < iters)
    {
      struct ibv_wc wc[WC_BATCH];
      int ne, i;

      do
        {
          ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
          if (ne < 0)
            {
              fprintf(stderr, "sized %d\n", ne);
              return 1;
            }

        }
      while (ne < 1);
      for (i = 0; i < ne; ++i)
        {
          if (wc[i].status != IBV_WC_SUCCESS)
            {
              fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                      ibv_wc_status_str(wc[i].status),
                      wc[i].status, (int) wc[i].wr_id);
              return 1;
            }

          switch ((int) wc[i].wr_id)
            {
              case PINGPONG_SEND_WRID:
                ++scnt;
              break;

              case PINGPONG_RECV_WRID:
                ++rcnt;
              break;

              default:
                fprintf(stderr, "Completion for unknown wr_id %d\n",
                        (int) wc[i].wr_id);
              return 1;
            }
        }

    }
  return 0;
}

static void usage(const char *argv0)
{
  printf("Usage:\n");
  printf("  %s            start a server and wait for connection\n", argv0);
  printf("  %s <host>     connect to server at <host>\n", argv0);
  printf("\n");
  printf("Options:\n");
  printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
  printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
  printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
  printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
  printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
  printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
  printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
  printf("  -l, --sl=<sl>          service level value\n");
  printf("  -e, --events           sleep on CQ events (default poll)\n");
  printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

//-------------- API functions -----------------//

/* Connect to server */
int kv_open(char *servername, void **kv_handle)
{
  struct pingpong_context **all_ctx = (struct pingpong_context **) kv_handle;
  struct ibv_device **dev_list;
  struct ibv_device *ib_dev;
  struct pingpong_context *ctx;
  struct pingpong_dest my_dest;
  struct pingpong_dest *rem_dest;
  char *ib_devname = NULL;
  int port = PORT_NUM;
  int ib_port = 1;
  enum ibv_mtu mtu = IBV_MTU_4096;
  int rx_depth = NUM_BUFFERS_SEND_AND_RECEIVE;
  int tx_depth = NUM_BUFFERS_SEND_AND_RECEIVE;
  int iters = 1000;
  int use_event = 0;
  int size = BUFFER_SIZE;
  int sl = 0;
  int gidx = -1;
  char gid[33];

  srand48(getpid() * time(NULL));

  while (1)
    {
      int c;

      static struct option long_options[] = {
          {.name = "port", .has_arg = 1, .val = 'p'},
          {.name = "ib-dev", .has_arg = 1, .val = 'd'},
          {.name = "ib-port", .has_arg = 1, .val = 'i'},
          {.name = "size", .has_arg = 1, .val = 's'},
          {.name = "mtu", .has_arg = 1, .val = 'm'},
          {.name = "rx-depth", .has_arg = 1, .val = 'r'},
          {.name = "iters", .has_arg = 1, .val = 'n'},
          {.name = "sl", .has_arg = 1, .val = 'l'},
          {.name = "events", .has_arg = 0, .val = 'e'},
          {.name = "gid-idx", .has_arg = 1, .val = 'g'},
          {0}
      };

      c = getopt_long(all_argc, all_argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
      if (c == -1)
        break;

      switch (c)
        {
          case 'p':
            port = strtol(optarg, NULL, 0);
          if (port < 0 || port > 65535)
            {
              usage(all_argv[0]);
              return 1;
            }
          break;

          case 'd':
            ib_devname = strdup(optarg);
          break;

          case 'i':
            ib_port = strtol(optarg, NULL, 0);
          if (ib_port < 0)
            {
              usage(all_argv[0]);
              return 1;
            }
          break;

          case 's':
            size = strtol(optarg, NULL, 0);
          break;

          case 'm':
            mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
          if (mtu < 0)
            {
              usage(all_argv[0]);
              return 1;
            }
          break;

          case 'r':
            rx_depth = strtol(optarg, NULL, 0);
          break;

          case 'n':
            iters = strtol(optarg, NULL, 0);
          break;

          case 'l':
            sl = strtol(optarg, NULL, 0);
          break;

          case 'e':
            ++use_event;
          break;

          case 'g':
            gidx = strtol(optarg, NULL, 0);
          break;

          default:
            usage(all_argv[0]);
          return 1;
        }
    }


  page_size = sysconf(_SC_PAGESIZE);

  dev_list = ibv_get_device_list(NULL);
  if (!dev_list)
    {
      perror("Failed to get IB devices list");
      return 1;
    }

  if (!ib_devname)
    {
      ib_dev = *dev_list;
      if (!ib_dev)
        {
          fprintf(stderr, "No IB devices found\n");
          return 1;
        }
    }
  else
    {
      int i;
      for (i = 0; dev_list[i]; ++i)
        if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
          break;
      ib_dev = dev_list[i];
      if (!ib_dev)
        {
          fprintf(stderr, "IB device %s not found\n", ib_devname);
          return 1;
        }
    }

  ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
  if (!ctx)
    return 1;
  sender_type sender = CLIENT;
  if (!servername)
    sender = SERVER;
  pp_post_recv(ctx, sender);

  if (use_event)
    if (ibv_req_notify_cq(ctx->cq, 0))
      {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return 1;
      }

  if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo))
    {
      fprintf(stderr, "Couldn't get port info\n");
      return 1;
    }

  my_dest.lid = ctx->portinfo.lid;
  if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid)
    {
      fprintf(stderr, "Couldn't get local LID\n");
      return 1;
    }

  if (gidx >= 0)
    {
      if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid))
        {
          fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
          return 1;
        }
    }
  else
    memset(&my_dest.gid, 0, sizeof my_dest.gid);

  my_dest.qpn = ctx->qp->qp_num;
  my_dest.psn = lrand48() & 0xffffff;
  inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
  printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         my_dest.lid, my_dest.qpn, my_dest.psn, gid);

  if (servername)
    rem_dest = pp_client_exch_dest(servername, port, &my_dest);
  else
    rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

  if (!rem_dest)
    return 1;

  inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
  printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

  if (servername)
    if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
      return 1;

  *all_ctx = ctx;
  ibv_free_device_list(dev_list);
  free(rem_dest);
  return 0;
}

/* Called after get() on value pointer */
void kv_release(char *value)
{
  free(value);
}

/* Destroys the QP */
int kv_close(void *kv_handle)
{
  return pp_close_ctx(kv_handle);
}

//Given a key and a value – store it on the server (possibly overwriting the old value).
//The server can store the key-value mapping in a simple array (or any other data), to simplify
//implementation.
int kv_set(void *kv_handle, const char *key, const char *value)
{
  struct pingpong_context *ctx = (struct pingpong_context *) kv_handle;

  // get value size > 4096 -> create control message to server with new size needed,
  // and server returns a new Query with the new size of value allocated
  if (strlen(value) > MAX_EAGER_SIZE)
    {
      return client_set_rendezvous(ctx, key, value);
    }
  return client_set_eager(ctx, key, value);

}

// Given a (string) key – retrieve the (string) value from the server (default is “”).
int kv_get(void *kv_handle, const char *key, char **value)
{
  struct pingpong_context *ctx = (struct pingpong_context *) kv_handle;
  Query *query = (Query *) malloc(sizeof(Query));
  if (!query)
    {
      return FAILURE;
    }
  strcpy(query->key, (char *) key);
  query->value = NULL;
  query->request = GET;
  query->protocol = EAGER; // doesn't matter
  query->value_len = 0;
  if (send_query(ctx, query, CLIENT) == FAILURE)
    {
      return FAILURE;
    }
  free(query);
  Query *rec_q = receive_query(ctx);
  if (rec_q == NULL)
    {
      return FAILURE;
    }
  // key not found
  if (rec_q->value_len == 0)
    {
      *value = (char *) malloc(sizeof(char));
      if (!*value)
        {
          free(rec_q->value);
          free(rec_q);
          query = NULL;
          return FAILURE;
        }
      (*value)[0] = '\0';
      free(rec_q->value);
      free(rec_q);
      query = NULL;
      return SUCCESS;
    }
  *value = (char *) malloc(sizeof(char) * rec_q->value_len + 1);
  if (!*value)
    {
      free(rec_q->value);
      free(rec_q);
      query = NULL;
      return FAILURE;
    }
  if (rec_q->protocol == RENDEZVOUS)
    {
      if (client_get_rendezvous(ctx, key, value, rec_q) == FAILURE)
        {
          free(query);
          query = NULL;
          return FAILURE;
        }
      return SUCCESS;
    }


  // EAGER CASE
  strcpy(*value, rec_q->value);
  free(rec_q->value);
  free(rec_q);
  query = NULL;
  return SUCCESS;

}

//-------------- helper functions -----------------//

int find_query_in_db(Queries *queries, char *key)
{
  int i = 0;
  for (; i < queries->size; i++)
    {
      if (strcmp(queries->kv_table[i].key, key) == 0)
        {
          return i;
        }
    }
  return i;
}

int
pp_wait_completions_server(struct server_context *contexts, int* chosen_client)
{
  int rcnt = 0, scnt = 0; // Counter for receive completions and send completions
  while (rcnt + scnt < 1)
    {
      struct ibv_wc wc[WC_BATCH];
      int ne, i;
      do
        {
          int start = rand() % contexts->num_clients;
          for (int j = start; j < contexts->num_clients + start; j++)
            {
              int cur_i = j % contexts->num_clients;
              ne = ibv_poll_cq(contexts->client_contexts[cur_i]->cq, WC_BATCH, wc);
              if (ne < 0)
                {
                  fprintf(stderr, "poll CQ failed %d\n", ne);
                  return 1;
                }
              if (ne > 0)
                {
                  *chosen_client = cur_i;
                  break;
                }
            }
        }
      while (ne < 1);
      for (i = 0; i < ne; ++i)
        {
          if (wc[i].status != IBV_WC_SUCCESS)
            {
              fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                      ibv_wc_status_str(wc[i].status),
                      wc[i].status, (int) wc[i].wr_id);
              return 1;
            }

          switch ((int) wc[i].wr_id)
            {
              case PINGPONG_SEND_WRID:
                ++scnt;
              break;

              case PINGPONG_RECV_WRID:
                ++rcnt;
              break;

              default:
                fprintf(stderr, "Completion for unknown wr_id %d\n",
                        (int) wc[i].wr_id);
              return 1;
            }
        }
    }
  return 0;
}

// this is pp_post_send with custom opcode and RDMA protocol
static int
pp_post_send_with_RDMA(struct pingpong_context *ctx, int opcode, struct ibv_mr *local_mr, uint64_t remote_addr, uint32_t remote_key)
{
  struct ibv_sge list = {
      .addr    = (uint64_t) local_mr->addr,
      .length = local_mr->length,
      .lkey    = local_mr->lkey
  };

  struct ibv_send_wr *bad_wr, wr = {
      .wr_id        = PINGPONG_SEND_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = opcode,
      .send_flags = IBV_SEND_SIGNALED,
      .next       = NULL,
      .wr.rdma.remote_addr = remote_addr,
      .wr.rdma.rkey = remote_key
  };

  return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

int send_query(struct pingpong_context *ctx, Query *query, sender_type sender)
{
  int buf_to_use = (ctx->cur_buf + 1) % NUM_BUFFERS_SEND_AND_RECEIVE;
  if (sender == SERVER){
      buf_to_use = SERVER_SEND_BUFFER;
    }

  if (query->value != NULL && strcmp(query->value, "ACK") == 0)
    {
      sprintf(ctx->buf[buf_to_use], "%s:%s:%d:%c",
              query->key, query->value, query->request, '\0');
    }

  else if (query->value_len == 0)
    {
      sprintf(ctx->buf[buf_to_use], "%s:0:%d:%d:0:%c",
              query->key, query->request, query->protocol, '\0');
    }

  else
    {
      sprintf(ctx->buf[buf_to_use], "%s:%s:%d:%d:%d:%c",
              query->key, query->value, query->request, query->protocol, query->value_len, '\0');
    }

  if (pp_post_send(ctx, sender))
    {
      fprintf(stderr, "Couldn't post send\n");
      return FAILURE;
    }

  if (pp_wait_completions(ctx, 1))
    {
      fprintf(stderr, "Couldn't get completions\n");
      return FAILURE;
    }
  return SUCCESS;
}

Query *parse_query(struct pingpong_context *ctx, int buffer_index, sender_type sender)
{
  Query *query = (Query *) malloc(sizeof(Query));
  if (!query)
    {
      return NULL;
    }
  char *key = strtok(ctx->buf[buffer_index], ":");
  strcpy(query->key, key);

  char *value = strtok(NULL, ":");

  char *request = strtok(NULL, ":");
  query->request = atoi(request);

  if (strcmp(value, "ACK") != 0)
    {
      char *protocol = strtok(NULL, ":");
      query->protocol = atoi(protocol);

      char *value_len = strtok(NULL, ":");
      query->value_len = atoi(value_len);
    }
  else
    {
      query->protocol = RENDEZVOUS;
      query->value_len = ACK_MSG_VALUE_SIZE;
    }

  query->value = (char *) malloc(sizeof(char) * query->value_len + 1);
  if (!query->value)
    {
      free(query);
      return NULL;
    }
  strcpy(query->value, value);

  ctx->cur_active_wrs[buffer_index] = INACTIVE_WR;
  strcpy(ctx->buf[buffer_index], "\0");
  pp_post_recv(ctx, sender);
  return query;
}

Query *receive_query(struct pingpong_context *ctx)
{
  if (pp_wait_completions(ctx, 1))
    {
      fprintf(stderr, "Couldn't get completions\n");
      return NULL;
    }
  return parse_query(ctx, CLIENT_RECEIVE_BUFFER, CLIENT);
}

Queries *initialize_data()
{
  Queries *queries = (Queries *) malloc(sizeof(Queries));
  if (!queries)
    {
      printf("failed to allocate queries\n");
      return NULL;
    }
  queries->size = 0;
  queries->capacity = 16;
  queries->kv_table = (Query *) malloc(sizeof(Query) * queries->capacity);
  if (!queries->kv_table)
    {
      printf("failed to allocate kv_table\n");
      free(queries);
      return NULL;
    }
  return queries;
}

int server_request_manager(void *kv_handle, int client, Queries *queries, Queries *pending_queries, Pending_Dereg_List* get_dereg_list)
{
  struct pingpong_context *server_ctx = (struct pingpong_context *) kv_handle;
  for (int i = 0; i < NUM_BUFFERS_SEND_AND_RECEIVE; i++)
    {
      if (strcmp(server_ctx->buf[i], ""))
        {
          Query *query = parse_query(server_ctx, i, SERVER);
          if (!query)
            {
              return FAILURE;
            }
          if (strcmp(query->value, "ACK") == 0)
            {
              if (query->request == SET)
                {
                  server_set_rendezvous_second_phase(server_ctx, query, queries, pending_queries);
                }
              dereg_pending_request(server_ctx, query, get_dereg_list, client);
            }
          else if (query->request == GET)
            {
              int ind = find_query_in_db(queries, query->key);
              if (ind == queries->size)
                {
                  printf("key not found\n");
                  query->value_len = 0;
                  send_query(server_ctx, query, SERVER);
                  return SUCCESS;
                }
              if (queries->kv_table[ind].protocol == EAGER)
                {
                  if (send_query(server_ctx, &queries->kv_table[ind], SERVER)
                      == FAILURE)
                    {
                      return FAILURE;
                    }
                  free(query->value);
                  free(query);
                  return SUCCESS;
                }
              server_get_rendezvous(server_ctx, &queries->kv_table[ind], get_dereg_list, client);
            }
          else if (query->request == SET)
            {
              if (query->protocol == EAGER)
                {
                  if (insert_query_to_db(query, (void *) query->value, queries)
                      != SUCCESS)
                    {
                      return FAILURE;
                    }
                }
              else if (query->protocol == RENDEZVOUS)
                {
                  if (server_set_rendezvous(server_ctx, (void *) query, pending_queries, get_dereg_list, client)
                      != SUCCESS)
                    {
                      printf("failed to set value\n");
                      return FAILURE;
                    }
                }
            }
          free(query->value);
          free(query);
        }
    }
  return SUCCESS;
}
void
dereg_pending_request(struct pingpong_context *ctx, Query *query, Pending_Dereg_List *get_dereg_list, int client)
{
  int i;
  for (i = 0; i < get_dereg_list->size; i++)
    {
      if (strcmp(get_dereg_list->get_dereg_list[i]->key, query->key) == 0 && get_dereg_list->get_dereg_list[i]->client_id == client)
        {
          ibv_dereg_mr(get_dereg_list->get_dereg_list[i]->mr);
          free(get_dereg_list->get_dereg_list[i]);
          break;
        }
    }
  // update dereg list
  for (int j = i; j < get_dereg_list->size - 1; j++)
    {
      get_dereg_list->get_dereg_list[j] = get_dereg_list->get_dereg_list[j + 1];
    }
  get_dereg_list->size--;
}
void
server_set_rendezvous_second_phase(struct pingpong_context *ctx, Query *query, Queries *queries, Queries *pending_queries)
{
  int ind = find_query_in_db(pending_queries, query->key);
  if (ind == pending_queries->size)
    {
      return;
    }
  if (insert_query_to_db(&pending_queries->kv_table[ind], (void *) pending_queries->kv_table[ind].value, queries)
      != SUCCESS)
    {
      return;
    }
  for (int i = ind; i < pending_queries->size - 1; i++)
    {
      pending_queries->kv_table[i] = pending_queries->kv_table[i + 1];
    }
  pending_queries->size--;
}

void print_queries(Queries *p_queries)
{
  printf("beginning print queries\n");
  printf("size = %d\n", p_queries->size);
  for (int i = 0; i < p_queries->size; i++)
    {
      printf("key = %s\n", p_queries->kv_table[i].key);
      printf("value = %.10s\n", p_queries->kv_table[i].value);
      printf("value_len = %d\n", p_queries->kv_table[i].value_len);
      printf("protocol = %d\n", p_queries->kv_table[i].protocol);
    }
  printf("ending print queries\n");
}

//----------------EAGER-----------------//

int
client_set_eager(struct pingpong_context *ctx, const char *key, const char *value)
{
  Query *query = (Query *) malloc(sizeof(Query));
  if (!query)
    {
      return FAILURE;
    }
  query->value = (char *) malloc(sizeof(char) * strlen(value) + 1);
  if (!query->value)
    {
      free(query);
      return FAILURE;
    }
  // Copy key and value into the query struct
  strcpy(query->key, key);
  strcpy(query->value, value);

  // Set the request and protocol fields
  query->request = SET;
  query->protocol = EAGER;
  query->value_len = strlen(value);
  if (send_query(ctx, query, CLIENT) == FAILURE)
    {
      return FAILURE;
    }
  free(query->value);
  free(query);
  return SUCCESS;
}


int insert_query_to_db(Query *query, void *value, Queries *queries)
{
  char *key = query->key;
  int value_len = query->value_len;
  int ind = find_query_in_db(queries, key);
  if (queries->size == queries->capacity)
    {
      printf("server realloc\n");
      queries->capacity *= 2;
      Query *temp = (Query *) realloc(queries->kv_table,
                                      sizeof(Query) * queries->capacity);
      if (temp != NULL)
        {
          queries->kv_table = temp;
        }
      else
        {
          printf("Failed to reallocate memory");
          queries->capacity /= 2;
          return FAILURE;
        }
    }
  // replacing the value of the key if it already exists
  if (ind < queries->size)
    {
      free(queries->kv_table[ind].value);
    }
  else
    {
      strcpy(queries->kv_table[ind].key, key);
    }

  queries->kv_table[ind].value_len = value_len;
  if (value_len > MAX_EAGER_SIZE)
    {
      queries->kv_table[ind].protocol = RENDEZVOUS;
      queries->kv_table[ind].value = value;
    }
  else
    {
      queries->kv_table[ind].protocol = EAGER;
      queries->kv_table[ind].value = (char *) malloc(
          sizeof(char) * value_len + 1);
      strcpy(queries->kv_table[ind].value, value);
    }

  if (ind == queries->size)
    {
      queries->size++;
    }
  return SUCCESS;
}

//--------------- RENDEZVOUS -----------------//
int
server_set_rendezvous(struct pingpong_context *ctx, Query *query, Queries *pending_queries, Pending_Dereg_List *pending_dereg_list
    , int client_id)
{
  // Step 1: Allocate memory for the value
  char *value = (char *) malloc(query->value_len + 1);
  if (!value) return FAILURE;


  // Step 2: Register memory region with RDMA write permissions
  struct ibv_mr *mr = ibv_reg_mr(ctx->pd, value,
                                 query->value_len + 1, IBV_ACCESS_REMOTE_WRITE
                                                       | IBV_ACCESS_LOCAL_WRITE);
  if (!mr) return FAILURE;
  sprintf(ctx->buf[SERVER_SEND_BUFFER], "%u|%lu", mr->rkey, (u_int64_t) mr->addr);

  // Step 3: Wait for permissions to be sent to the client
  if (pp_post_send(ctx, SERVER))
    {
      fprintf(stderr, "Couldn't post send\n");
      return FAILURE;
    }
  if (pp_wait_completions(ctx, 1))
    {
      return FAILURE;
    }

  // Step 4: insert the query to the pending queries and the mr to the pending mrs
  // and move on to work on other requests
  Query* pending_query = (Query *) malloc(sizeof(Query));
  if (!pending_query) return FAILURE;
  strcpy(pending_query->key, query->key);
  pending_query->value = value;
  pending_query->value_len = query->value_len;
  pending_query->protocol = RENDEZVOUS;
  pending_query->request = SET;
  if (insert_query_to_db(pending_query, value, pending_queries) != SUCCESS) return FAILURE;
  free(pending_query);

  Pending_Dereg *pending_dereg = (Pending_Dereg *) malloc(sizeof(Pending_Dereg));
  if (!pending_dereg) return FAILURE;
  strcpy(pending_dereg->key, query->key);
  pending_dereg->mr = mr;
  pending_dereg->client_id = client_id;
  pending_dereg_list->get_dereg_list[pending_dereg_list->size] = pending_dereg;
  pending_dereg_list->size++;
  return SUCCESS;
}

int
client_set_rendezvous(struct pingpong_context *ctx, const char *key, const char *value)
{
  // Send the server the control msg with the size of value needed
  Query *query = (Query *) malloc(sizeof(Query));
  if (!query)
    {
      return FAILURE;
    }
  query->request = SET;
  query->protocol = RENDEZVOUS;
  query->value_len = strlen(value);
  query->value = (char *) calloc(sizeof(char), 2);
  char *temp = "0";
  strcpy(query->value, temp);
  query->value[1] = '\0';
  strcpy(query->key, key);

  // Sending the query to the server in order to get the new value size with the RDMA
  if (send_query(ctx, query, CLIENT) == FAILURE)
    {
      return FAILURE;
    }


  // Variables to hold server MR info
  uint64_t remote_addr;
  uint32_t remote_key;

  // Receive server MR info
  if (pp_wait_completions(ctx, 1))
    {
      return FAILURE;
    }
  sscanf(ctx->buf[CLIENT_RECEIVE_BUFFER], "%u|%lu", &remote_key, &remote_addr);
  ctx->cur_active_wrs[CLIENT_RECEIVE_BUFFER] = INACTIVE_WR;
  strcpy(ctx->buf[CLIENT_RECEIVE_BUFFER], "\0");
  pp_post_recv(ctx, CLIENT);

  // Register client memory for RDMA write
  struct ibv_mr *client_mr = ibv_reg_mr(ctx->pd, (void *) value, query->value_len + 1,
                                        IBV_ACCESS_REMOTE_READ
                                        | IBV_ACCESS_LOCAL_WRITE
                                        | IBV_ACCESS_REMOTE_WRITE);
  if (!client_mr)
    {
      return FAILURE;
    }

  // Perform RDMA write
  if (pp_post_send_with_RDMA(ctx, IBV_WR_RDMA_WRITE, client_mr, remote_addr, remote_key)
      == FAILURE)
    {
      return FAILURE;
    }
  if (pp_wait_completions(ctx, 1)) return FAILURE;

  // Send RDMA ACK to the server
  Query *ack_query = (Query *) malloc(sizeof(Query));
  if (!ack_query)
    {
      return FAILURE;
    }
  ack_query->request = SET;
  ack_query->value = (char *) malloc(ACK_MSG_VALUE_SIZE + 1);
  strcpy(ack_query->value, "ACK");
  strcpy(ack_query->key, key);
  if (send_query(ctx, ack_query, CLIENT) == FAILURE)
    {
      return FAILURE;
    }

  ibv_dereg_mr(client_mr);
  free(query->value);
  free(query);
  free(ack_query->value);
  free(ack_query);
  return SUCCESS;
}

int server_get_rendezvous(struct pingpong_context *ctx, Query *query, Pending_Dereg_List* get_dereg_list, int client_id)
{

  // Send control message to client
  Query *response = (Query *) malloc(sizeof(Query));
  if (!response)
    {
      return FAILURE;
    }
  response->request = GET;
  response->protocol = RENDEZVOUS;
  response->value_len = query->value_len;
  struct ibv_mr *mr = ibv_reg_mr(ctx->pd, query->value,
                                 query->value_len + 1, IBV_ACCESS_REMOTE_READ);
  if (!mr)
    {
      return FAILURE;
    }

  int permissions_len =
      snprintf(NULL, 0, "%u|%lu", mr->rkey, (uint64_t) mr->addr) + 1;
  response->value = (char *) malloc(permissions_len);
  sprintf(response->value, "%u|%lu", mr->rkey, (u_int64_t) mr->addr);
  strcpy(response->key, query->key);
  if (send_query(ctx, response, SERVER) == FAILURE)
    {
      return FAILURE;
    } // Send the client the control msg with the permissions of value needed

  // insert the mr to the pending mrs so we could deregister it later
  Pending_Dereg *get_dereg = (Pending_Dereg *) malloc(sizeof(Pending_Dereg));
  get_dereg->mr = mr;
  get_dereg->client_id = client_id;
  strcpy(get_dereg->key, query->key);
  get_dereg_list->get_dereg_list[get_dereg_list->size] = get_dereg;
  get_dereg_list->size++;

  free(response->value);
  free(response);
  return SUCCESS;
}

int
client_get_rendezvous(struct pingpong_context *ctx, const char *key, char **value, Query *query)
{
  // Step 1: Register memory with RDMA write permissions
  struct ibv_mr *mr = ibv_reg_mr(ctx->pd, *value,
                                 query->value_len + 1, IBV_ACCESS_REMOTE_WRITE
                                                       | IBV_ACCESS_LOCAL_WRITE);
  if (!mr) return FAILURE;

  // Step 2: Wait for server to send permissions
  // Variables to hold server MR info
  uint64_t remote_addr;
  uint32_t remote_key;

  // read the permissions from the value of the query
  sscanf(query->value, "%u|%lu", &remote_key, &remote_addr);

  if (pp_post_send_with_RDMA(ctx, IBV_WR_RDMA_READ, mr, remote_addr, remote_key)
      == FAILURE)
    {
      return FAILURE;
    }
  if (pp_wait_completions(ctx, 1)) return FAILURE;

  // Step 3: Tell the server that the client has finished reading the value
  Query *ack_query = (Query *) malloc(sizeof(Query));
  if (!ack_query)
    {
      return FAILURE;
    }
  ack_query->request = GET;
  ack_query->value = (char *) malloc(ACK_MSG_VALUE_SIZE + 1);
  strcpy(ack_query->value, "ACK");
  strcpy(ack_query->key, key);
  if (send_query(ctx, ack_query, CLIENT) == FAILURE)
    {
      return FAILURE;
    }

  free(query->value);
  free(query);
  free (ack_query->value);
  free (ack_query);
  ibv_dereg_mr(mr);

  return SUCCESS;
}

//-------------- main func -----------------//
#define KB 1024

int set_get(void *kv_handle, char *key, char *value)
{
//  sleep (2);
  int result = kv_set(kv_handle, key, value);
  if (result == FAILURE)
    {
      return FAILURE;
    }

  char *value_retrieved;
//  sleep (1);
  result = kv_get(kv_handle, key, &value_retrieved);
  if (result == FAILURE)
    {
      return FAILURE;
    }
  if (strcmp(value, value_retrieved) != 0)
    {
      printf("compare failed\n");
      return FAILURE;
    }
  kv_release(value_retrieved);
  return EXIT_SUCCESS;
}

void test_2(void *kv_handle)
{
  printf("Test 2: Set and Get in Eager mode\n");

  // Set + Get
  char *key = "key";
  char *value = "value";
  char *getter = "";
  int result = kv_set(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 2: Failed\n");
      return;
    }
  result = kv_get(kv_handle, key, &getter);
  if (result == FAILURE)
    {
      printf("Test 2: Failed\n");
      return;
    }
  if (strcmp(value, getter) != 0)
    {
      printf("Test 2: Failed\n");
      return;
    }
  free(getter);
//   Set + Get
  char *key2 = "key2";
  char *value2 = "value2";
  result = kv_set(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 2: Failed\n");
      return;
    }
  result = kv_get(kv_handle, key, &getter);
  if (result == FAILURE)
    {
      printf("Test 2: Failed\n");
      return;
    }
  if (strcmp(value, getter) != 0)
    {
      printf("Test 2: Failed\n");
      return;
    }
  free(getter);
  printf("Test 2: Passed\n");
}

void test_3(void *kv_handle)
{
  printf("Test 3: Double Set and Get (different keys) in Eager mode\n");

  // Set + Get 1
  char *key = "key";
  char *value = "value";
  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 3: Failed\n");
      return;
    }

  // Set + Get 2
  char *key2 = "key2";
  char *value2 = "value2";
  result = set_get(kv_handle, key2, value2);
  if (result == FAILURE)
    {
      printf("Test 3: Failed\n");
      return;
    }

  printf("Test 3: Passed\n");
}

void test_4(void *kv_handle)
{
  printf("Test 4: Double Set and Get (same keys) in Eager mode\n");


  // Set + Get 1
  char *key = "key";
  char *value = "value";
  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 4: Failed\n");
      return;
    }

//  sleep(2);

  // Set + Get 2
  char *key2 = "key";
  char *value2 = "value2";
  result = set_get(kv_handle, key2, value2);
  if (result == FAILURE)
    {
      printf("Test 4: Failed\n");
      return;
    }

  printf("Test 4: Passed\n");
}

void test_5(void *kv_handle)
{
  printf("Test 5: Set and Get in Rendezvous mode\n");

  // Set + Get
  char *key = "key";
  char *value = (char *) malloc(4 * KB + 11);
  memset(value, 'a', 4 * KB + 10);
  value[4 * KB + 10] = '\0';

  int result = set_get(kv_handle, key, value);
//  int result = SUCCESS;
  if (result == FAILURE)
    {
      printf("Test 5: Failed\n");
      return;
    }

  //deallocating memory
  free(value);
  printf("Test 5: Passed\n");
}

void test_6(void *kv_handle)
{
  printf("Test 6: Double Set and Get (different keys) in Rendezvous mode\n");

  // Set + Get 1
  char *key = "key";
  char *value = (char *) malloc(4 * KB + 11);
  memset(value, 'a', 4 * KB + 10);
  value[4 * KB + 10] = '\0';

  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 6: Failed\n");
      return;
    }
  sleep(2);
  // Set + Get 2
  char *key2 = "key2";
  char *value2 = (char *) malloc(4 * KB + 11);
  memset(value2, 'b', 4 * KB + 10);
  result = set_get(kv_handle, key2, value2);
  if (result == FAILURE)
    {
      printf("Test 6: Failed\n");
      return;
    }

  //deallocating memory
  free(value);
  free(value2);
  printf("Test 6: Passed\n");
}

void test_7(void *kv_handle)
{
  printf("Test 7: Double Set and Get (same keys) in Rendezvous mode\n");

  // Set + Get 1
  char *key = "key";
  char *value = (char *) malloc(4 * KB + 11);
  memset(value, 'a', 4 * KB + 10);
  value[4 * KB + 10] = '\0';
  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 7: Failed\n");
      return;
    }
  sleep(2);
  // Set + Get 2
  char *value2 = (char *) malloc(4 * KB + 11);
  memset(value2, 'b', 4 * KB + 10);
  result = set_get(kv_handle, key, value2);
  if (result == FAILURE)
    {
      printf("Test 7: Failed\n");
      return;
    }

  //deallocating memory
  free(value);
  free(value2);
  printf("Test 7: Passed\n");
}

void test_8(void *kv_handle)
{
  printf("Test 8: Double Set and Get Eager then Rendezvous mode\n");

  // Set + Get 1
  char *key = "key";
  char *value = "value";
  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 8: Failed\n");
      return;
    }

  sleep(2);
  // Set + Get 2
  char *value2 = (char *) malloc(4 * KB + 11);
  memset(value2, 'a', 4 * KB + 10);
  result = set_get(kv_handle, key, value2);
  if (result == FAILURE)
    {
      printf("Test 8: Failed\n");
      return;
    }

  //deallocating memory
  free(value2);
  printf("Test 8: Passed\n");
}

void test_9(void *kv_handle)
{
  printf("Test 9: Double Set and Get Rendezvous then Eager mode\n");

  // Set + Get 1
  char *key = "key";
  char *value = (char *) malloc(4 * KB + 11);
  memset(value, 'a', 4 * KB + 10);
  value[4 * KB + 10] = '\0';
  int result = set_get(kv_handle, key, value);
  if (result == FAILURE)
    {
      printf("Test 9: Failed\n");
      return;
    }
  sleep(2);
  // Set + Get 2
  char *value2 = "value2";
  result = set_get(kv_handle, key, value2);
  if (result == FAILURE)
    {
      printf("Test 9: Failed\n");
      return;
    }

  //deallocating memory
  free(value);
  printf("Test 9: Passed\n");
}

void test_10(void *kv_handle)
{
  printf("Test 10: All types of set and get overrides\n");

  // Set + Get 1
  char *key1 = "key1";
  char *value1 = "value1";
  char *key2 = "key2";
  char *value2 = "value2";
  char *value3 = (char *) malloc(4 * KB + 11);
  memset(value3, 'a', 4 * KB + 10);
  value3[4 * KB + 10] = '\0';
  char *value4 = (char *) malloc(4 * KB + 11);
  memset(value4, 'b', 4 * KB + 10);
  value4[4 * KB + 10] = '\0';
//  sleep (5);
  kv_set(kv_handle, key1, value1);
//  sleep (1);
  kv_set(kv_handle, key2, value2);
//  sleep (1);
//  printf ("beginning key1 second set and key1 = %s\n", key1);
  kv_set(kv_handle, key1, value3);
//  sleep (1);
  kv_set(kv_handle, key2, value4);
//  sleep (1);
  char *get1;
  kv_get(kv_handle, key1, &get1);
//  sleep (1);
//  printf ("get1 = %s\n\n  value3 = %s\n", get1, value3);
  if (strcmp(get1, value3) != 0)
    {
      printf("Test 10: Failed\n");
      return;
    }
  free(get1);

  char *get2;
  kv_get(kv_handle, key2, &get2);
//  printf ("get2 = %s value4 = %s\n", get2, value4);
  if (strcmp(get2, value4) != 0)
    {
      printf("Test 10: Failed\n");
      return;
    }
  free(get2);


  //deallocating memory
  free(value3);
  free(value4);
  printf("Test 10: Passed\n");
}

void test_11(void *kv_handle)
{
  printf("Test 11: get of a non existent key\n");

  char *get2;
  kv_get(kv_handle, "key777777777", &get2);

  printf("get2 = %s\n", get2);
  free(get2);
  printf("Test 11: Passed\n");
}

void test_12(void *kv_handle)
{
  printf("Test 12: Tons of sets\n");

  // Set + Get 1
  char *key1 = "key1";
  char *value1 = "value1";
  char *key2 = "key2";
  char *value2 = "value2";
  char *value3 = (char *) malloc(4 * KB + 10);
  memset(value3, 'a', 4 * KB + 10);
  value3[4 * KB + 10] = '\0';
  char *value4 = (char *) malloc(4 * KB + 10);
  memset(value4, 'b', 4 * KB + 10);
  value4[4 * KB + 10] = '\0';
  for (int i = 0; i < 500; i++)
    {

      char key[10];
      snprintf(key, 10, "Ma%d%c", i, '\0');
//      printf("key = %s\n", key);
      kv_set(kv_handle, key, value1);
    }
//  sleep(2);
  printf("beginning gets of eager\n");

  for (int i = 0; i < 500; i++)
    {
      char *get1;
      char key[10];
      snprintf(key, 10, "Ma%d%c", i, '\0');
//      printf("key = %s\n", key);
      kv_get(kv_handle, key, &get1);
      if (strcmp(get1, value1) != 0)
        {
          printf("Test 12: Failed\n");
          return;
        }
      free(get1);
    }
  for (int i = 0; i < 500; i++)
    {
      char key[10];
      snprintf(key, 10, "Ken%d%c", i, '\0');
      kv_set(kv_handle, key, value4);
//      sleep(1);
    }
  printf("Now gets of rend\n");
  for (int i = 0; i < 500; i++)
    {
      char *get2;
      char key[10];
      snprintf(key, 10, "Ken%d%c", i, '\0');
      kv_get(kv_handle, key, &get2);
//      sleep(1);
      if (strcmp(get2, value4) != 0)
        {
          printf("key = %s\n", key);
          printf("get2 = %s\n", get2);
          printf("value4 = %s\n", value4);
          printf("Test 12: Failed\n");
          return;
        }
      free(get2);
    }
  free(value3);
  free(value4);
  printf("Test 12: Passed\n");
}

void run_tests(char *servername)
{
  void *kv_handle;
  kv_open(servername, &kv_handle);
  sleep(1);

  test_2(kv_handle);
//  sleep(2);
  test_3(kv_handle);
//  sleep(2);
  test_4(kv_handle);
//  sleep(2);
  test_5(kv_handle);
//  sleep(2);
  test_6(kv_handle);
//  sleep(2);
  test_7(kv_handle);
//  sleep(2);
  test_8(kv_handle);
//  sleep(2);
  test_9(kv_handle);
//  sleep(2);
  test_10(kv_handle);
//  sleep(2);
  test_11(kv_handle);
//  sleep(2);
  test_12(kv_handle);
//  sleep(2);
//
//  sleep(8);
  kv_close(kv_handle);
//  test_disconnection(kv_handle);
}

int main(int argc, char *argv[])
{
//  printf ("hi 1");
  char *servername;
  if (optind == argc - 1)
    {
      servername = strdup(argv[optind]);
    }
  else if (optind < argc)
    {
      usage(argv[0]);
      return 1;
    }
//  printf ("hi 2");
  all_argv = argv;
  all_argc = argc;
  if (!servername)
    { //Server
      printf("inside server");
      Queries* queries = initialize_data();
      Queries* pending_queries = initialize_data();
      Pending_Dereg_List * get_dereg_list = initialize_get_dereg();
      server_context *server_ctx = (server_context *) malloc(sizeof(server_context));
      if (!server_ctx)
        {
          return 1;
        }
      server_ctx->num_clients = MAX_CLIENTS;
      server_ctx->client_contexts = (struct pingpong_context **) malloc(
          sizeof(server_context) * server_ctx->num_clients);
      if (!server_ctx->client_contexts)
        {
          free(server_ctx);
          return 1;
        }
      for (int i = 0; i < server_ctx->num_clients; i++)
        {
          if (kv_open(0, (void **) &server_ctx->client_contexts[i]) != 0)
            {
              return 1;
            }
        }

      while (1)
        {
          struct pingpong_context *chosen_ctx;
          int chosen_client;
          pp_wait_completions_server(server_ctx, &chosen_client);
          server_request_manager(server_ctx->client_contexts[chosen_client], chosen_client, queries, pending_queries, get_dereg_list);
        }
      close_server(server_ctx, queries, pending_queries, get_dereg_list);
    }
  else
    {
      printf("inside client\n");
      void *kv_handle;
      run_tests(servername);
      free(servername);
      return 0;
    }
}
Pending_Dereg_List *initialize_get_dereg()
{
  Pending_Dereg_List *get_dereg_list = (Pending_Dereg_List *) malloc(sizeof(Pending_Dereg_List));
  if (!get_dereg_list)
    {
      // Handle allocation failure if needed
      printf("failed to allocate get_dereg_list\n");
      return NULL;
    }
  get_dereg_list->size = 0;
  get_dereg_list->capacity = 16;
  get_dereg_list->get_dereg_list = (Pending_Dereg **) malloc(sizeof(Pending_Dereg*) * get_dereg_list->capacity);
  if (!get_dereg_list->get_dereg_list)
    {
      // Handle allocation failure if needed
      printf("failed to allocate kv_table\n");
      free(get_dereg_list);
      return NULL;
    }
  return get_dereg_list;
}

void close_server(server_context *server_context, Queries *queries, Queries *pending_queries, Pending_Dereg_List *get_dereg_list)
{
  printf("closing server\n");
  for (int i = 0; i < queries->size; i++)
    {
      free(queries->kv_table[i].value);
    }
  free(queries->kv_table);
  free(queries);
  for (int i = 0; i < pending_queries->size; i++)
    {
      free(pending_queries->kv_table[i].value);
    }
  free(pending_queries->kv_table);
  free(pending_queries);

  free(get_dereg_list->get_dereg_list);
  free(get_dereg_list);

  for (int i = 0; i < server_context->num_clients; i++)
    {
      kv_close(server_context->client_contexts[i]);
    }
  free(server_context->client_contexts);
  free(server_context);
}
