/******************************************************************************
 * $Id$
 *
 * Project:  MapServer
 * Purpose:  MapCache tile caching support file: OS-level locking support
 * Author:   Thomas Bonfort and the MapServer team.
 *
 ******************************************************************************
 * Copyright (c) 1996-2011 Regents of the University of Minnesota.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies of this Software or works derived from this Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/

#include "mapcache.h"
#include <apr_file_io.h>
#include <apr_strings.h>
#include <apr_time.h>
#ifndef _WIN32
#include <unistd.h>
#endif

typedef struct {
  mapcache_locker locker;

  /**
   * directory where lock files will be placed.
   * Must be readable and writable by the apache user.
   * Must be placed on a network mounted shared directory if multiple mapcache instances
   * need to be synchronized
   */
  const char *dir;
} mapcache_locker_disk;

typedef struct {
    mapcache_locker locker;
    apr_array_header_t *lockers;
} mapcache_locker_fallback;


#define MAPCACHE_LOCKFILE_PREFIX "_gc_lock"

char* lock_filename_for_resource(mapcache_context *ctx, mapcache_locker_disk *ldisk, const char *resource)
{
  char *saferes = apr_pstrdup(ctx->pool,resource);
  char *safeptr = saferes;
  while(*safeptr) {
    if(*safeptr==' ' || *safeptr == '/' || *safeptr == '~' || *safeptr == '.') {
      *safeptr = '#';
    }
    safeptr++;
  }
  return apr_psprintf(ctx->pool,"%s/"MAPCACHE_LOCKFILE_PREFIX"%s.lck",
                      ldisk->dir,saferes);
}

int mapcache_lock_or_wait_for_resource(mapcache_context *ctx, mapcache_locker *locker, char *resource, void **lock)
{
  mapcache_lock_result rv = locker->aquire_lock(ctx, locker, resource, lock);
  if(GC_HAS_ERROR(ctx)) {
    return MAPCACHE_FAILURE;
  }
  if(rv == MAPCACHE_LOCK_AQUIRED)
    return MAPCACHE_TRUE;
  else {
    apr_time_t start_wait = apr_time_now();
    rv = MAPCACHE_LOCK_LOCKED;

    while(rv != MAPCACHE_LOCK_NOENT) {
      unsigned int waited = apr_time_as_msec(apr_time_now()-start_wait);
      if(waited > locker->timeout*1000) {
        mapcache_unlock_resource(ctx,locker,*lock);
        ctx->log(ctx,MAPCACHE_ERROR,"deleting a possibly stale lock after waiting on it for %g seconds",waited/1000.0);
        return MAPCACHE_FALSE;
      }
      apr_sleep(locker->retry_interval * 1000000);
      rv = locker->ping_lock(ctx,locker, *lock);
    }
    return MAPCACHE_FALSE;
  }
}


mapcache_lock_result mapcache_locker_disk_aquire_lock(mapcache_context *ctx, mapcache_locker *self, char *resource, void **lock) {
  char *lockname, errmsg[120];
  mapcache_locker_disk *ldisk;
  apr_file_t *lockfile;
  apr_status_t rv;

  assert(self->type == MAPCACHE_LOCKER_DISK);
  ldisk = (mapcache_locker_disk*)self;

  lockname = lock_filename_for_resource(ctx,ldisk,resource);
  *lock = lockname;
  
  /* create the lockfile */
  rv = apr_file_open(&lockfile,lockname,APR_WRITE|APR_CREATE|APR_EXCL|APR_XTHREAD,APR_OS_DEFAULT,ctx->pool);

  /* if the file already exists, wait for it to disappear */
  /* TODO: check the lock isn't stale (i.e. too old) */
  if( rv != APR_SUCCESS ) {
    if( !APR_STATUS_IS_EEXIST(rv) ) {
      ctx->set_error(ctx, 500, "failed to create lockfile %s: %s", lockname, apr_strerror(rv,errmsg,120));
      return MAPCACHE_LOCK_NOENT;
    }
    return MAPCACHE_LOCK_LOCKED;
  } else {
    /* we acquired the lock */
    char *pid_s;
    pid_t pid;
    apr_size_t pid_s_len;
    pid = getpid();
    pid_s = apr_psprintf(ctx->pool,"%"APR_PID_T_FMT,pid);
    pid_s_len = strlen(pid_s);
    apr_file_write(lockfile,pid_s,&pid_s_len);
    apr_file_close(lockfile);
    return MAPCACHE_LOCK_AQUIRED;
  }
}

mapcache_lock_result mapcache_locker_disk_ping_lock(mapcache_context *ctx, mapcache_locker *self, void *lock) {
  apr_finfo_t info;
  apr_status_t rv;
  char *lockname;
  lockname = (char*)lock;
  rv = apr_stat(&info,lockname,0,ctx->pool);
  if(APR_STATUS_IS_ENOENT(rv)) {
    return MAPCACHE_LOCK_NOENT;
  } else {
    return MAPCACHE_LOCK_LOCKED;
  }
}

void mapcache_locker_disk_release_lock(mapcache_context *ctx, mapcache_locker *self, void *lock)
{
  char *lockname = (char*)lock;
  apr_file_remove(lockname,ctx->pool);
}

void mapcache_unlock_resource(mapcache_context *ctx, mapcache_locker *locker, void *lock) {
  locker->release_lock(ctx, locker, lock);
}

void mapcache_locker_disk_parse_xml(mapcache_context *ctx, mapcache_locker *self, ezxml_t doc) {
  mapcache_locker_disk *ldisk = (mapcache_locker_disk*)self;
  ezxml_t node;
  if((node = ezxml_child(doc,"directory")) != NULL) {
    ldisk->dir = apr_pstrdup(ctx->pool, node->txt);
  } else {
    ldisk->dir = apr_pstrdup(ctx->pool,"/tmp");
  }
}

mapcache_locker* mapcache_locker_disk_create(mapcache_context *ctx) {
  mapcache_locker_disk *ld = (mapcache_locker_disk*)apr_pcalloc(ctx->pool, sizeof(mapcache_locker_disk));
  mapcache_locker *l = (mapcache_locker*)ld;
  l->type = MAPCACHE_LOCKER_DISK;
  l->aquire_lock = mapcache_locker_disk_aquire_lock;
  l->parse_xml = mapcache_locker_disk_parse_xml;
  l->release_lock = mapcache_locker_disk_release_lock;
  l->ping_lock = mapcache_locker_disk_ping_lock;
  return l;
}

struct mapcache_locker_fallback_lock {
  mapcache_locker *locker; /*the locker that actually acquired the lock*/
  void *lock; /*the opaque lock returned by the locker*/
};

void mapcache_locker_fallback_release_lock(mapcache_context *ctx, mapcache_locker *self, void *lock) {
  struct mapcache_locker_fallback_lock *flock = lock;
  flock->locker->release_lock(ctx,flock->locker,flock->lock);
}

mapcache_lock_result mapcache_locker_fallback_ping_lock(mapcache_context *ctx, mapcache_locker *self, void *lock) {
  struct mapcache_locker_fallback_lock *flock = lock;
  return flock->locker->ping_lock(ctx,flock->locker,flock->lock);
}

mapcache_lock_result mapcache_locker_fallback_aquire_lock(mapcache_context *ctx, mapcache_locker *self, char *resource, void **lock) {
  int i;
  mapcache_locker_fallback *locker = (mapcache_locker_fallback*)self;
  struct mapcache_locker_fallback_lock *fallback_lock = apr_pcalloc(ctx->pool, sizeof(struct mapcache_locker_fallback_lock));
  *lock = fallback_lock;
  for(i=0;i<locker->lockers->nelts;i++) {
    mapcache_lock_result lock_result;
    mapcache_locker *child_locker = APR_ARRAY_IDX(locker->lockers, i, mapcache_locker*);
    void *error;
    ctx->pop_errors(ctx,&error);
    lock_result = child_locker->aquire_lock(ctx, child_locker, resource, &(fallback_lock->lock));
    if(!GC_HAS_ERROR(ctx)) {
      fallback_lock->locker = child_locker;
      ctx->push_errors(ctx,error);
      return lock_result;
    } else {
      /*clear the current error if we still have a fallback lock to try */
      if(i<locker->lockers->nelts-1) {
        ctx->clear_errors(ctx);
      }
    }
    ctx->push_errors(ctx,error);
  }
  return MAPCACHE_LOCK_NOENT;
}


void mapcache_locker_fallback_parse_xml(mapcache_context *ctx, mapcache_locker *self, ezxml_t doc) {
  mapcache_locker_fallback *lm = (mapcache_locker_fallback*)self;
  ezxml_t node;
  lm->lockers = apr_array_make(ctx->pool,2,sizeof(mapcache_locker*));
  for(node = ezxml_child(doc,"locker"); node; node = node->next) {
    mapcache_locker *child_locker;
    mapcache_config_parse_locker(ctx,node,&child_locker);
    GC_CHECK_ERROR(ctx);
    APR_ARRAY_PUSH(lm->lockers,mapcache_locker*) = child_locker;
  }
}

#ifdef USE_MEMCACHE

#include <apr_memcache.h>

typedef struct {
  char *host;
  int port;
} mapcache_locker_memcache_server;

typedef struct {
  mapcache_locker locker;
  int nservers;
  mapcache_locker_memcache_server *servers;
  char *key_prefix;
} mapcache_locker_memcache;

void mapcache_locker_memcache_parse_xml(mapcache_context *ctx, mapcache_locker *self, ezxml_t doc) {
  mapcache_locker_memcache *lm = (mapcache_locker_memcache*)self;
  ezxml_t node,server_node;
  char *endptr;
  for(server_node = ezxml_child(doc,"server"); server_node; server_node = server_node->next) {
    lm->nservers++;
  }
  lm->servers = apr_pcalloc(ctx->pool, lm->nservers * sizeof(mapcache_locker_memcache_server));
  lm->nservers = 0;
  for(server_node = ezxml_child(doc,"server"); server_node; server_node = server_node->next) {
    if((node = ezxml_child(server_node,"host")) != NULL) {
      lm->servers[lm->nservers].host = apr_pstrdup(ctx->pool, node->txt);
    } else {
      ctx->set_error(ctx, 400, "memcache locker: no <host> provided");
      return;
    }

    if((node = ezxml_child(server_node,"port")) != NULL) {
      lm->servers[lm->nservers].port = (unsigned int)strtol(node->txt,&endptr,10);
      if(*endptr != 0 || lm->servers[lm->nservers].port <= 0) {
        ctx->set_error(ctx, 400, "failed to parse memcache locker port \"%s\". Expecting a positive integer",
            node->txt);
        return;
      }
    } else {
      /* default memcached port */
      lm->servers[lm->nservers].port = 11211;
    }
    lm->nservers++;
  }
  if((node = ezxml_child(doc,"key_prefix")) != NULL) {
    lm->key_prefix = apr_pstrdup(ctx->pool, node->txt);
  }
}

static char* memcache_key_for_resource(mapcache_context *ctx, mapcache_locker_memcache *lm, const char *resource)
{
  char *saferes = apr_pstrdup(ctx->pool,resource);
  char *safeptr = saferes;
  while(*safeptr) {
    if(*safeptr==' ' || *safeptr == '/' || *safeptr == '~' || *safeptr == '.' ||
        *safeptr == '\r' || *safeptr == '\n' || *safeptr == '\t' || *safeptr == '\f' || *safeptr == '\e' || *safeptr == '\a' || *safeptr == '\b') {
      *safeptr = '#';
    }
    safeptr++;
  }
  return apr_psprintf(ctx->pool,"%s"MAPCACHE_LOCKFILE_PREFIX"%s.lck",lm->key_prefix?lm->key_prefix:"",saferes);
}

apr_memcache_t* create_memcache(mapcache_context *ctx, mapcache_locker_memcache *lm) {
  apr_status_t rv;
  apr_memcache_t *memcache;
  char errmsg[120];
  int i;
  if(APR_SUCCESS != apr_memcache_create(ctx->pool, lm->nservers, 0, &memcache)) {
    ctx->set_error(ctx,500,"memcache locker: failed to create memcache backend");
    return NULL;
  }

  for(i=0;i<lm->nservers;i++) {
    apr_memcache_server_t *server;
    rv = apr_memcache_server_create(ctx->pool,lm->servers[i].host,lm->servers[i].port,1,1,1,10000,&server);
    if(APR_SUCCESS != rv) {
      ctx->set_error(ctx,500,"memcache locker: failed to create server %s:%d: %s",lm->servers[i].host,lm->servers[i].port, apr_strerror(rv,errmsg,120));
      return NULL;
    }

    rv = apr_memcache_add_server(memcache,server);
    if(APR_SUCCESS != rv) {
      ctx->set_error(ctx,500,"memcache locker: failed to add server %s:%d: %s",lm->servers[i].host,lm->servers[i].port, apr_strerror(rv,errmsg,120));
      return NULL;
    }
  }
  return memcache;
}

typedef struct {
  apr_memcache_t *memcache;
  char *lockname;
} mapcache_lock_memcache;

mapcache_lock_result mapcache_locker_memcache_ping_lock(mapcache_context *ctx, mapcache_locker *self, void *lock) {
  apr_status_t rv;
  char *one;
  size_t ione;
  mapcache_lock_memcache *mlock = (mapcache_lock_memcache*)lock;
  if(!mlock || !mlock->lockname || !mlock->memcache)
    return MAPCACHE_LOCK_NOENT;
  rv = apr_memcache_getp(mlock->memcache,ctx->pool,mlock->lockname,&one,&ione,NULL);
  if(rv == APR_SUCCESS)
    return MAPCACHE_LOCK_LOCKED;
  else
    return MAPCACHE_LOCK_NOENT;
}


mapcache_lock_result mapcache_locker_memcache_aquire_lock(mapcache_context *ctx, mapcache_locker *self, char *resource, void **lock) {
  apr_status_t rv;
  mapcache_locker_memcache *lm = (mapcache_locker_memcache*)self;
  char errmsg[120];
  mapcache_lock_memcache *mlock = apr_pcalloc(ctx->pool, sizeof(mapcache_lock_memcache));
  mlock->lockname = memcache_key_for_resource(ctx, lm, resource);
  mlock->memcache = create_memcache(ctx,lm);
  if(GC_HAS_ERROR(ctx)) {
    return MAPCACHE_LOCK_NOENT;
  }
  *lock = mlock;
  rv = apr_memcache_add(mlock->memcache,mlock->lockname,"1",1,self->timeout,0);
  if( rv == APR_SUCCESS) {
    return MAPCACHE_LOCK_AQUIRED;
  } else if ( rv == APR_EEXIST ) {
    return MAPCACHE_LOCK_LOCKED;
  } else {
    ctx->set_error(ctx,500,"failed to lock resource %s to memcache locker: %s",resource, apr_strerror(rv,errmsg,120));
    return MAPCACHE_LOCK_NOENT;
  }
}

void mapcache_locker_memcache_release_lock(mapcache_context *ctx, mapcache_locker *self, void *lock) {
  apr_status_t rv;
  mapcache_lock_memcache *mlock = (mapcache_lock_memcache*)lock;
  char errmsg[120];
  if(!mlock || !mlock->memcache || !mlock->lockname) {
    /*error*/
    return;
  }

  rv = apr_memcache_delete(mlock->memcache,mlock->lockname,0);
  if(rv != APR_SUCCESS && rv!= APR_NOTFOUND) {
    ctx->set_error(ctx,500,"memcache: failed to delete key %s: %s", mlock->lockname, apr_strerror(rv,errmsg,120));
  }

}

mapcache_locker* mapcache_locker_memcache_create(mapcache_context *ctx) {
  mapcache_locker_memcache *lm = (mapcache_locker_memcache*)apr_pcalloc(ctx->pool, sizeof(mapcache_locker_memcache));
  mapcache_locker *l = (mapcache_locker*)lm;
  l->type = MAPCACHE_LOCKER_MEMCACHE;
  l->aquire_lock = mapcache_locker_memcache_aquire_lock;
  l->ping_lock = mapcache_locker_memcache_ping_lock;
  l->parse_xml = mapcache_locker_memcache_parse_xml;
  l->release_lock = mapcache_locker_memcache_release_lock;
  lm->nservers = 0;
  lm->servers = NULL;
  lm->key_prefix = NULL;
  return l;
}

#endif

mapcache_locker* mapcache_locker_fallback_create(mapcache_context *ctx) {
  mapcache_locker_fallback *lm = (mapcache_locker_fallback*)apr_pcalloc(ctx->pool, sizeof(mapcache_locker_fallback));
  mapcache_locker *l = (mapcache_locker*)lm;
  l->type = MAPCACHE_LOCKER_FALLBACK;
  l->aquire_lock = mapcache_locker_fallback_aquire_lock;
  l->ping_lock = mapcache_locker_fallback_ping_lock;
  l->parse_xml = mapcache_locker_fallback_parse_xml;
  l->release_lock = mapcache_locker_fallback_release_lock;
  return l;
}

void mapcache_config_parse_locker_old(mapcache_context *ctx, ezxml_t doc, mapcache_cfg *config) {
  /* backwards compatibility */
  int micro_retry;
  ezxml_t node;
  mapcache_locker_disk *ldisk;
  config->locker = mapcache_locker_disk_create(ctx);
  ldisk = (mapcache_locker_disk*)config->locker;
  if((node = ezxml_child(doc,"lock_dir")) != NULL) {
    ldisk->dir = apr_pstrdup(ctx->pool, node->txt);
  } else {
    ldisk->dir = apr_pstrdup(ctx->pool,"/tmp");
  }

  if((node = ezxml_child(doc,"lock_retry")) != NULL) {
    char *endptr;
    micro_retry = strtol(node->txt,&endptr,10);
    if(*endptr != 0 || micro_retry <= 0) {
      ctx->set_error(ctx, 400, "failed to parse lock_retry microseconds \"%s\". Expecting a positive integer",
          node->txt);
      return;
    }
  } else {
    /* default retry interval is 1/100th of a second, i.e. 10000 microseconds */
    micro_retry = 10000;
  }
  config->locker->retry_interval = micro_retry / 1000000.0;
  config->locker->timeout=120;
}


void mapcache_config_parse_locker(mapcache_context *ctx, ezxml_t node, mapcache_locker **locker) {
  ezxml_t cur_node;
  const char *ltype = ezxml_attr(node, "type");
  if(!ltype) ltype = "disk";
  if(!strcmp(ltype,"disk")) {
    *locker = mapcache_locker_disk_create(ctx);
  } else if(!strcmp(ltype,"fallback")) {
    *locker = mapcache_locker_fallback_create(ctx);
  } else if(!strcmp(ltype,"memcache")) {
#ifdef USE_MEMCACHE
    *locker = mapcache_locker_memcache_create(ctx);
#else
    ctx->set_error(ctx,400,"<locker>: type \"memcache\" cannot be used as memcache support is not compiled in");
    return;
#endif
  } else {
    ctx->set_error(ctx,400,"<locker>: unknown type \"%s\" (allowed are disk and memcache)",ltype);
    return;
  }
  (*locker)->parse_xml(ctx, *locker, node);

  if((cur_node = ezxml_child(node,"retry")) != NULL) {
    char *endptr;
    (*locker)->retry_interval = strtod(cur_node->txt,&endptr);
    if(*endptr != 0 || (*locker)->retry_interval <= 0) {
      ctx->set_error(ctx, 400, "failed to locker parse retry seconds \"%s\". Expecting a positive floating point number",
              cur_node->txt);
      return;
    }
  } else {
    /* default retry interval is 1/10th of a second */
    (*locker)->retry_interval = 0.1;
  }
  if((cur_node = ezxml_child(node,"timeout")) != NULL) {
    char *endptr;
    (*locker)->timeout = strtod(cur_node->txt,&endptr);
    if(*endptr != 0 || (*locker)->timeout <= 0) {
      ctx->set_error(ctx, 400, "failed to parse locker timeout seconds \"%s\". Expecting a positive floating point number",
              cur_node->txt);
      return;
    }
  } else {
    /* default timeout is 2 minutes */
    (*locker)->timeout = 120;
  }
}
/* vim: ts=2 sts=2 et sw=2
*/
