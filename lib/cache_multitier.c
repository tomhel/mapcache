/******************************************************************************
 *
 * Project:  MapServer
 * Purpose:  MapCache tile caching: multitier cache backend.
 * Author:   Thomas Bonfort and the MapServer team.
 *
 ******************************************************************************
 * Copyright (c) 1996-2011 Regents of the University of Minnesota.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without multitierriction, including without limitation
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
#include <string.h>

typedef struct mapcache_cache_multitier mapcache_cache_multitier;

struct mapcache_cache_multitier {
  mapcache_cache cache;
  apr_array_header_t *caches;
  int write_cache_idx;
};


static int _mapcache_cache_multitier_tile_exists(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile)
{
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  int i;
  for(i=0; i<cache->caches->nelts; i++) {
    mapcache_cache *subcache = APR_ARRAY_IDX(cache->caches,i,mapcache_cache*);
    if(mapcache_cache_tile_exists(ctx, subcache, tile) == MAPCACHE_TRUE) {
      return MAPCACHE_TRUE;
    }
  }
  return MAPCACHE_FALSE;
}

static void _mapcache_cache_multitier_tile_delete(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile)
{
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  int i;
  for(i=0; i<cache->caches->nelts; i++) {
    mapcache_cache *subcache = APR_ARRAY_IDX(cache->caches,i,mapcache_cache*);
    mapcache_cache_tile_delete(ctx, subcache, tile);
    ctx->clear_errors(ctx); /* ignore errors */
  }
}

/**
 * \brief get content of given tile
 *
 * fills the mapcache_tile::data of the given tile with content stored on the multitier server
 * \private \memberof mapcache_cache_multitier
 * \sa mapcache_cache::tile_get()
 */
static int _mapcache_cache_multitier_tile_get(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile)
{
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  mapcache_cache *subcache;
  int i,ret;
  subcache = APR_ARRAY_IDX(cache->caches,0,mapcache_cache*);
  ret = mapcache_cache_tile_get(ctx, subcache, tile);
  
  if(ret == MAPCACHE_CACHE_MISS) {
    for(i=1; i<cache->caches->nelts; i++) {
      subcache = APR_ARRAY_IDX(cache->caches,i,mapcache_cache*);
      if(mapcache_cache_tile_get(ctx, subcache, tile) == MAPCACHE_SUCCESS) {
        ctx->log(ctx,MAPCACHE_DEBUG,"got tile (%s,z=%d,y=%d,x=%d) from secondary cache (%s)",tile->tileset->name, tile->z, tile->y, tile->x, subcache->name);
        for(--i;i>=0;i--) {
          subcache = APR_ARRAY_IDX(cache->caches,i,mapcache_cache*);
          mapcache_cache_tile_set(ctx, subcache, tile);
          ctx->clear_errors(ctx); /* silently ignore these errors */
          ctx->log(ctx,MAPCACHE_DEBUG,"transferring tile (%s,z=%d,y=%d,x=%d) to cache (%s)",tile->tileset->name, tile->z, tile->y, tile->x, subcache->name);
        }
        return MAPCACHE_SUCCESS;
      }
    }
    return MAPCACHE_CACHE_MISS;
  } else {
    //ctx->log(ctx,MAPCACHE_DEBUG,"got tile (%s,z=%d,y=%d,x=%d) from primary cache (%s)",tile->tileset->name, tile->z, tile->y, tile->x, subcache->name);
    return ret;
  }
}

static void _mapcache_cache_multitier_tile_set(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile)
{
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  mapcache_cache *subcache = APR_ARRAY_IDX(cache->caches,cache->write_cache_idx,mapcache_cache*);
  return mapcache_cache_tile_set(ctx, subcache, tile);
}

static void _mapcache_cache_multitier_tile_multi_set(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tiles, int ntiles)
{
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  mapcache_cache *subcache = APR_ARRAY_IDX(cache->caches,cache->write_cache_idx,mapcache_cache*);
  return mapcache_cache_tile_multi_set(ctx, subcache, tiles, ntiles);
}

/**
 * \private \memberof mapcache_cache_multitier
 */
static void _mapcache_cache_multitier_configuration_parse_xml(mapcache_context *ctx, ezxml_t node, mapcache_cache *pcache, mapcache_cfg *config)
{
  int i,rw_cache=-1;
  ezxml_t cur_node;
  mapcache_cache_multitier *cache = (mapcache_cache_multitier*)pcache;
  cache->caches = apr_array_make(ctx->pool,3,sizeof(mapcache_cache*));
  for(i=0,cur_node = ezxml_child(node,"cache"); cur_node; cur_node = cur_node->next,i++) {
    char *write;
    if((write = ezxml_attr(cur_node,"write")) != NULL) {
      /* check if write flag is set to true on this child cache */
      if(!strcmp(write,"true")) {
        if(cache->write_cache_idx >= 0) {
          /* can only set write flag to true on one child cache */
          ctx->set_error(ctx,400,"multitier cache \"%s\" has write attribute set to true on more than one child cache", pcache->name);
          return;
        }
        cache->write_cache_idx = i;
      }
    } else {
      /* write flag not set, this child cache is writable by default. we'll remember the last one */
      rw_cache = i;
    }

    mapcache_cache *refcache = mapcache_configuration_get_cache(config, cur_node->txt);
    if(!refcache) {
      ctx->set_error(ctx, 400, "multitier cache \"%s\" references cache \"%s\","
                     " but it is not configured (hint:referenced caches must be declared before this multitier cache in the xml file)", pcache->name, cur_node->txt);
      return;
    }
    APR_ARRAY_PUSH(cache->caches,mapcache_cache*) = refcache;
  }
  if(cache->caches->nelts == 0) {
    ctx->set_error(ctx,400,"multitier cache \"%s\" does not reference any child caches", pcache->name);
    return;
  }

  if(cache->write_cache_idx < 0) {
    /* write flag was not set to true on any child cache */
    if(rw_cache < 0) {
      ctx->set_error(ctx,400,"multitier cache \"%s\" has no child cache configured as writable", pcache->name);
      return;
    }
    /* write tiles to last writable child cache */
    cache->write_cache_idx = rw_cache;
  }
}

/**
 * \private \memberof mapcache_cache_multitier
 */
static void _mapcache_cache_multitier_configuration_post_config(mapcache_context *ctx, mapcache_cache *cache,
    mapcache_cfg *cfg)
{
}


/**
 * \brief creates and initializes a mapcache_cache_multitier
 */
mapcache_cache* mapcache_cache_multitier_create(mapcache_context *ctx)
{
  mapcache_cache_multitier *cache = apr_pcalloc(ctx->pool,sizeof(mapcache_cache_multitier));
  if(!cache) {
    ctx->set_error(ctx, 500, "failed to allocate multitier cache");
    return NULL;
  }
  cache->write_cache_idx = -1;
  cache->cache.metadata = apr_table_make(ctx->pool,3);
  cache->cache.type = MAPCACHE_CACHE_COMPOSITE;
  cache->cache._tile_delete = _mapcache_cache_multitier_tile_delete;
  cache->cache._tile_get = _mapcache_cache_multitier_tile_get;
  cache->cache._tile_exists = _mapcache_cache_multitier_tile_exists;
  cache->cache._tile_set = _mapcache_cache_multitier_tile_set;
  cache->cache._tile_multi_set = _mapcache_cache_multitier_tile_multi_set;
  cache->cache.configuration_post_config = _mapcache_cache_multitier_configuration_post_config;
  cache->cache.configuration_parse_xml = _mapcache_cache_multitier_configuration_parse_xml;
  return (mapcache_cache*)cache;
}

