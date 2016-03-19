/******************************************************************************
 * $Id$
 *
 * Project:  MapServer
 * Purpose:  MapCache tile caching support file: riak cache backend.
 * Author:   Michael Downey and the MapServer team.
 *
 ******************************************************************************
 * Copyright (c) 1996-2013 Regents of the University of Minnesota.
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
#ifdef USE_RIAK

#include <riack.h>

//ugly way to detect if we are using Riack > 1.4
#ifdef RSTR_HAS_CONTENT_P

#include <apr_strings.h>
#include <apr_reslist.h>
#include <apr_hash.h>

#include <string.h>
#include <errno.h>

typedef struct mapcache_cache_riak mapcache_cache_riak;

/**\class mapcache_cache_riak
 * \brief a mapcache_cache for riak servers
 * \implements mapcache_cache
 */
struct mapcache_cache_riak {
   mapcache_cache cache;
   char *host;
   int port;
   char *key_template;
   char *bucket_template;
   char *bucket_type_template;
};

struct riak_conn_params {
  mapcache_cache_riak *cache;
};

void mapcache_riak_connection_constructor(mapcache_context *ctx, void **conn_, void *params) {
    mapcache_cache_riak *cache = ((struct riak_conn_params*)params)->cache;
    riack_connection_options options;
    riack_client *client = riack_new_client(0);

    if (client == NULL) {
        ctx->set_error(ctx,500,"failed to riack_new_client(0)");
        return;
    }

    options.recv_timeout_ms = 2000;
    options.send_timeout_ms = 2000;
    if (riack_connect(client, cache->host, cache->port, &options) != RIACK_SUCCESS) {
        riack_free(client);
        ctx->set_error(ctx,500,"failed to riack_connect()");
        return;
    }

    if (riack_ping(client) != RIACK_SUCCESS) {
        riack_free(client);
        ctx->set_error(ctx,500,"failed to riack_ping()");
        return;
    }

    *conn_ = client;
}

void mapcache_riak_connection_destructor(void *conn_) {
    riack_client *client = (riack_client *)conn_;
    riack_free(client);
}

static mapcache_pooled_connection* _riak_get_connection(mapcache_context *ctx, mapcache_cache_riak *cache, mapcache_tile *tile)
{
  mapcache_pooled_connection *pc;
  struct riak_conn_params params;

  params.cache = cache;

  pc = mapcache_connection_pool_get_connection(ctx,cache->cache.name,mapcache_riak_connection_constructor,
          mapcache_riak_connection_destructor, &params);

  return pc;
}

static char* string_from_template(mapcache_context *ctx, mapcache_tile *tile, char *template_string) {
    if (strchr(template_string,'{')) {
        return mapcache_util_get_tile_key(ctx, tile, template_string, " \r\n\t\f\e\a\b", "#");
    } else {
        return template_string;
    }
}

static int _mapcache_cache_riak_has_tile(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile) {
    int error;
    int connect_error = RIACK_SUCCESS;
    int retries = 3;
    riack_string key,bucket,bucket_type,*pbucket_type = NULL;
    riack_get_object *obj;
    riack_client *client;
    mapcache_pooled_connection *pc;
    mapcache_cache_riak *cache = (mapcache_cache_riak*)pcache;

    key.value = mapcache_util_get_tile_key(ctx, tile, cache->key_template, " \r\n\t\f\e\a\b", "#");
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FALSE;
    }
    key.len = strlen(key.value);

    bucket.value = string_from_template(ctx, tile, cache->bucket_template);
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FALSE;
    }
    bucket.len = strlen(bucket.value);

    if (cache->bucket_type_template) {
        bucket_type.value = string_from_template(ctx, tile, cache->bucket_type_template);
        if (GC_HAS_ERROR(ctx)) {
            return MAPCACHE_FALSE;
        }
        bucket_type.len = strlen(bucket_type.value);
        pbucket_type = &bucket_type;
    }

    pc = _riak_get_connection(ctx, cache, tile);
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FALSE;
    }
    client = pc->connection;

    do
    {
        error = riack_get_ext(client, &bucket, &key, 0, pbucket_type, &obj, 0);
        if (error != RIACK_SUCCESS) {
            ctx->log(ctx, MAPCACHE_WARN, "Retry %d in riak_has_tile for tile %s from cache %s due to error %d", (4-retries), key.value, cache->cache.name, error);
            for (connect_error = riack_reconnect(client);
                 connect_error != RIACK_SUCCESS && retries > 0;
                 connect_error = riack_reconnect(client))
            {
              --retries;
            }

            --retries;
        }
    }
    while (error != RIACK_SUCCESS && retries >= 0);

    if (error != RIACK_SUCCESS) {
        riack_free_get_object_p(client, &obj);    // riack_get allocates the returned object so we need to deallocate it.
        mapcache_connection_pool_invalidate_connection(ctx,pc);
        ctx->set_error(ctx, 500, "riak: failed to get key %s: %d", key.value, error);
        return MAPCACHE_FALSE;
    }

    if (obj->object.content_count < 1 || obj->object.content[0].data_len == 0) {
      error = MAPCACHE_FALSE;
    } else {
      error = MAPCACHE_TRUE;
    }

    riack_free_get_object_p(client, &obj);    // riack_get allocates the returned object so we need to deallocate it.
    mapcache_connection_pool_release_connection(ctx,pc);

    return error;
}

static void _mapcache_cache_riak_delete(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile) {
    int error;
    int connect_error = RIACK_SUCCESS;
    int retries = 3;
    riack_string key,bucket,bucket_type,*pbucket_type = NULL;
    riack_client *client;
    riack_del_properties properties;
    mapcache_pooled_connection *pc;
    mapcache_cache_riak *cache = (mapcache_cache_riak*)pcache;

    memset(&properties, 0, sizeof(riack_del_properties));

    key.value = mapcache_util_get_tile_key(ctx, tile, cache->key_template, " \r\n\t\f\e\a\b", "#");
    GC_CHECK_ERROR(ctx);
    key.len = strlen(key.value);

    bucket.value = string_from_template(ctx, tile, cache->bucket_template);
    GC_CHECK_ERROR(ctx);
    bucket.len = strlen(bucket.value);

    if (cache->bucket_type_template) {
        bucket_type.value = string_from_template(ctx, tile, cache->bucket_type_template);
        GC_CHECK_ERROR(ctx);
        bucket_type.len = strlen(bucket_type.value);
        pbucket_type = &bucket_type;
    }

    pc = _riak_get_connection(ctx, cache, tile);
    GC_CHECK_ERROR(ctx);
    client = pc->connection;

    properties.rw_use = 1;
    properties.rw = (4294967295 - 3);	// Special value meaning "ALL"

    do
    {
        error = riack_delete_ext(client, &bucket, pbucket_type, &key, &properties, 0);
        if (error != RIACK_SUCCESS) {
            ctx->log(ctx, MAPCACHE_WARN, "Retry %d in riak_delete for tile %s from cache %s due to error %d", (4-retries), key.value, cache->cache.name, error);
            for (connect_error = riack_reconnect(client);
                 connect_error != RIACK_SUCCESS && retries > 0;
                 connect_error = riack_reconnect(client))
            {
              --retries;
            }

            --retries;
        }
    }
    while (error != RIACK_SUCCESS && retries >= 0);

    mapcache_connection_pool_release_connection(ctx,pc);

    if (error != RIACK_SUCCESS) {
        ctx->set_error(ctx, 500, "riak: failed to delete key %s: %d", key.value, error);
    }
}

/**
 * \brief get content of given tile
 *
 * fills the mapcache_tile::data of the given tile with content stored on the riak server
 * \private \memberof mapcache_cache_riak
 * \sa mapcache_cache::tile_get()
 */
static int _mapcache_cache_riak_get(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile) {
    int error;
    int connect_error = RIACK_SUCCESS;
    int retries = 3;
    riack_string key,bucket,bucket_type,*pbucket_type = NULL;
    riack_get_object *obj;
    riack_get_properties properties;
    riack_client *client;
    mapcache_pooled_connection *pc;
    mapcache_cache_riak *cache = (mapcache_cache_riak*)pcache;

    memset(&properties, 0, sizeof(riack_get_properties));

	//Use Buckets defaults instead of setting the read/write attributes
    /*
	properties.r_use = 1;
    properties.r = 1;
	*/

    key.value = mapcache_util_get_tile_key(ctx, tile, cache->key_template, " \r\n\t\f\e\a\b", "#");
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FAILURE;
    }
    key.len = strlen(key.value);

    bucket.value = string_from_template(ctx, tile, cache->bucket_template);
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FAILURE;
    }
    bucket.len = strlen(bucket.value);

    if (cache->bucket_type_template) {
        bucket_type.value = string_from_template(ctx, tile, cache->bucket_type_template);
        if (GC_HAS_ERROR(ctx)) {
            return MAPCACHE_FAILURE;
        }
        bucket_type.len = strlen(bucket_type.value);
        pbucket_type = &bucket_type;
    }

    pc = _riak_get_connection(ctx, cache, tile);
    if (GC_HAS_ERROR(ctx)) {
        return MAPCACHE_FAILURE;
    }
    client = pc->connection;

    // If we get an error it is advised that we call reconnect.  It also appears
    // that every now and then we get an error and need to retry once again to
    // get it to work.
    do
    {
        error = riack_get_ext(client, &bucket, &key, &properties, pbucket_type, &obj, 0);
        if (error != RIACK_SUCCESS) {
            ctx->log(ctx, MAPCACHE_WARN, "Retry %d in riak_get for tile %s from cache %s due to error %d", (4-retries), key.value, cache->cache.name, error);
            for (connect_error = riack_reconnect(client);
                 connect_error != RIACK_SUCCESS && retries > 0;
                 connect_error = riack_reconnect(client))
            {
              --retries;
            }

            --retries;
        }
    }
    while (error != RIACK_SUCCESS && retries >= 0);

    if (error != RIACK_SUCCESS)
    {
        if (connect_error != RIACK_SUCCESS)
            mapcache_connection_pool_invalidate_connection(ctx,pc);
        else
            mapcache_connection_pool_release_connection(ctx,pc);

        ctx->set_error(ctx, 500, "Failed to get tile %s from cache %s due to error %d", key.value, cache->cache.name, error);
        return MAPCACHE_FAILURE;
    }

    // Check if tile exists.  If it doesn't we need to return CACHE_MISS or things go wrong.
    // Mapcache doesn't appear to use the has_tile function and uses _get instead so we need
    // to do this sort of test here instead of erroring.
    if (obj->object.content_count < 1 || obj->object.content[0].data_len == 0) {
        riack_free_get_object_p(client, &obj);  // Need to free the object here as well.
        mapcache_connection_pool_release_connection(ctx,pc);
        return MAPCACHE_CACHE_MISS;
    }

    // Copy the data into the buffer
    tile->encoded_data = mapcache_buffer_create(0, ctx->pool);
    mapcache_buffer_append(tile->encoded_data, obj->object.content[0].data_len, obj->object.content[0].data);

    // Get modified time
    if (obj->object.content[0].last_modified_present && obj->object.content[0].last_modified_usecs_present) {
        tile->mtime = apr_time_make(obj->object.content[0].last_modified, obj->object.content[0].last_modified_usecs);
    } else if (obj->object.content[0].last_modified_present) {
        tile->mtime = apr_time_make(obj->object.content[0].last_modified, 0);
    }

    riack_free_get_object_p(client, &obj);    // riack_get allocates the returned object so we need to deallocate it.

    mapcache_connection_pool_release_connection(ctx,pc);

    return MAPCACHE_SUCCESS;
}

/**
 * \brief push tile data to riak
 *
 * writes the content of mapcache_tile::data to the configured riak instance(s)
 * \private \memberof mapcache_cache_riak
 * \sa mapcache_cache::tile_set()
 */
static void _mapcache_cache_riak_set(mapcache_context *ctx, mapcache_cache *pcache, mapcache_tile *tile) {
    char *key,*content_type;
    int error;
    int connect_error = RIACK_SUCCESS;
    int retries = 3;
    riack_object object;
    riack_content content;
    riack_put_properties properties;
    riack_client *client;
    riack_string bucket,bucket_type,*pbucket_type = NULL;
    mapcache_pooled_connection *pc;
    mapcache_cache_riak *cache = (mapcache_cache_riak*)pcache;

    memset(&content, 0, sizeof(riack_content));
    memset(&object, 0, sizeof(riack_object));
    memset(&properties, 0, sizeof(riack_put_properties));

	//Use Buckets defaults instead of setting the read/write attributes
	/*
    properties.w_use = 1;
    properties.w = 1;

    properties.dw_use = 1;
    properties.dw = 0;*/

    key = mapcache_util_get_tile_key(ctx, tile, cache->key_template, " \r\n\t\f\e\a\b", "#");
    GC_CHECK_ERROR(ctx);

    bucket.value = string_from_template(ctx, tile, cache->bucket_template);
    GC_CHECK_ERROR(ctx);
    bucket.len = strlen(bucket.value);

    if (cache->bucket_type_template) {
        bucket_type.value = string_from_template(ctx, tile, cache->bucket_type_template);
        GC_CHECK_ERROR(ctx);
        bucket_type.len = strlen(bucket_type.value);
        pbucket_type = &bucket_type;
    }

    if (!tile->encoded_data) {
        tile->encoded_data = tile->tileset->format->write(ctx, tile->raw_image, tile->tileset->format);
        GC_CHECK_ERROR(ctx);
    }
    content_type = tile->tileset->format?(tile->tileset->format->mime_type?tile->tileset->format->mime_type:NULL):NULL;

    if(!content_type) {
      /* compute the content-type */
      mapcache_image_format_type t = mapcache_imageio_header_sniff(ctx,tile->encoded_data);
      if(t == GC_PNG)
        content_type = "image/png";
      else if(t == GC_JPEG)
        content_type = "image/jpeg";
    }

    pc = _riak_get_connection(ctx, cache, tile);
    GC_CHECK_ERROR(ctx);
    client = pc->connection;

    // Set up the riak object to put.  Need to do this after we get the client connection
    object.bucket = bucket;
    object.key.value = key;
    object.key.len = strlen(key);
    object.vclock.len = 0;
    object.content_count = 1;
    object.content = &content;
    content.content_type.value = content_type;
    content.content_type.len = content_type?strlen(content_type):0;
    content.data = (uint8_t*)tile->encoded_data->buf;
    content.data_len = tile->encoded_data->size;

    // If we get an error it is advised that we call reconnect.  It also appears
    // that every now and then we get an error and need to retry once again to
    // get it to work.
    do
    {
        error = riack_put_ext(client, &object, pbucket_type, 0, &properties, 0);
        if (error != RIACK_SUCCESS) {
            ctx->log(ctx, MAPCACHE_WARN, "Retry %d in riak_set for tile %s from cache %s due to eror %d", (4 - retries), key, cache->cache.name, error);
            for (connect_error = riack_reconnect(client);
                 connect_error != RIACK_SUCCESS && retries > 0;
                 connect_error = riack_reconnect(client))
            {
                --retries;
            }

            --retries;
        }
    }
    while (error != RIACK_SUCCESS && retries >= 0);

    if (connect_error != RIACK_SUCCESS)
        mapcache_connection_pool_invalidate_connection(ctx,pc);
    else
        mapcache_connection_pool_release_connection(ctx,pc);

    if (error != RIACK_SUCCESS)
    {
        ctx->set_error(ctx, 500, "failed to store tile %s to cache %s due to error %d.", key, cache->cache.name, error);
    }
}

/**
 * \private \memberof mapcache_cache_riak
 */
static void _mapcache_cache_riak_configuration_parse_xml(mapcache_context *ctx, ezxml_t node, mapcache_cache *cache, mapcache_cfg *config) {
    ezxml_t cur_node,xhost,xport,xbucket,xkey,xbucket_type;
    mapcache_cache_riak *dcache = (mapcache_cache_riak*)cache;
    int servercount = 0;

    for (cur_node = ezxml_child(node,"server"); cur_node; cur_node = cur_node->next) {
        servercount++;
    }

    if (!servercount) {
        ctx->set_error(ctx, 400, "riak cache %s has no <server>s configured", cache->name);
        return;
    }

    if (servercount > 1) {
        ctx->set_error(ctx, 400, "riak cache %s has more than 1 server configured", cache->name);
        return;
    }

    cur_node = ezxml_child(node, "server");
    xhost = ezxml_child(cur_node, "host");   /* Host should contain just server */
    xport = ezxml_child(cur_node, "port");
    xbucket = ezxml_child(cur_node, "bucket");
    xkey = ezxml_child(cur_node, "key");
    xbucket_type = ezxml_child(cur_node, "bucket_type");

    if (!xhost || !xhost->txt || ! *xhost->txt) {
        ctx->set_error(ctx, 400, "cache %s: <server> with no <host>", cache->name);
        return;
    } else {
        dcache->host = apr_pstrdup(ctx->pool, xhost->txt);
    }

    if (!xport || !xport->txt || ! *xport->txt) {
        ctx->set_error(ctx, 400, "cache %s: <server> with no <port>", cache->name);
        return;
    } else {
        dcache->port = atoi(xport->txt);
    }

    if (!xbucket || !xbucket->txt || ! *xbucket->txt) {
        ctx->set_error(ctx, 400, "cache %s: <server> with no <bucket>", cache->name);
        return;
    } else {
        dcache->bucket_template = apr_pstrdup(ctx->pool, xbucket->txt);
    }

    if(xkey && xkey->txt && *xkey->txt) {
        dcache->key_template = apr_pstrdup(ctx->pool, xkey->txt);
    }

    if (xbucket_type && xbucket_type->txt && *xbucket_type->txt) {
        dcache->bucket_type_template = apr_pstrdup(ctx->pool, xbucket_type->txt);
    }
}

/**
 * \private \memberof mapcache_cache_riak
 */
static void _mapcache_cache_riak_configuration_post_config(mapcache_context *ctx, mapcache_cache *cache, mapcache_cfg *cfg) {
    riack_init();
}

/**
 * \brief creates and initializes a mapcache_riak_cache
 */
mapcache_cache* mapcache_cache_riak_create(mapcache_context *ctx) {
    mapcache_cache_riak *cache = apr_pcalloc(ctx->pool,sizeof(mapcache_cache_riak));
    if (!cache) {
        ctx->set_error(ctx, 500, "failed to allocate riak cache");
        return NULL;
    }

    cache->cache.metadata = apr_table_make(ctx->pool, 3);
    cache->cache.type = MAPCACHE_CACHE_RIAK;
    cache->cache._tile_get = _mapcache_cache_riak_get;
    cache->cache._tile_exists = _mapcache_cache_riak_has_tile;
    cache->cache._tile_set = _mapcache_cache_riak_set;
    cache->cache._tile_delete = _mapcache_cache_riak_delete;
    cache->cache.configuration_parse_xml = _mapcache_cache_riak_configuration_parse_xml;
    cache->cache.configuration_post_config = _mapcache_cache_riak_configuration_post_config;
    cache->host = NULL;
    cache->port = 8087;	// Default RIAK port used for protobuf
    cache->bucket_template = NULL;
    cache->key_template = NULL;
    cache->bucket_type_template = NULL;

    return (mapcache_cache*)cache;
}
#endif
// Commented out because this function is already defined in cache_riak.c
/*#else
mapcache_cache* mapcache_cache_riak_create(mapcache_context *ctx) {
  ctx->set_error(ctx,400,"RIAK support not compiled in this version");
  return NULL;
}*/
#endif
