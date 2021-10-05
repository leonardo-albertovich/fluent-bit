/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019-2021 The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_input_plugin.h>
#include <fluent-bit/flb_input_chunk.h>
#include <fluent-bit/flb_storage.h>
#include <fluent-bit/flb_utils.h>
#include <chunkio/chunkio.h>

#include <sys/types.h>
#include <sys/stat.h>

#ifndef FLB_SYSTEM_WINDOWS
#include <unistd.h>
#endif

#define RELEASE_REGARDLESS_OF_OUTCOME       0
#define RELEASE_ONLY_ON_ENOUGH_SATISFACTION 1

struct sb_out_chunk {
    struct cio_chunk  *chunk;
    struct cio_stream *stream;
    size_t             size;
    // uint64_t          routes[FLB_ROUTES_MASK_ELEMENTS];
    struct mk_list    _head;
};

struct sb_out_queue {
    struct flb_output_instance *ins;
    struct mk_list              chunks; /* head for every sb_out_chunk */
    struct mk_list              _head;
};

struct flb_sb {
    int coll_fd;                        /* collector id */
    size_t mem_limit;                   /* memory limit */
    struct flb_input_instance *ins;     /* input instance */
    struct cio_ctx *cio;                /* chunk i/o instance */
    struct mk_list backlogs;            /* list of all pending chunks segregated by output plugin */
};

static int  sb_allocate_backlogs(struct flb_sb *ctx);

static void sb_destroy_backlogs(struct flb_sb *ctx);

static struct sb_out_queue *sb_find_segregated_backlog_by_output_plugin_instance(
                                struct flb_output_instance *output_plugin_instance,
                                struct flb_sb              *context);

static struct sb_out_chunk *sb_allocate_chunk(struct cio_chunk *chunk,
                                              struct cio_stream *stream,
                                              size_t size);

static void sb_destroy_chunk(struct sb_out_chunk *chunk);

static int sb_release_used_space_by_output_plugin_instance(
                struct flb_input_chunk     *new_input_chunk_instance,
                struct flb_output_instance *output_plugin_instance,
                struct flb_sb              *context,
                size_t                      minimum_actionable_release);

static int sb_release_segregated_backlog_space_by_output_plugin_instance(
                struct flb_input_chunk     *new_input_chunk_instance,
                struct flb_output_instance *output_plugin_instance,
                struct flb_sb              *context,
                size_t                      minimum_actionable_release);

int sb_release_output_queue_space(struct flb_input_chunk     *new_input_chunk_instance,
                                  struct flb_output_instance *output_plugin_instance,
                                  size_t                      minimum_actionable_release);

size_t sb_get_releaseable_used_space_by_output_plugin_instance(struct flb_output_instance *output_plugin_instance,
                                                               struct flb_sb              *context,
                                                               size_t                      required_space);


size_t sb_get_releaseable_segregated_backlog_space_by_output_plugin_instance(struct flb_output_instance *output_plugin_instance,
                                                                             struct flb_sb              *context,
                                                                             size_t                      required_space);

size_t sb_get_releaseable_output_queue_space(struct flb_output_instance *output_plugin_instance,
                                             size_t                      required_space);


static void sb_remove_chunk_from_segregated_backlog(struct cio_chunk    *chunk,
                                                    struct sb_out_queue *backlog,
                                                    int                  destroy);

static void sb_remove_chunk_from_segregated_backlogs(struct cio_chunk *chunk,
                                                     struct flb_sb    *context);

static int sb_append_chunk_to_segregated_backlog(struct cio_chunk    *chunk,
                                                 struct cio_stream   *stream,
                                                 size_t               size,
                                                 struct sb_out_queue *backlog);

static int sb_append_chunk_to_segregated_backlogs(struct cio_chunk  *chunk,
                                                  struct cio_stream *stream,
                                                  struct flb_sb     *context);

int sb_segregate_chunks(struct flb_input_instance *in);



/* cb_collect callback */
static int cb_queue_chunks(struct flb_input_instance *in,
                           struct flb_config *config, void *data)
{
    size_t                  empty_output_queue_count;
    struct mk_list         *output_queue_iterator;
    struct sb_out_queue    *output_queue_instance;
    struct sb_out_chunk    *chunk_instance;
    struct mk_list         *tmp;
    struct mk_list         *head;
    struct sb_chunk        *sbc;
    struct flb_sb          *ctx;
    struct flb_input_chunk *ic;
    void                   *ch;
    size_t                  total = 0;
    ssize_t                 size;
    int                     ret;

    /* Get context */
    ctx = (struct flb_sb *) data;

    /* Get the total number of bytes already enqueued */
    total = flb_input_chunk_total_size(in);

    /* If we already hitted our limit, just wait and re-check later */
    if (total >= ctx->mem_limit) {
        return 0;
    }

    empty_output_queue_count = 0;

    while (total < ctx->mem_limit &&
           empty_output_queue_count < mk_list_size(&ctx->backlogs)) {
        empty_output_queue_count = 0;

        mk_list_foreach(output_queue_iterator, &ctx->backlogs) {
            output_queue_instance = mk_list_entry(output_queue_iterator,
                                                  struct sb_out_queue,
                                                  _head);

            if (mk_list_is_empty(&output_queue_instance->chunks) != 0) {
                chunk_instance = mk_list_entry_first(&output_queue_instance->chunks,
                                                     struct sb_out_chunk,
                                                     _head);

                /* Try to enqueue one chunk */
                /*
                 * All chunks on this backlog are 'file' based, always try to set
                 * them up. We validate the status.
                 */
                ret = cio_chunk_is_up(chunk_instance->chunk);

                if (ret == CIO_FALSE) {
                    ret = cio_chunk_up(chunk_instance->chunk);

                    if (ret == CIO_CORRUPTED) {
                        flb_plg_error(ctx->ins, "removing corrupted chunk from the "
                                      "queue %s:%s",
                                      chunk_instance->stream->name, chunk_instance->chunk->name);
                        cio_chunk_close(chunk_instance->chunk, FLB_FALSE);
                        sb_remove_chunk_from_segregated_backlogs(chunk_instance->chunk, ctx);
                        /* This function will indirecly release chunk_instance so it has to be
                         * called last.
                         */
                        continue;
                    }
                    else if (ret == CIO_ERROR || ret == CIO_RETRY) {
                        continue;
                    }
                }

                /* get the number of bytes being used by the chunk */
                size = cio_chunk_get_content_size(chunk_instance->chunk);
                if (size <= 0) {
                    flb_plg_error(ctx->ins, "removing empty chunk from the "
                                  "queue %s:%s",
                                  chunk_instance->stream->name, chunk_instance->chunk->name);
                    cio_chunk_close(chunk_instance->chunk, FLB_TRUE);
                    sb_remove_chunk_from_segregated_backlogs(chunk_instance->chunk, ctx);
                    /* This function will indirecly release chunk_instance so it has to be
                     * called last.
                     */
                    continue;
                }

                ch = chunk_instance->chunk;

                /* Associate this backlog chunk to this instance into the engine */
                ic = flb_input_chunk_map(in, ch);
                if (!ic) {
                    flb_plg_error(ctx->ins, "removing chunk %s:%s from the queue",
                                  chunk_instance->stream->name, chunk_instance->chunk->name);
                    cio_chunk_down(chunk_instance->chunk);

                    /*
                     * If the file cannot be mapped, just drop it. Failures are all
                     * associated with data corruption.
                     */
                    cio_chunk_close(chunk_instance->chunk, FLB_TRUE);
                    sb_remove_chunk_from_segregated_backlogs(chunk_instance->chunk, ctx);
                    /* This function will indirecly release chunk_instance so it has to be
                     * called last.
                     */
                    continue;
                }

                flb_plg_info(ctx->ins, "queueing %s:%s",
                             chunk_instance->stream->name, chunk_instance->chunk->name);

                /* We are removing this chunk reference from this specific backlog
                 * queue but we need to leave it in the remainder queues.
                 */
                sb_remove_chunk_from_segregated_backlogs(chunk_instance->chunk, ctx);

                /* check our limits */
                total += size;
            }
            else {
                empty_output_queue_count++;
            }
        }
    }

    return 0;
}

/* Append a chunk candidate to the list */
static int sb_append_chunk(struct cio_chunk *chunk, struct cio_stream *stream,
                           struct flb_sb *ctx)
{
/*
    struct sb_chunk *sbc;

    sbc = flb_malloc(sizeof(struct sb_chunk));
    if (!sbc) {
        flb_errno();
        return -1;
    }

    sbc->chunk = chunk;
    sbc->stream = stream;
    mk_list_add(&sbc->_head, &ctx->backlog);

    /* lock the chunk *
    cio_chunk_lock(chunk);
    flb_plg_info(ctx->ins, "register %s/%s", stream->name, chunk->name);
*/

    return 0;
}

static int sb_allocate_backlogs(struct flb_sb *ctx)
{
    struct flb_output_instance *out_instance;
    struct sb_out_queue        *out_queue;
    struct mk_list             *head;

    mk_list_foreach(head, &ctx->ins->config->outputs) {
        out_instance = mk_list_entry(head, struct flb_output_instance, _head);

        out_queue = (struct sb_out_queue *) \
                        flb_calloc(1, sizeof(struct sb_out_queue));

        if (out_queue == NULL) {
            sb_destroy_backlogs(ctx);

            return -1;
        }

        out_queue->ins = out_instance;
        mk_list_init(&out_queue->chunks);

        mk_list_add(&out_queue->_head, &ctx->backlogs);
    }

    return 0;
}

static void sb_destroy_backlogs(struct flb_sb *ctx)
{
    struct sb_out_queue *out_queue;
    struct mk_list      *head;

    mk_list_foreach(head, &ctx->backlogs) {
        out_queue = mk_list_entry(head, struct sb_out_queue, _head);

        mk_list_del(&out_queue->_head);

        flb_free(out_queue);
    }
}

static struct sb_out_queue *sb_find_segregated_backlog_by_output_plugin_instance(
                                struct flb_output_instance *output_plugin_instance,
                                struct flb_sb              *context)
{
    struct mk_list      *output_instance_backlog_iterator;
    struct sb_out_queue *output_instance_backlog;

    mk_list_foreach(output_instance_backlog_iterator, &context->backlogs) {
        output_instance_backlog = mk_list_entry(output_instance_backlog_iterator,
                                                struct sb_out_queue,
                                                _head);

        if (output_plugin_instance == output_instance_backlog->ins) {
            return output_instance_backlog;
        }
    }
}


size_t sb_get_releaseable_used_space_by_output_plugin_instance(struct flb_output_instance *output_plugin_instance,
                                                               struct flb_sb              *context,
                                                               size_t                      required_space)
{
    struct mk_list         *input_chunk_iterator;
    struct flb_input_chunk *input_chunk_instance;
    size_t                  releasable_space;
    int                     chunk_was_up;

    releasable_space = 0;

    mk_list_foreach(input_chunk_iterator, &context->ins->chunks) {
        input_chunk_instance = mk_list_entry(input_chunk_iterator, struct flb_input_chunk, _head);

        chunk_was_up = cio_chunk_is_up(input_chunk_instance->chunk);

        if (!chunk_was_up) {
            cio_chunk_up_force(input_chunk_instance->chunk);
        }

        if (!flb_routes_mask_get_bit(input_chunk_instance->routes_mask,
                                     output_plugin_instance->id)) {
            if (!chunk_was_up) {
                cio_chunk_down(input_chunk_instance->chunk);
            }

            continue;
        }

        if (flb_input_chunk_is_task_safe_delete(input_chunk_instance->task) == FLB_FALSE) {

            if (!chunk_was_up) {
                cio_chunk_down(input_chunk_instance->chunk);
            }

            continue;
        }

        releasable_space += cio_chunk_get_real_size(input_chunk_instance->chunk);

        if (!chunk_was_up) {
            cio_chunk_down(input_chunk_instance->chunk);
        }

        if (releasable_space >= required_space) {
            break;
        }
    }

    return releasable_space;
}

static int sb_release_used_space_by_output_plugin_instance(
                struct flb_input_chunk     *new_input_chunk_instance,
                struct flb_output_instance *output_plugin_instance,
                struct flb_sb              *context,
                size_t                      minimum_actionable_release)
{
    struct mk_list         *input_chunk_iterator_tmp;
    struct mk_list         *input_chunk_iterator;
    struct flb_input_chunk *input_chunk_instance;
    size_t                  releasable_space;
    size_t                  released_space;
    size_t                  required_space;
    int                     chunk_released;
    int                     chunk_was_up;
    size_t                  chunk_size;

    required_space = cio_chunk_get_real_size(new_input_chunk_instance->chunk);
    releasable_space = 0;

    releasable_space = sb_get_releaseable_used_space_by_output_plugin_instance(output_plugin_instance,
                                                                               context,
                                                                               required_space);

    if (releasable_space < required_space) {
        if (releasable_space < minimum_actionable_release &&
            minimum_actionable_release > 0) {
            return -1;
        }
        else {
            required_space = minimum_actionable_release;
        }
    }

    released_space = 0;

    mk_list_foreach_safe(input_chunk_iterator,
                         input_chunk_iterator_tmp,
                         &context->ins->chunks) {
        input_chunk_instance = mk_list_entry(input_chunk_iterator, struct flb_input_chunk, _head);

        chunk_was_up = cio_chunk_is_up(input_chunk_instance->chunk);

        if (!chunk_was_up) {
            cio_chunk_up_force(input_chunk_instance->chunk);
        }

        if (!flb_routes_mask_get_bit(input_chunk_instance->routes_mask,
                                     output_plugin_instance->id)) {
            if (!chunk_was_up) {
                cio_chunk_down(input_chunk_instance->chunk);
            }

            continue;
        }

        if (flb_input_chunk_safe_delete(new_input_chunk_instance,
                                        input_chunk_instance,
                                        output_plugin_instance->id) == FLB_FALSE ||
            flb_input_chunk_is_task_safe_delete(input_chunk_instance->task) == FLB_FALSE) {
            if (!chunk_was_up) {
                cio_chunk_down(input_chunk_instance->chunk);
            }
            continue;
        }

        chunk_size = cio_chunk_get_real_size(input_chunk_instance->chunk);
        chunk_released = FLB_FALSE;

        if (input_chunk_instance->task != NULL) {
            /*
             * If the chunk is referenced by a task and task has no active route,
             * we need to destroy the task as well.
             */
            if (input_chunk_instance->task->users == 0) {
                // flb_debug("[task] drop task_id %d with no active route from input plugin %s",
                //           input_chunk_instance->task->id, new_input_chunk_instance->in->name);
                flb_task_destroy(input_chunk_instance->task, FLB_TRUE);

                chunk_released = FLB_TRUE;
            }
        }
        else {
            // flb_debug("[input chunk] drop chunk %s with no output route from input plugin %s",
            //           flb_input_chunk_get_name(input_chunk_instance), new_input_chunk_instance->in->name);

            flb_input_chunk_destroy(input_chunk_instance, FLB_TRUE);

            chunk_released = FLB_TRUE;
        }

        if (chunk_released) {
            released_space += chunk_size;
        }

        if (released_space >= required_space) {
            break;
        }
    }

    if (released_space < required_space) {
        return -2;
    }

    return 0;
}

size_t sb_get_releaseable_segregated_backlog_space_by_output_plugin_instance(struct flb_output_instance *output_plugin_instance,
                                                                             struct flb_sb              *context,
                                                                             size_t                      required_space)
{
    struct sb_out_queue *output_instance_backlog;
    struct mk_list      *output_chunk_iterator;
    struct sb_out_chunk *output_chunk_instance;
    size_t               releasable_space;

    output_instance_backlog = sb_find_segregated_backlog_by_output_plugin_instance(
                                output_plugin_instance, context);

    if (output_instance_backlog == NULL) {
        return 0;
    }

    releasable_space = 0;

    mk_list_foreach(output_chunk_iterator,
                    &output_instance_backlog->chunks) {
        output_chunk_instance = mk_list_entry(output_chunk_iterator,
                                              struct sb_out_chunk,
                                              _head);

        releasable_space += output_chunk_instance->size;

        if (releasable_space >= required_space) {
            break;
        }
    }

    return releasable_space;
}

static int sb_release_segregated_backlog_space_by_output_plugin_instance(
                struct flb_input_chunk     *new_input_chunk_instance,
                struct flb_output_instance *output_plugin_instance,
                struct flb_sb              *context,
                size_t                      minimum_actionable_release)
{
    struct mk_list      *output_chunk_iterator_tmp;
    struct sb_out_queue *output_instance_backlog;
    struct mk_list      *output_chunk_iterator;
    struct sb_out_chunk *output_chunk_instance;
    size_t               releasable_space;
    size_t               released_space;
    size_t               required_space;
    size_t               chunk_size;

    output_instance_backlog = sb_find_segregated_backlog_by_output_plugin_instance(
                                output_plugin_instance, context);

    if (output_instance_backlog == NULL) {
        return -1;
    }

    chunk_size = cio_chunk_get_real_size(new_input_chunk_instance->chunk);

    required_space = chunk_size;
    releasable_space = 0;

    releasable_space = sb_get_releaseable_segregated_backlog_space_by_output_plugin_instance(output_plugin_instance,
                                                                                             context,
                                                                                             required_space);

    if (releasable_space < required_space) {
        if (releasable_space < minimum_actionable_release &&
            minimum_actionable_release > 0) {
            return -2;
        }
        else {
            required_space = minimum_actionable_release;
        }
    }

    released_space = 0;

    mk_list_foreach_safe(output_chunk_iterator,
                         output_chunk_iterator_tmp,
                         &output_instance_backlog->chunks) {
        output_chunk_instance = mk_list_entry(output_chunk_iterator,
                                              struct sb_out_chunk,
                                              _head);

        released_space += output_chunk_instance->size;

        cio_chunk_close(output_chunk_instance->chunk, FLB_TRUE);
        sb_remove_chunk_from_segregated_backlogs(output_chunk_instance->chunk,
                                                 context);

        if (released_space >= required_space) {
            break;
        }
    }

    if (released_space < required_space) {
        return -3;
    }

    return 0;
}

static struct sb_out_chunk *sb_allocate_chunk(struct cio_chunk *chunk,
                                              struct cio_stream *stream,
                                              size_t size)
{
    struct sb_out_chunk *result;

    result = (struct sb_out_chunk *) \
                  flb_calloc(1, sizeof(struct sb_out_chunk));

    if (result != NULL) {
        result->chunk  = chunk;
        result->stream = stream;
        result->size   = size;
    }

    return result;
}

static void sb_destroy_chunk(struct sb_out_chunk *chunk)
{
    flb_free(chunk);
}

static void sb_remove_chunk_from_segregated_backlog(struct cio_chunk    *chunk,
                                                    struct sb_out_queue *backlog,
                                                    int                  destroy)
{
    struct mk_list      *output_chunk_iterator_tmp;
    struct mk_list      *output_chunk_iterator;
    struct sb_out_chunk *output_chunk_instance;

    mk_list_foreach_safe(output_chunk_iterator,
                         output_chunk_iterator_tmp,
                         &backlog->chunks) {
        output_chunk_instance = mk_list_entry(output_chunk_iterator,
                                              struct sb_out_chunk,
                                              _head);

        if (output_chunk_instance->chunk == chunk) {
            mk_list_del(&output_chunk_instance->_head);

            backlog->ins->fs_backlog_chunks_size -= \
                cio_chunk_get_real_size(chunk);

            if (destroy) {
                sb_destroy_chunk(output_chunk_instance);
            }

            break;
        }
    }
}

static void sb_remove_chunk_from_segregated_backlogs(struct cio_chunk *chunk,
                                                     struct flb_sb    *context)
{
    struct mk_list      *output_instance_backlog_iterator;
    struct sb_out_queue *output_instance_backlog;

    mk_list_foreach(output_instance_backlog_iterator, &context->backlogs) {
        output_instance_backlog = mk_list_entry(output_instance_backlog_iterator,
                                                struct sb_out_queue,
                                                _head);

        sb_remove_chunk_from_segregated_backlog(chunk,
                                                output_instance_backlog,
                                                FLB_TRUE);
    }
}

static int sb_append_chunk_to_segregated_backlog(struct cio_chunk    *chunk,
                                                 struct cio_stream   *stream,
                                                 size_t               size,
                                                 struct sb_out_queue *backlog)
{
    struct sb_out_chunk *sbc;

    sbc = sb_allocate_chunk(chunk, stream, size);

    if (sbc == NULL) {
        flb_errno();
        return -1;
    }

    mk_list_add(&sbc->_head, &backlog->chunks);

    backlog->ins->fs_backlog_chunks_size += \
        cio_chunk_get_real_size(chunk);

    return 0;
}

static int sb_append_chunk_to_segregated_backlogs(struct cio_chunk  *chunk,
                                                  struct cio_stream *stream,
                                                  struct flb_sb     *context)
{
    struct mk_list        *output_instance_backlog_iterator;
    struct sb_out_queue   *output_instance_backlog;
    struct flb_input_chunk dummy_input_chunk;
    size_t                 chunk_size;
    int                    tag_len;
    const char *           tag_buf;
    int                    ret;

    memset(&dummy_input_chunk, 0, sizeof(struct flb_input_chunk));

    dummy_input_chunk.in    = context->ins;
    dummy_input_chunk.chunk = chunk;

    chunk_size = cio_chunk_get_real_size(chunk);

    if (chunk_size < 0) {
        flb_warn("[input chunk] could not retrieve chunk real size");
        return -1;
    }

    ret = flb_input_chunk_get_tag(&dummy_input_chunk, &tag_buf, &tag_len);

    if (ret == -1) {
        flb_error("[input chunk] error retrieving tag of input chunk");
        return -2;
    }

    flb_routes_mask_set_by_tag(dummy_input_chunk.routes_mask,
                               tag_buf,
                               tag_len,
                               context->ins);

    mk_list_foreach(output_instance_backlog_iterator, &context->backlogs) {
        output_instance_backlog = mk_list_entry(output_instance_backlog_iterator,
                                                struct sb_out_queue,
                                                _head);

        if (flb_routes_mask_get_bit(dummy_input_chunk.routes_mask,
                                    output_instance_backlog->ins->id)) {
            ret = sb_append_chunk_to_segregated_backlog(chunk,
                                                        stream,
                                                        chunk_size,
                                                        output_instance_backlog);
            if (ret) {
                return -3;
            }
        }
    }

    return 0;
}

size_t sb_get_releaseable_output_queue_space(struct flb_output_instance *output_plugin_instance,
                                             size_t                      required_space)
{
    struct flb_input_instance *input_plugin_instance;
    struct flb_sb             *context;
    size_t                     result;

    input_plugin_instance = flb_get_input_by_name("storage_backlog.1",
                                                  output_plugin_instance->config);

    if (input_plugin_instance == NULL) {
        return 0;
    }

    context = (struct flb_sb *) input_plugin_instance->context;

    result  = sb_get_releaseable_used_space_by_output_plugin_instance(output_plugin_instance,
                                                                      context,
                                                                      required_space);

    result += sb_get_releaseable_segregated_backlog_space_by_output_plugin_instance(
                                                        output_plugin_instance,
                                                        context,
                                                        required_space);

    return result;
}

int sb_release_output_queue_space(struct flb_input_chunk     *new_input_chunk_instance,
                                  struct flb_output_instance *output_plugin_instance,
                                  size_t                      minimum_actionable_release)
{
    struct flb_input_instance *input_plugin_instance;
    struct flb_sb             *context;
    int                        result;

    input_plugin_instance = flb_get_input_by_name("storage_backlog.1",
                                                  output_plugin_instance->config);

    if (input_plugin_instance == NULL) {
        return -1;
    }

    context = (struct flb_sb *) input_plugin_instance->context;

    result = sb_release_used_space_by_output_plugin_instance(new_input_chunk_instance,
                                                             output_plugin_instance,
                                                             context,
                                                             minimum_actionable_release);

    if (result) {
        result = sb_release_segregated_backlog_space_by_output_plugin_instance(
                                                            new_input_chunk_instance,
                                                            output_plugin_instance,
                                                            context,
                                                            minimum_actionable_release);
    }

    return result;
}

int sb_segregate_chunks(struct flb_input_instance *input_plugin_instance)
{
    int                  error_detected;
    struct sb_out_queue *out_queue;
    struct flb_sb       *ctx;
    int                  ret;

    struct mk_list      *c_head;
    struct cio_stream   *stream;
    struct cio_chunk    *chunk;
    struct cio_ctx      *cio;

    struct mk_list             *head;
    struct mk_list             *output_instance_backlog_iterator;
    struct sb_out_queue        *output_instance_backlog;

    struct mk_list             *output_instance_iterator;
    struct flb_output_instance *output_instance;


    if (input_plugin_instance == NULL) {
        return -1;
    }

    ctx = (struct flb_sb *) input_plugin_instance->context;
    ret = sb_allocate_backlogs(ctx);

    if (ret) {
        return -2;
    }

    cio = ctx->cio;

    mk_list_foreach(head, &cio->streams) {
        stream = mk_list_entry(head, struct cio_stream, _head);

        mk_list_foreach(c_head, &stream->chunks) {
            chunk = mk_list_entry(c_head, struct cio_chunk, _head);

            if (!cio_chunk_is_up(chunk)) {
                cio_chunk_up_force(chunk);
            }

            ret = sb_append_chunk_to_segregated_backlogs(chunk, stream, ctx);

            if (ret) {
                flb_error("[input chunk] error distributing chunk references");
                return -2;
            }

            /* lock the chunk */
            cio_chunk_lock(chunk);
            flb_plg_info(ctx->ins, "register %s/%s", stream->name, chunk->name);

            if (cio_chunk_is_up(chunk) == CIO_TRUE) {
                cio_chunk_down(chunk);
            }
        }
    }

    return 0;
}

static int sb_prepare_environment(struct flb_sb *ctx)
{
/*
    int ret;
    struct mk_list *head;
    struct mk_list *c_head;
    struct cio_stream *stream;
    struct cio_chunk *chunk;
    struct cio_ctx *cio;

    cio = ctx->cio;
    mk_list_foreach(head, &cio->streams) {
        stream = mk_list_entry(head, struct cio_stream, _head);
        mk_list_foreach(c_head, &stream->chunks) {
            chunk = mk_list_entry(c_head, struct cio_chunk, _head);

            if (!cio_chunk_is_up(chunk)) {
                cio_chunk_up_force(chunk);
            }

            flb_input_chunk_attribute_backlog_storage_usage(ctx->ins, chunk);

            ctx->ins->config->storage_global_space_used += cio_chunk_get_real_size(chunk);

            ret = sb_append_chunk(chunk, stream, ctx);
            if (ret == -1) {
                flb_error("[storage_backlog] could not enqueue %s/%s",
                          stream->name, chunk->name);
                continue;
            }

            if (cio_chunk_is_up(chunk) == CIO_TRUE) {
                cio_chunk_down(chunk);
            }
        }
    }
*/
    return 0;
}

/* Initialize plugin */
static int cb_sb_init(struct flb_input_instance *in,
                      struct flb_config *config, void *data)
{
    int ret;
    char mem[32];
    struct flb_sb *ctx;

    ctx = flb_malloc(sizeof(struct flb_sb));
    if (!ctx) {
        flb_errno();
        return -1;
    }

    ctx->cio = data;
    ctx->ins = in;
    ctx->mem_limit = flb_utils_size_to_bytes(config->storage_bl_mem_limit);

    mk_list_init(&ctx->backlogs);

    flb_utils_bytes_to_human_readable_size(ctx->mem_limit, mem, sizeof(mem) - 1);
    flb_plg_info(ctx->ins, "queue memory limit: %s", mem);

    /* export plugin context */
    flb_input_set_context(in, ctx);

    /* Set a collector to trigger the callback to queue data every second */
    ret = flb_input_set_collector_time(in, cb_queue_chunks, 1, 0, config);
    if (ret < 0) {
        flb_plg_error(ctx->ins, "could not create collector");
        flb_free(ctx);
        return -1;
    }
    ctx->coll_fd = ret;

    /* Based on discovered chunks, create a local reference list */
    /* We're not using this at the moment in favor of late segregation */
    /* sb_prepare_environment(ctx); */

    return 0;
}

static void cb_sb_pause(void *data, struct flb_config *config)
{
    struct flb_sb *ctx = data;
    flb_input_collector_pause(ctx->coll_fd, ctx->ins);
}

static void cb_sb_resume(void *data, struct flb_config *config)
{
    struct flb_sb *ctx = data;
    flb_input_collector_resume(ctx->coll_fd, ctx->ins);
}

static int cb_sb_exit(void *data, struct flb_config *config)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct flb_sb *ctx = data;
    struct sb_chunk *sbc;

    flb_input_collector_pause(ctx->coll_fd, ctx->ins);

/*
    mk_list_foreach_safe(head, tmp, &ctx->backlog) {
        sbc = mk_list_entry(head, struct sb_chunk, _head);
        mk_list_del(&sbc->_head);
        flb_free(sbc);
    }
*/
    printf("We've got to clear the backlog queues here!\n");

    flb_free(ctx);
    return 0;
}

/* Plugin reference */
struct flb_input_plugin in_storage_backlog_plugin = {
    .name         = "storage_backlog",
    .description  = "Storage Backlog",
    .cb_init      = cb_sb_init,
    .cb_pre_run   = NULL,
    .cb_collect   = NULL,
    .cb_ingest    = NULL,
    .cb_flush_buf = NULL,
    .cb_pause     = cb_sb_pause,
    .cb_resume    = cb_sb_resume,
    .cb_exit      = cb_sb_exit,

    /* This plugin can only be configured and invoked by the Engine */
    .flags        = FLB_INPUT_PRIVATE
};
