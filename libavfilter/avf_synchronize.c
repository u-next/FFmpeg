/**
 * Copyright (c) 2025 U-NEXT Co. LTD
 *
 * This is a basic synchronization filter that uses PTS delay for
 * delays. Ideally you could use the power of the PTS to automatically
 * set this delay, but in reality, two encoders might not be even remotely
 * similarly configured. Thus, there is an exposed HTTP server that allows
 * for the configuration of this delay.
 */

#include "libavutil/attributes.h"
#include "libavutil/avassert.h"
#include "libavutil/imgutils.h"
#include "libavutil/log.h"
#include "libavutil/mem.h"
#include "libavutil/opt.h"
#include "libavutil/time.h"

#include "avfilter.h"
#include "filters.h"
#include "formats.h"
#include "video.h"
#include "stdatomic.h"
#include <pthread.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct SynchronizeServerconfig {
    int port;
} SynchronizeServerconfig;

typedef struct SynchronizeContext {
    const AVClass *class;
    int should_delay;
    int ix;
    float fps;
    
    /* Has this source been sync'd during this period? */
    int has_been_synced;

    /* at timestamp tN, what was the PTS? */
    int64_t prev_pts;
    int64_t timestamp;

    /* How much offset necessary to resync */
    int64_t pts_offset;

    SynchronizeServerconfig server_config;
} SynchronizeContext;

/* a dynamic array that we maintain for the inputs here.
   Becuase these are inputs into a filter, we have no particular
   method to delete them.
   Note that on shutdown this memory is just leaked.
*/
static pthread_mutex_t access_mx = PTHREAD_MUTEX_INITIALIZER;
static int ctxts_len = 0;
static SynchronizeContext **ctxts = NULL;

/* we only want a single instance of the server to exist. */
static _Atomic(uint8_t) initialized_server = 0;

#define OFFSET(x) offsetof(SynchronizeContext, x)
#define FLAGS AV_OPT_FLAG_FILTERING_PARAM|AV_OPT_FLAG_VIDEO_PARAM
static const AVOption synchronize_options[] = {
    { "delay", "delay", OFFSET(should_delay), AV_OPT_TYPE_BOOL, { .i64 = 0 }, 0, 1, FLAGS},
    { "server_port", "server_port", OFFSET(server_config.port), AV_OPT_TYPE_INT, { .i64 = 8080 }, 1, UINT16_MAX-1, FLAGS },
    { "fps", "fps", OFFSET(fps), AV_OPT_TYPE_FLOAT, { .i64 = 25 }, 0, 400, FLAGS },
    { NULL }
};

AVFILTER_DEFINE_CLASS(synchronize);

/* Simple embedded HTTP (GET-only) server that is responsible for allowing
   manual control of the delay offset for a given source. */
static void *server_run(void *arg) {
    const SynchronizeServerconfig *config = arg;

    int fd, accepted_fd;
    int result;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len;
    char client_buffer[1024];
    int received;

    char *token, *end_ptr, *save_ptr, *save_ptr_sub;
    size_t tok_len;

    int target_ix;
    long new_offset;

    const int enable = 1;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(config->port);

    fd = socket(AF_INET, SOCK_STREAM, 0);

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        av_log(NULL, AV_LOG_ERROR, "Failed to set sockopt for synchronize server\n");
        return NULL;
    }
    
    result = bind(fd, (const struct sockaddr*)&server_addr, sizeof(server_addr));
    if (result < 0) {
        av_log(NULL, AV_LOG_ERROR, "Failed to bind for synchronize server\n");
        return NULL;
    }

    for (;;) {
        result = listen(fd, 16);
        client_len = sizeof(client_addr); 
        accepted_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);

        received = read(accepted_fd, client_buffer, 1024);
        if (received < 0) {
            goto close;
        }

        if (strcmp((token = strtok_r(client_buffer, " ", &save_ptr)), "GET") != 0) {
            /* HTTP 405 */
            goto close;
        }

        token = strtok_r(NULL, " ", &save_ptr);
        tok_len = strlen(token);
        if (token == NULL || tok_len < 4 || (target_ix = token[1] - '0') > 9 || token[2] != '/') {
            /* HTTP 404 */
            goto close;
        }

        if (target_ix > ctxts_len || ctxts[target_ix] == NULL) {
            /* HTTP 404 */
            goto close;
        }

        /* I'm not an enormous fan of the strtol function but its ultimately
           fairly okay as far as i'm concerned. This won't be exposed to the
           internet. */
        new_offset = strtol(token+3, &end_ptr, 10);
        if (end_ptr == token) {
            /* HTTP 404 */
            goto close;
        }
        
        pthread_mutex_lock(&access_mx);

        ctxts[target_ix]->pts_offset += new_offset;
        av_log(NULL, AV_LOG_WARNING, "target_ix=%d new_offset=%lld\n", target_ix, ctxts[target_ix]->pts_offset);

        pthread_mutex_unlock(&access_mx);

        write(accepted_fd, "DONE\n\0", 6);

    close:
        close(accepted_fd);
        continue;
    }

    close(fd);

    return NULL;
}


static pthread_t server_thread;
static int init_server(SynchronizeServerconfig *config) {
    int status;

    status = pthread_create(&server_thread, NULL, server_run, config);

    return status;
}


static av_cold int init(AVFilterContext *ctx)
{
    SynchronizeContext *sync = ctx->priv;

    int ret = 0;
    if ((ret = pthread_mutex_lock(&access_mx)) < 0) {
        return AVERROR(ret);
    }

    sync->ix = ctxts_len;

    /* save a reference to this source */
    av_log(ctx, AV_LOG_WARNING, "ALLOC %d\n", ctxts_len);
    av_dynarray_add(&ctxts, &ctxts_len, sync);
    if (ctxts_len == 0) {
        goto unlock;
    }

    /* initialized in critical section such that we're guaranteed only
       one synchronize filter will attempt to start a server. Each filter
       /could/ be given its own server such that we don't need to do any
       such management but like... meh. This is easier for the client
       to deal with. */
    if (!initialized_server) {
        if ((ret = init_server(&sync->server_config)) < 0) {
            goto unlock;
        }
        initialized_server = 1;
    }

 unlock:
    if ((ret = pthread_mutex_unlock(&access_mx)) < 0) {
        return AVERROR(ret);
    }

    return ret;
}

static int query_formats(const AVFilterContext *ctx, AVFilterFormatsConfig **cfg_in, AVFilterFormatsConfig **cfg_out) {
    return 0;
}

static int config_props(AVFilterLink *inlink) {
    return 0;
}


static int filter_frame(AVFilterLink *inlink, AVFrame *in)
{
    const AVFilterContext *ctx = inlink->dst;
    SynchronizeContext *sync = ctx->priv;
    AVFilterLink *outlink = ctx->outputs[0];

    in->pts += sync->pts_offset * in->time_base.den / sync->fps;

    sync->prev_pts = in->pts;
    
    return ff_filter_frame(outlink, in);
}

/* TODO(Ben) */
static av_cold void uninit(AVFilterContext *ctx){
    const SynchronizeContext *sync = ctx->priv;

    /* because this reference is managed by the ffmpeg runtime, we
       can just discard it without fear. */
    ctxts[sync->ix] = NULL;
}

static const AVFilterPad synchronize_inputs[] = {
    {
        .name = "one",
        .type = AVMEDIA_TYPE_VIDEO,
        .config_props = config_props,
        .filter_frame = filter_frame,
    },
};

const FFFilter ff_avf_synchronize = {
    .p.name = "synchronize",
    .p.description = NULL_IF_CONFIG_SMALL("Synchronize a series of inputs via remote control"),
    .p.priv_class = &synchronize_class,
    .p.flags = AVFILTER_FLAG_SUPPORT_TIMELINE_GENERIC,
    .priv_size = sizeof(SynchronizeContext),
    .init = init,
    .uninit = uninit,
    FILTER_INPUTS(synchronize_inputs),
    FILTER_OUTPUTS(ff_video_default_filterpad),
    FILTER_QUERY_FUNC2(query_formats),
};
