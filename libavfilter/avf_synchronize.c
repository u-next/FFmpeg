/*
 * Copyright (c) 2012-2014 Clément Bœsch <u pkh me>
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/**
 * Using PTS delta as a metric, force synchronize double-broadcast streams before
 * rebroadcasting them.
 */

#include <stdint.h>
#include <stdlib.h>
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
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

typedef struct SynchronizeContext {
    const AVClass *class;
    int should_delay;
    uint32_t ix;
    
    /* Has this source been sync'd during this period? */
    int has_been_synced;

    /* at timestamp tN, what was the PTS? */
    int64_t sampled_pts;
    int64_t timestamp;

    /* How much offset necessary to resync */
    int64_t pts_offset;
} SynchronizeContext;

#define OFFSET(x) offsetof(SynchronizeContext, x)
#define FLAGS AV_OPT_FLAG_FILTERING_PARAM|AV_OPT_FLAG_VIDEO_PARAM
static const AVOption synchronize_options[] = {
    { "delay", "delay", OFFSET(should_delay), AV_OPT_TYPE_BOOL, { .i64 = 0 }, 0, 1, FLAGS},
    { NULL }
};

AVFILTER_DEFINE_CLASS(synchronize);

SynchronizeContext *ctxts[] = {NULL, NULL};
/* hacked in server to allow for direct control of PTS. */
static void *server_run(void *arg) {
    int fd, accepted_fd;
    int result;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len;
    char client_buffer[1024];
    int received;

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(8080);

    fd = socket(AF_INET, SOCK_STREAM, 0);
    result = bind(fd, &server_addr, sizeof(server_addr));

    for (;;) {
        result = listen(fd, 16);
        client_len = sizeof(client_addr); 
        accepted_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);

        received = read(accepted_fd, client_buffer, 1024);
        if (received < 0) {
           close(accepted_fd);
           continue;
        }

        switch(client_buffer[0]) {
            case 'H':
                ctxts[0]->pts_offset += 50000;
                break;
            case 'J':
                ctxts[0]->pts_offset -= 50000;
                break;
            case 'K':
                ctxts[1]->pts_offset += 50000;
                break;
            case 'L':
                ctxts[1]->pts_offset -= 50000;
                break;
            case 'V':
                ctxts[0]->pts_offset += 500000;
                break;
            case 'B':
                ctxts[0]->pts_offset -= 500000;
                break;
            case 'N':
                ctxts[1]->pts_offset += 500000;
                break;
            case 'M':
                ctxts[1]->pts_offset -= 500000;
                break;
           default:
                break;
        }

        write(accepted_fd, "DONE\n\0", 6); 
        close(accepted_fd);
    }

    close(fd);

    return NULL;
}


static pthread_t server_thread;
static void init_server() {
    int status;

    status = pthread_create(&server_thread, NULL, server_run, NULL);
    if (status != 0) {}
}


static av_cold int init(AVFilterContext *ctx)
{
    SynchronizeContext *sync = ctx->priv;

    /* silly hack */
    sync->ix = ctx->name[19] - '0';

    init_server();

    return 0;
}

static int query_formats(const AVFilterContext *ctx, AVFilterFormatsConfig **cfg_in, AVFilterFormatsConfig **cfg_out) {
    return 0;
}

static int config_props(AVFilterLink *inlink)
{
    return 0;
}

const static int64_t ms = 1000;
const static int64_t sec = 1000 * ms;
const static int64_t min = 60 * sec;

/* how frequently should we store the PTS */
const static int64_t pts_storage_cadence = 1000000;

static int filter_frame(AVFilterLink *inlink, AVFrame *in)
{
    const AVFilterContext *ctx = inlink->dst;
    SynchronizeContext *sync = ctx->priv;
    AVFilterLink *outlink = ctx->outputs[0];

    ctxts[sync->ix] = sync;
    
    /* dumb hack since we're only using two sources */
    const uint32_t target = sync->ix ^ 1;
    SynchronizeContext *other = ctxts[target];

    const int64_t now = av_gettime();
   
    /* apply the per-source offset, if one exists. */
    in->pts += sync->pts_offset;
    
    return ff_filter_frame(outlink, in);
}

static av_cold void uninit(AVFilterContext *ctx){ }

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
    .p.description = NULL_IF_CONFIG_SMALL("Synchronize two inputs"),
    .p.priv_class = &synchronize_class,
    .p.flags = AVFILTER_FLAG_SUPPORT_TIMELINE_GENERIC,
    .priv_size = sizeof(SynchronizeContext),
    .init = init,
    .uninit = uninit,
    FILTER_INPUTS(synchronize_inputs),
    FILTER_OUTPUTS(ff_video_default_filterpad),
    FILTER_QUERY_FUNC2(query_formats),
};
