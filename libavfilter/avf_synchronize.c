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

typedef struct SynchronizeContext {
    const AVClass *class;
    int should_sleep;
    uint32_t ix;

    int64_t pts_offset;

    int has_started; /* does not account for pauses and restarts. */
} SynchronizeContext;

#define OFFSET(x) offsetof(SynchronizeContext, x)
#define FLAGS AV_OPT_FLAG_FILTERING_PARAM|AV_OPT_FLAG_VIDEO_PARAM
static const AVOption synchronize_options[] = {
    { "delay", "delay", OFFSET(should_sleep), AV_OPT_TYPE_BOOL, { .i64 = 0 }, 0, 1, FLAGS},
    { NULL }
};

AVFILTER_DEFINE_CLASS(synchronize);

static av_cold int init(AVFilterContext *ctx)
{
    SynchronizeContext *sync = ctx->priv;

    /* silly hack */
    sync->ix = ctx->name[19] - '0';

    return 0;
}

static int query_formats(const AVFilterContext *ctx, AVFilterFormatsConfig **cfg_in, AVFilterFormatsConfig **cfg_out) {
    return 0;
}

static int config_props(AVFilterLink *inlink)
{
    return 0;
}

static int64_t pts[] = { 0, 0 };
static int filter_frame(AVFilterLink *inlink, AVFrame *in)
{
    const AVFilterContext *ctx = inlink->dst;
    const SynchronizeContext *sync = ctx->priv;
    AVFilterLink *outlink = ctx->outputs[0];

    const uint32_t target = sync->ix ^ 1;

    pts[sync->ix] = in->pts; /* store ASAP for the other filter */

    const int64_t target_pts = pts[target];
    const int64_t delta_pts = in->pts - target_pts;
    /* the output PTS of frame 0 obviously does not impact the PTS of frame 1, so this can be
       almost stateless, with the exception of a single lookbehind to see the pts of the other
       video. */
    if (sync->should_sleep && delta_pts > 0) {
        in->pts += delta_pts; /* delay this frame to line up. */
    }

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
