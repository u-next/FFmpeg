#include "libavutil/avassert.h"
#include "libavutil/imgutils.h"
#include "libavutil/mem.h"
#include "libavutil/opt.h"
#include "avfilter.h"
#include "filters.h"
#include "formats.h"
#include "video.h"
#include <libavutil/frame.h>

static const AVOption timestamp_options[] ={
    { NULL }
};

AVFILTER_DEFINE_CLASS(timestamp);

struct timestamp_context {
    const AVClass *class;
  };

static av_cold int init(AVFilterContext *ctx) {
    return 0;
}

static av_cold void uninit(AVFilterContext *ctx) {}

static int config_props(AVFilterLink *inlink) {
    return 0;
}

static int filter_frame(AVFilterLink *inlink, AVFrame *in) {
    AVFilterContext *ctx = inlink->dst;
    AVFilterLink *outlink = ctx->outputs[0];

    AVFrameSideData *sd = av_frame_new_side_data(in, AV_FRAME_DATA_SEI_UNREGISTERED, 16+sizeof(uint64_t)); /* you /need/ a UUID. */
    if (!sd) {
        return AVERROR(ENOMEM);
    }

    uint64_t data = 0xDEADBEEFULL;

    memset(sd->data, 0x42, 16); /* fake UUID */
    memcpy(sd->data+16, &data, sizeof(uint64_t));

    av_log(ctx, AV_LOG_ERROR, "sd: %d\n", in->nb_side_data);
    
    return ff_filter_frame(outlink, in);
}

static const AVFilterPad timestamp_inputs[] = {
    {
        .name = "default",
        .type = AVMEDIA_TYPE_VIDEO,
        .config_props = config_props,
        .filter_frame = filter_frame,
    },
};

static int query_formats(AVFilterContext *ctx) {
    return ff_set_common_formats(ctx, ff_formats_pixdesc_filter(0, AV_PIX_FMT_FLAG_HWACCEL));
}

const AVFilter ff_vf_timestamp = {
    .name          = "timestamp",
    .description   = NULL_IF_CONFIG_SMALL("Embed user unregistered SEI timing info"),
    .priv_size     = sizeof(struct timestamp_context),
    .init          = init,
    .uninit        = uninit,
    FILTER_INPUTS(timestamp_inputs),
    FILTER_OUTPUTS(ff_video_default_filterpad),
    FILTER_QUERY_FUNC(query_formats),
    .priv_class    = &timestamp_class,
    .flags         = AVFILTER_FLAG_SUPPORT_TIMELINE_GENERIC,
};
