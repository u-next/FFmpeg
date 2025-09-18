/**
 * @file
 * Synchronize multiple a/v sources by PTS and wallclock
 */

#include "libavutil/avstring.h"
#include "libavutil/channel_layout.h"
#include "libavutil/mem.h"
#include "libavutil/opt.h"
#include "avfilter.h"
#include "filters.h"
#include "formats.h"
#include "video.h"
#include "audio.h"

struct synchronize_context {
    const AVClass *class;
};

static const AVOption synchronize_options[] = {
    { NULL }
};

AVFILTER_DEFINE_CLASS(synchronize);

static av_cold int init(AVFilterContext *ctx) { return 0; }

static av_cold void uninit(AVFilterContext *ctx) {}

static int activate(AVFilterContext *ctx) { return 0; }

static int query_formats(const AVFilterContext *ctx,
                         AVFilterFormatsConfig **cfg_in,
                         AVFilterFormatsConfig **cfg_out) {
    return 0;
}

static int process_command(AVFilterContext *ctx, const char *cmd, const char *args,
                           char *res, int res_len, int flags) {
    return 0;
}

const FFFilter ff_avf_synchronize = {
    .p.name        = "concat",
    .p.description = NULL_IF_CONFIG_SMALL("Synchronize audio and video streams via PTS and wallclock."),
    .p.inputs      = NULL,
    .p.outputs     = NULL,
    .p.priv_class  = &synchronize_class,
    .p.flags       = AVFILTER_FLAG_DYNAMIC_INPUTS | AVFILTER_FLAG_DYNAMIC_OUTPUTS,
    .init          = init,
    .uninit        = uninit,
    .activate      = activate,
    .priv_size     = sizeof(struct synchronize_context),
    FILTER_QUERY_FUNC2(query_formats),
    .process_command = process_command,
};
