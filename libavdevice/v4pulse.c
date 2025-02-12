/*
 * Copyright (c) 2013 Lukasz Marek <lukasz.m.luki@gmail.com>
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

#include <math.h>
#include <pulse/pulseaudio.h>
#include <pulse/error.h>
#include "libavformat/avformat.h"
#include "libavformat/internal.h"
#include "libavformat/mux.h"
#include "libavformat/version.h"
#include "libavutil/avutil.h"
#include "libavutil/channel_layout.h"
#include "libavutil/error.h"
#include "libavutil/frame.h"
#include "libavutil/internal.h"
#include "libavutil/opt.h"
#include "libavutil/pixfmt.h"
#include "libavutil/time.h"
#include "libavutil/log.h"
#include "libavutil/attributes.h"
#include "pulse_audio_common.h"
#include "v4l2-common.h"
#include "libavutil/time.h"
#include "libavutil/imgutils.h"
#include "libavutil/pixdesc.h"
#include <pthread.h>
#include <sched.h>

#define RING_BUFFER_SIZE 5
#define MIN_FRAMES_TO_START 3

typedef struct {
    AVClass *class;
    int fd;
} V4L2Context;

typedef struct PulseData {
    AVClass *class;
    const char *server;
    const char *name;
    const char *stream_name;
    const char *device;
    int64_t timestamp;
    int buffer_size;               /**< Buffer size in bytes */
    int buffer_duration;           /**< Buffer size in ms, recalculated to buffer_size */
    int prebuf;
    int minreq;
    int last_result;
    pa_threaded_mainloop *mainloop;
    pa_context *ctx;
    pa_stream *stream;
    int nonblocking;
    int mute;
    pa_volume_t base_volume;
    pa_volume_t last_volume;
    V4L2Context v4l2; // ridiculous hack, who PAYS me to commit such heinous crimes

    // Buffer management
    AVPacket *ring_buffer[RING_BUFFER_SIZE];
    int buffer_head;
    int buffer_tail;
    int buffer_count;
    AVPacket *last_frame;  // Store last frame for underrun

    // Thread management
    pthread_t output_thread;
    pthread_mutex_t buffer_lock;
    pthread_cond_t buffer_cond;
    int thread_running;

    // Timing management
    int64_t frame_interval;
    int64_t last_output_time;
} PulseData;

static void buffer_init(PulseData *ctx) {
    memset(ctx->ring_buffer, 0, sizeof(ctx->ring_buffer));
    ctx->buffer_head = 0;
    ctx->buffer_tail = 0;
    ctx->buffer_count = 0;
    ctx->last_frame = NULL;
    pthread_mutex_init(&ctx->buffer_lock, NULL);
    pthread_cond_init(&ctx->buffer_cond, NULL);
    ctx->thread_running = 0;
    ctx->last_output_time = 0;
}

static void buffer_destroy(PulseData *ctx) {
    pthread_mutex_lock(&ctx->buffer_lock);
    for (int i = 0; i < RING_BUFFER_SIZE; i++) {
        if (ctx->ring_buffer[i]) {
            av_packet_free(&ctx->ring_buffer[i]);
        }
    }
    if (ctx->last_frame) {
        av_packet_free(&ctx->last_frame);
    }
    pthread_mutex_unlock(&ctx->buffer_lock);
    pthread_mutex_destroy(&ctx->buffer_lock);
    pthread_cond_destroy(&ctx->buffer_cond);
}

static int buffer_push(PulseData *ctx, AVPacket *pkt) {
    pthread_mutex_lock(&ctx->buffer_lock);

    if (ctx->buffer_count >= RING_BUFFER_SIZE) {
        // Buffer is full, discard oldest frame
        AVPacket *old_pkt = ctx->ring_buffer[ctx->buffer_tail];
        if (old_pkt) {
            av_packet_free(&old_pkt);
        }
        ctx->ring_buffer[ctx->buffer_tail] = NULL;
        ctx->buffer_tail = (ctx->buffer_tail + 1) % RING_BUFFER_SIZE;
        ctx->buffer_count--;

        av_log(ctx, AV_LOG_WARNING, "Buffer full, discarding oldest frame\n");
    }

    AVPacket *new_pkt = av_packet_alloc();
    av_packet_ref(new_pkt, pkt);

    ctx->ring_buffer[ctx->buffer_head] = new_pkt;
    ctx->buffer_head = (ctx->buffer_head + 1) % RING_BUFFER_SIZE;
    ctx->buffer_count++;

    pthread_cond_signal(&ctx->buffer_cond);
    pthread_mutex_unlock(&ctx->buffer_lock);
    return 0;
}

static AVPacket* buffer_pop(PulseData *ctx) {
    pthread_mutex_lock(&ctx->buffer_lock);

    if (!ctx->thread_running || ctx->buffer_count == 0) {
        pthread_mutex_unlock(&ctx->buffer_lock);
        return NULL;
    }

    AVPacket *pkt = ctx->ring_buffer[ctx->buffer_tail];
    ctx->ring_buffer[ctx->buffer_tail] = NULL;
    ctx->buffer_tail = (ctx->buffer_tail + 1) % RING_BUFFER_SIZE;
    ctx->buffer_count--;

    pthread_mutex_unlock(&ctx->buffer_lock);
    return pkt;
}

static void update_last_frame(PulseData *ctx, const AVPacket *pkt) {
    if (ctx->last_frame) {
        av_packet_free(&ctx->last_frame);
    }
    ctx->last_frame = av_packet_alloc();
    av_packet_ref(ctx->last_frame, pkt);
}

static void *output_thread_func(void *arg) {
    PulseData *ctx = arg;
    V4L2Context v4l2 = ctx->v4l2;
    int64_t next_frame_time = 0;

    // Wait for initial buffer fill
    pthread_mutex_lock(&ctx->buffer_lock);
    while (ctx->thread_running && ctx->buffer_count < MIN_FRAMES_TO_START) {
        pthread_cond_wait(&ctx->buffer_cond, &ctx->buffer_lock);
    }
    pthread_mutex_unlock(&ctx->buffer_lock);

    while (ctx->thread_running) {
        int64_t current_time = av_gettime();

        if (next_frame_time == 0) {
            next_frame_time = current_time;
        }

        if (current_time >= next_frame_time) {
            AVPacket *pkt = buffer_pop(ctx);
            if (pkt) {
                int64_t time_delta = ctx->last_output_time ? current_time - ctx->last_output_time : 0;
                if (write(v4l2.fd, pkt->data, pkt->size) == -1) {
                    av_log(ctx, AV_LOG_ERROR, "Failed to write frame: %s\n", av_err2str(AVERROR(errno)));
                }
                update_last_frame(ctx, pkt);
                av_packet_free(&pkt);
                av_log(ctx, AV_LOG_VERBOSE, "frame written to device, buffer level: %d/%d, time delta: %"PRId64" us\n", 
                       ctx->buffer_count, RING_BUFFER_SIZE, time_delta);
                next_frame_time += ctx->frame_interval;
                ctx->last_output_time = current_time;
            } else if (ctx->thread_running) {
                // Buffer underrun - try to output last frame again
                if (ctx->last_frame) {
                    if (write(v4l2.fd, ctx->last_frame->data, ctx->last_frame->size) == -1) {
                        av_log(ctx, AV_LOG_ERROR, "Failed to write last frame: %s\n", av_err2str(AVERROR(errno)));
                    }
                    av_log(ctx, AV_LOG_WARNING, "Buffer underrun, outputting last frame at %"PRId64" us\n", current_time);
                    next_frame_time += ctx->frame_interval;
                    ctx->last_output_time = current_time;
                } else {
                    // No last frame available, wait half interval
                    av_log(ctx, AV_LOG_WARNING, "Buffer underrun with no last frame, retrying in %"PRId64" us\n", ctx->frame_interval / 2);
                    next_frame_time = current_time + (ctx->frame_interval / 2);
                }
            }
        } else {
            // sleep and wake up 2ms earlier then make short snoozes
            int64_t sleep_time = next_frame_time - current_time;
            if (sleep_time > 2000) {
                av_usleep(sleep_time - 2000); // Wake up 2ms early to account for scheduling
            }
            else {
                av_usleep(200);
            }
        }
    }

    return NULL;
}

static inline int get_stream_index(AVFormatContext *h, enum AVMediaType type) {
    int res_ix = -1;
    for (int ix = 0; ix < h->nb_streams; ix += 1) {
        AVStream *st = h->streams[ix];
        if (st->codecpar->codec_type == type) {
            res_ix = ix;
        }
    }
    return res_ix;
}

static av_cold int write_header_v4l2(AVFormatContext *s1, PulseData *s2) {
    int res = 0, flags = O_RDWR;
    struct v4l2_format fmt = {
        .type = V4L2_BUF_TYPE_VIDEO_OUTPUT
    };
    AVCodecParameters *par;
    uint32_t v4l2_pixfmt;

    V4L2Context *s = &(s2->v4l2);

    int stream_ix = get_stream_index(s1, AVMEDIA_TYPE_VIDEO);
    if (stream_ix == -1) {
        av_log(s1, AV_LOG_WARNING, "Failed to find video stream\n");
        return 0; // treated as non-fatal
    }

    if (s1->flags & AVFMT_FLAG_NONBLOCK)
        flags |= O_NONBLOCK;

    s->fd = open("/dev/video0", flags);
    if (s->fd < 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "Unable to open V4L2 device '%s'\n", s1->url);
        return res;
    }

    par = s1->streams[stream_ix]->codecpar;

    if(par->codec_id == AV_CODEC_ID_RAWVIDEO) {
        v4l2_pixfmt = ff_fmt_ff2v4l(par->format, AV_CODEC_ID_RAWVIDEO);
    } else {
        v4l2_pixfmt = ff_fmt_ff2v4l(AV_PIX_FMT_NONE, par->codec_id);
    }

    if (!v4l2_pixfmt) { // XXX: try to force them one by one?
        av_log(s1, AV_LOG_ERROR, "Unknown V4L2 pixel format equivalent for %s\n",
               av_get_pix_fmt_name(par->format));
        return AVERROR(EINVAL);
    }

    if (ioctl(s->fd, VIDIOC_G_FMT, &fmt) < 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "ioctl(VIDIOC_G_FMT): %s\n", av_err2str(res));
        return res;
    }

    fmt.fmt.pix.width       = par->width;
    fmt.fmt.pix.height      = par->height;
    fmt.fmt.pix.pixelformat = v4l2_pixfmt;
    fmt.fmt.pix.sizeimage   = av_image_get_buffer_size(par->format, par->width, par->height, 1);

    if (ioctl(s->fd, VIDIOC_S_FMT, &fmt) < 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "ioctl(VIDIOC_S_FMT): %s\n", av_err2str(res));
        return res;
    }

       av_log(s1, AV_LOG_INFO, "Frame rate: %d/%d\n", s1->streams[0]->time_base.num, s1->streams[0]->time_base.den);

    // Calculate frame interval in microseconds
    s2->frame_interval = (int64_t)s1->streams[0]->time_base.num * 1000000LL / s1->streams[0]->time_base.den;
    av_log(s1, AV_LOG_INFO, "Frame interval: %"PRId64" us\n", s2->frame_interval);

    // Initialize buffer and start thread
    buffer_init(s2);
    s2->thread_running = 1;

    if (pthread_create(&s2->output_thread, NULL, output_thread_func, s2) != 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "Failed to create output thread\n");
        return res;
    }

    // Try to set higher priority using nice value
    pthread_setschedprio(s2->output_thread, -10);
    av_log(s1, AV_LOG_INFO, "Output thread created with elevated priority\n");

    return res;
}

static void pulse_audio_sink_device_cb(pa_context *ctx, const pa_sink_info *dev,
                                       int eol, void *userdata)
{
    PulseData *s = userdata;

    if (s->ctx != ctx)
        return;

    if (eol) {
        pa_threaded_mainloop_signal(s->mainloop, 0);
    } else {
        if (dev->flags & PA_SINK_FLAT_VOLUME)
            s->base_volume = dev->base_volume;
        else
            s->base_volume = PA_VOLUME_NORM;
        av_log(s, AV_LOG_DEBUG, "base volume: %u\n", s->base_volume);
    }
}

/* Mainloop must be locked before calling this function as it uses pa_threaded_mainloop_wait. */
static int pulse_update_sink_info(AVFormatContext *h)
{
    PulseData *s = h->priv_data;
    pa_operation *op;
    if (!(op = pa_context_get_sink_info_by_name(s->ctx, s->device,
                                                pulse_audio_sink_device_cb, s))) {
        av_log(s, AV_LOG_ERROR, "pa_context_get_sink_info_by_name failed.\n");
        return AVERROR_EXTERNAL;
    }
    while (pa_operation_get_state(op) == PA_OPERATION_RUNNING)
        pa_threaded_mainloop_wait(s->mainloop);
    pa_operation_unref(op);
    return 0;
}

static void pulse_audio_sink_input_cb(pa_context *ctx, const pa_sink_input_info *i,
                                      int eol, void *userdata)
{
    AVFormatContext *h = userdata;
    PulseData *s = h->priv_data;

    if (s->ctx != ctx)
        return;

    if (!eol) {
        double val;
        pa_volume_t vol = pa_cvolume_avg(&i->volume);
        if (s->mute < 0 || (s->mute && !i->mute) || (!s->mute && i->mute)) {
            s->mute = i->mute;
            avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_MUTE_STATE_CHANGED, &s->mute, sizeof(s->mute));
        }

        vol = pa_sw_volume_divide(vol, s->base_volume);
        if (s->last_volume != vol) {
            val = (double)vol / PA_VOLUME_NORM;
            avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_VOLUME_LEVEL_CHANGED, &val, sizeof(val));
            s->last_volume = vol;
        }
    }
}

/* This function creates new loop so may be called from PA callbacks.
   Mainloop must be locked before calling this function as it operates on streams. */
static int pulse_update_sink_input_info(AVFormatContext *h)
{
    PulseData *s = h->priv_data;
    pa_operation *op;
    enum pa_operation_state op_state;
    pa_mainloop *ml = NULL;
    pa_context *ctx = NULL;
    int ret = 0;

    if ((ret = ff_pulse_audio_connect_context(&ml, &ctx, s->server, "Update sink input information")) < 0)
        return ret;

    if (!(op = pa_context_get_sink_input_info(ctx, pa_stream_get_index(s->stream),
                                              pulse_audio_sink_input_cb, h))) {
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    while ((op_state = pa_operation_get_state(op)) == PA_OPERATION_RUNNING)
        pa_mainloop_iterate(ml, 1, NULL);
    pa_operation_unref(op);
    if (op_state != PA_OPERATION_DONE) {
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

  fail:
    ff_pulse_audio_disconnect_context(&ml, &ctx);
    if (ret)
        av_log(s, AV_LOG_ERROR, "pa_context_get_sink_input_info failed.\n");
    return ret;
}

static void pulse_event(pa_context *ctx, pa_subscription_event_type_t t,
                        uint32_t idx, void *userdata)
{
    AVFormatContext *h = userdata;
    PulseData *s = h->priv_data;

    if (s->ctx != ctx)
        return;

    if ((t & PA_SUBSCRIPTION_EVENT_FACILITY_MASK) == PA_SUBSCRIPTION_EVENT_SINK_INPUT) {
        if ((t & PA_SUBSCRIPTION_EVENT_TYPE_MASK) == PA_SUBSCRIPTION_EVENT_CHANGE)
            // Calling from mainloop callback. No need to lock mainloop.
            pulse_update_sink_input_info(h);
    }
}

static void pulse_stream_writable(pa_stream *stream, size_t nbytes, void *userdata)
{
    AVFormatContext *h = userdata;
    PulseData *s = h->priv_data;
    int64_t val = nbytes;

    if (stream != s->stream)
        return;

    avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_BUFFER_WRITABLE, &val, sizeof(val));
    pa_threaded_mainloop_signal(s->mainloop, 0);
}

static void pulse_overflow(pa_stream *stream, void *userdata)
{
    AVFormatContext *h = userdata;
    avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_BUFFER_OVERFLOW, NULL, 0);
}

static void pulse_underflow(pa_stream *stream, void *userdata)
{
    AVFormatContext *h = userdata;
    avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_BUFFER_UNDERFLOW, NULL, 0);
}

static void pulse_stream_state(pa_stream *stream, void *userdata)
{
    PulseData *s = userdata;

    if (stream != s->stream)
        return;

    switch (pa_stream_get_state(s->stream)) {
        case PA_STREAM_READY:
        case PA_STREAM_FAILED:
        case PA_STREAM_TERMINATED:
            pa_threaded_mainloop_signal(s->mainloop, 0);
        default:
            break;
    }
}

static int pulse_stream_wait(PulseData *s)
{
    pa_stream_state_t state;

    while ((state = pa_stream_get_state(s->stream)) != PA_STREAM_READY) {
        if (state == PA_STREAM_FAILED || state == PA_STREAM_TERMINATED)
            return AVERROR_EXTERNAL;
        pa_threaded_mainloop_wait(s->mainloop);
    }
    return 0;
}

static void pulse_context_state(pa_context *ctx, void *userdata)
{
    PulseData *s = userdata;

    if (s->ctx != ctx)
        return;

    switch (pa_context_get_state(ctx)) {
        case PA_CONTEXT_READY:
        case PA_CONTEXT_FAILED:
        case PA_CONTEXT_TERMINATED:
            pa_threaded_mainloop_signal(s->mainloop, 0);
        default:
            break;
    }
}

static int pulse_context_wait(PulseData *s)
{
    pa_context_state_t state;

    while ((state = pa_context_get_state(s->ctx)) != PA_CONTEXT_READY) {
        if (state == PA_CONTEXT_FAILED || state == PA_CONTEXT_TERMINATED)
            return AVERROR_EXTERNAL;
        pa_threaded_mainloop_wait(s->mainloop);
    }
    return 0;
}

static void pulse_stream_result(pa_stream *stream, int success, void *userdata)
{
    PulseData *s = userdata;

    if (stream != s->stream)
        return;

    s->last_result = success ? 0 : AVERROR_EXTERNAL;
    pa_threaded_mainloop_signal(s->mainloop, 0);
}

static int pulse_finish_stream_operation(PulseData *s, pa_operation *op, const char *name)
{
    if (!op) {
        pa_threaded_mainloop_unlock(s->mainloop);
        av_log(s, AV_LOG_ERROR, "%s failed.\n", name);
        return AVERROR_EXTERNAL;
    }
    s->last_result = 2;
    while (s->last_result == 2)
        pa_threaded_mainloop_wait(s->mainloop);
    pa_operation_unref(op);
    pa_threaded_mainloop_unlock(s->mainloop);
    if (s->last_result != 0)
        av_log(s, AV_LOG_ERROR, "%s failed.\n", name);
    return s->last_result;
}

static int pulse_set_pause(PulseData *s, int pause)
{
    pa_operation *op;
    pa_threaded_mainloop_lock(s->mainloop);
    op = pa_stream_cork(s->stream, pause, pulse_stream_result, s);
    return pulse_finish_stream_operation(s, op, "pa_stream_cork");
}

static int pulse_flash_stream(PulseData *s)
{
    pa_operation *op;
    pa_threaded_mainloop_lock(s->mainloop);
    op = pa_stream_flush(s->stream, pulse_stream_result, s);
    return pulse_finish_stream_operation(s, op, "pa_stream_flush");
}

static void pulse_context_result(pa_context *ctx, int success, void *userdata)
{
    PulseData *s = userdata;

    if (s->ctx != ctx)
        return;

    s->last_result = success ? 0 : AVERROR_EXTERNAL;
    pa_threaded_mainloop_signal(s->mainloop, 0);
}

static int pulse_finish_context_operation(PulseData *s, pa_operation *op, const char *name)
{
    if (!op) {
        pa_threaded_mainloop_unlock(s->mainloop);
        av_log(s, AV_LOG_ERROR, "%s failed.\n", name);
        return AVERROR_EXTERNAL;
    }
    s->last_result = 2;
    while (s->last_result == 2)
        pa_threaded_mainloop_wait(s->mainloop);
    pa_operation_unref(op);
    pa_threaded_mainloop_unlock(s->mainloop);
    if (s->last_result != 0)
        av_log(s, AV_LOG_ERROR, "%s failed.\n", name);
    return s->last_result;
}

static int pulse_set_mute(PulseData *s)
{
    pa_operation *op;
    pa_threaded_mainloop_lock(s->mainloop);
    op = pa_context_set_sink_input_mute(s->ctx, pa_stream_get_index(s->stream),
                                        s->mute, pulse_context_result, s);
    return pulse_finish_context_operation(s, op, "pa_context_set_sink_input_mute");
}

static int pulse_set_volume(PulseData *s, double volume)
{
    pa_operation *op;
    pa_cvolume cvol;
    pa_volume_t vol;
    const pa_sample_spec *ss = pa_stream_get_sample_spec(s->stream);

    vol = pa_sw_volume_multiply(lrint(volume * PA_VOLUME_NORM), s->base_volume);
    pa_cvolume_set(&cvol, ss->channels, PA_VOLUME_NORM);
    pa_sw_cvolume_multiply_scalar(&cvol, &cvol, vol);
    pa_threaded_mainloop_lock(s->mainloop);
    op = pa_context_set_sink_input_volume(s->ctx, pa_stream_get_index(s->stream),
                                          &cvol, pulse_context_result, s);
    return pulse_finish_context_operation(s, op, "pa_context_set_sink_input_volume");
}

static int pulse_subscribe_events(PulseData *s)
{
    pa_operation *op;

    pa_threaded_mainloop_lock(s->mainloop);
    op = pa_context_subscribe(s->ctx, PA_SUBSCRIPTION_MASK_SINK_INPUT, pulse_context_result, s);
    return pulse_finish_context_operation(s, op, "pa_context_subscribe");
}

static void pulse_map_channels_to_pulse(const AVChannelLayout *channel_layout, pa_channel_map *channel_map)
{
    channel_map->channels = 0;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_FRONT_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_FRONT_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_FRONT_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_LOW_FREQUENCY) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_LFE;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_BACK_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_REAR_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_BACK_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_REAR_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_FRONT_LEFT_OF_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_LEFT_OF_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_FRONT_RIGHT_OF_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_RIGHT_OF_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_BACK_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_REAR_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_SIDE_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_SIDE_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_SIDE_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_SIDE_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_FRONT_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_FRONT_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_FRONT_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_FRONT_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_FRONT_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_FRONT_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_BACK_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_REAR_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_BACK_CENTER) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_REAR_CENTER;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_TOP_BACK_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_TOP_REAR_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_STEREO_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_LEFT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_STEREO_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_FRONT_RIGHT;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_WIDE_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_AUX0;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_WIDE_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_AUX1;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_SURROUND_DIRECT_LEFT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_AUX2;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_SURROUND_DIRECT_RIGHT) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_AUX3;
    if (av_channel_layout_index_from_channel(channel_layout, AV_CHAN_LOW_FREQUENCY_2) >= 0)
        channel_map->map[channel_map->channels++] = PA_CHANNEL_POSITION_LFE;
}

static av_cold int pulse_write_trailer_audio(AVFormatContext *h)
{
    PulseData *s = h->priv_data;

    if (s->mainloop) {
        pa_threaded_mainloop_lock(s->mainloop);
        if (s->stream) {
            pa_stream_disconnect(s->stream);
            pa_stream_set_state_callback(s->stream, NULL, NULL);
            pa_stream_set_write_callback(s->stream, NULL, NULL);
            pa_stream_set_overflow_callback(s->stream, NULL, NULL);
            pa_stream_set_underflow_callback(s->stream, NULL, NULL);
            pa_stream_unref(s->stream);
            s->stream = NULL;
        }
        if (s->ctx) {
            pa_context_disconnect(s->ctx);
            pa_context_set_state_callback(s->ctx, NULL, NULL);
            pa_context_set_subscribe_callback(s->ctx, NULL, NULL);
            pa_context_unref(s->ctx);
            s->ctx = NULL;
        }
        pa_threaded_mainloop_unlock(s->mainloop);
        pa_threaded_mainloop_stop(s->mainloop);
        pa_threaded_mainloop_free(s->mainloop);
        s->mainloop = NULL;
    }

    return 0;
}

static int write_trailer_video(AVFormatContext *s1)
{
    PulseData *handle = s1->priv_data;
    V4L2Context s = handle->v4l2;
    close(s.fd);

    // Stop thread
    handle->thread_running = 0;
    pthread_cond_signal(&handle->buffer_cond);
    pthread_join(handle->output_thread, NULL);

    // Cleanup
    buffer_destroy(handle);
    return 0;
}

static av_cold int pulse_write_trailer(AVFormatContext *h) {
    for (int ix = 0; ix < h->nb_streams; ix += 1) {
        AVStream *st = h->streams[ix];
        if (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            write_trailer_video(h);
        } else if (st->codecpar->codec_type == AVMEDIA_TYPE_AUDIO) {
            pulse_write_trailer_audio(h);
        }
    }

    // both functions can only return 0
    return 0;
}

static av_cold int pulse_write_header(AVFormatContext *h)
{
    PulseData *s = h->priv_data;
    int ret;
    pa_sample_spec sample_spec;
    pa_buffer_attr buffer_attributes = { -1, -1, -1, -1, -1 };
    pa_channel_map channel_map;
    pa_mainloop_api *mainloop_api;
    const char *stream_name = s->stream_name;
    static const pa_stream_flags_t stream_flags = PA_STREAM_INTERPOLATE_TIMING |
                                                  PA_STREAM_AUTO_TIMING_UPDATE |
                                                  PA_STREAM_NOT_MONOTONIC;

    
    int v4l2_res = write_header_v4l2(h, s);
    if (v4l2_res < 0) {
        av_log(s, AV_LOG_ERROR, "Failed to initialize v4l2 in pulse output");
        return AVERROR(ret);
    }

    int stream_ix = get_stream_index(h, AVMEDIA_TYPE_AUDIO);
    if (stream_ix == -1) {
        av_log(h, AV_LOG_ERROR, "Failed to find video stream\n");
        return AVERROR_STREAM_NOT_FOUND;
    }

    AVStream *st = h->streams[stream_ix];

    if (!stream_name) {
        if (h->url[0])
            stream_name = h->url;
        else
            stream_name = "Playback";
    }
    s->nonblocking = (h->flags & AVFMT_FLAG_NONBLOCK);

    if (s->buffer_duration) {
        int64_t bytes = av_rescale(s->buffer_duration,
                                   st->codecpar->ch_layout.nb_channels *
                                    (int64_t)st->codecpar->sample_rate *
                                    av_get_bytes_per_sample(st->codecpar->format),
                                   1000);
        buffer_attributes.tlength = FFMAX(s->buffer_size, av_clip64(bytes, 0, UINT32_MAX - 1));
        av_log(s, AV_LOG_DEBUG,
               "Buffer duration: %ums recalculated into %"PRId64" bytes buffer.\n",
               s->buffer_duration, bytes);
        av_log(s, AV_LOG_DEBUG, "Real buffer length is %u bytes\n", buffer_attributes.tlength);
    } else if (s->buffer_size)
        buffer_attributes.tlength = s->buffer_size;
    if (s->prebuf)
        buffer_attributes.prebuf = s->prebuf;
    if (s->minreq)
        buffer_attributes.minreq = s->minreq;

    sample_spec.format = ff_codec_id_to_pulse_format(st->codecpar->codec_id);
    sample_spec.rate = st->codecpar->sample_rate;
    sample_spec.channels = st->codecpar->ch_layout.nb_channels;
    /* if (!pa_sample_spec_valid(&sample_spec)) { */
    /*     av_log(s, AV_LOG_ERROR, "Invalid sample spec.\n"); */
    /*     return AVERROR(EINVAL); */
    /* } */

    if (sample_spec.channels == 1) {
        channel_map.channels = 1;
        channel_map.map[0] = PA_CHANNEL_POSITION_MONO;
    } else if (st->codecpar->ch_layout.order != AV_CHANNEL_ORDER_UNSPEC) {
        if (!av_channel_layout_check(&st->codecpar->ch_layout))
            return AVERROR(EINVAL);
        pulse_map_channels_to_pulse(&st->codecpar->ch_layout, &channel_map);
        /* Unknown channel is present in channel_layout, let PulseAudio use its default. */
        if (channel_map.channels != sample_spec.channels) {
            av_log(s, AV_LOG_WARNING, "Unknown channel. Using default channel map.\n");
            channel_map.channels = 0;
        }
    } else
        channel_map.channels = 0;

    if (!channel_map.channels)
        av_log(s, AV_LOG_WARNING, "Using PulseAudio's default channel map.\n");
    else if (!pa_channel_map_valid(&channel_map)) {
        av_log(s, AV_LOG_ERROR, "Invalid channel map.\n");
        return AVERROR(EINVAL);
    }

    /* start main loop */
    s->mainloop = pa_threaded_mainloop_new();
    if (!s->mainloop) {
        av_log(s, AV_LOG_ERROR, "Cannot create threaded mainloop.\n");
        return AVERROR(ENOMEM);
    }
    if ((ret = pa_threaded_mainloop_start(s->mainloop)) < 0) {
        av_log(s, AV_LOG_ERROR, "Cannot start threaded mainloop: %s.\n", pa_strerror(ret));
        pa_threaded_mainloop_free(s->mainloop);
        s->mainloop = NULL;
        return AVERROR_EXTERNAL;
    }

    pa_threaded_mainloop_lock(s->mainloop);

    mainloop_api = pa_threaded_mainloop_get_api(s->mainloop);
    if (!mainloop_api) {
        av_log(s, AV_LOG_ERROR, "Cannot get mainloop API.\n");
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    s->ctx = pa_context_new(mainloop_api, s->name);
    if (!s->ctx) {
        av_log(s, AV_LOG_ERROR, "Cannot create context.\n");
        ret = AVERROR(ENOMEM);
        goto fail;
    }
    pa_context_set_state_callback(s->ctx, pulse_context_state, s);
    pa_context_set_subscribe_callback(s->ctx, pulse_event, h);

    if ((ret = pa_context_connect(s->ctx, s->server, 0, NULL)) < 0) {
        av_log(s, AV_LOG_ERROR, "Cannot connect context: %s.\n", pa_strerror(ret));
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    if ((ret = pulse_context_wait(s)) < 0) {
        av_log(s, AV_LOG_ERROR, "Context failed.\n");
        goto fail;
    }

    s->stream = pa_stream_new(s->ctx, stream_name, &sample_spec,
                              channel_map.channels ? &channel_map : NULL);

    if ((ret = pulse_update_sink_info(h)) < 0) {
        av_log(s, AV_LOG_ERROR, "Updating sink info failed.\n");
        goto fail;
    }

    if (!s->stream) {
        av_log(s, AV_LOG_ERROR, "Cannot create stream.\n");
        ret = AVERROR(ENOMEM);
        goto fail;
    }
    pa_stream_set_state_callback(s->stream, pulse_stream_state, s);
    pa_stream_set_write_callback(s->stream, pulse_stream_writable, h);
    pa_stream_set_overflow_callback(s->stream, pulse_overflow, h);
    pa_stream_set_underflow_callback(s->stream, pulse_underflow, h);

    if ((ret = pa_stream_connect_playback(s->stream, s->device, &buffer_attributes,
                                          stream_flags, NULL, NULL)) < 0) {
        av_log(s, AV_LOG_ERROR, "pa_stream_connect_playback failed: %s.\n", pa_strerror(ret));
        ret = AVERROR_EXTERNAL;
        goto fail;
    }

    if ((ret = pulse_stream_wait(s)) < 0) {
        av_log(s, AV_LOG_ERROR, "Stream failed.\n");
        goto fail;
    }

    /* read back buffer attributes for future use */
    buffer_attributes = *pa_stream_get_buffer_attr(s->stream);
    s->buffer_size = buffer_attributes.tlength;
    s->prebuf = buffer_attributes.prebuf;
    s->minreq = buffer_attributes.minreq;
    av_log(s, AV_LOG_DEBUG, "Real buffer attributes: size: %d, prebuf: %d, minreq: %d\n",
           s->buffer_size, s->prebuf, s->minreq);

    pa_threaded_mainloop_unlock(s->mainloop);

    if ((ret = pulse_subscribe_events(s)) < 0) {
        av_log(s, AV_LOG_ERROR, "Event subscription failed.\n");
        /* a bit ugly but the simplest to lock here*/
        pa_threaded_mainloop_lock(s->mainloop);
        goto fail;
    }

    /* force control messages */
    s->mute = -1;
    s->last_volume = PA_VOLUME_INVALID;
    pa_threaded_mainloop_lock(s->mainloop);
    if ((ret = pulse_update_sink_input_info(h)) < 0) {
        av_log(s, AV_LOG_ERROR, "Updating sink input info failed.\n");
        goto fail;
    }
    pa_threaded_mainloop_unlock(s->mainloop);

    avpriv_set_pts_info(st, 64, 1, 1000000);  /* 64 bits pts in us */

    return 0;
  fail:
    pa_threaded_mainloop_unlock(s->mainloop);
    pulse_write_trailer(h);
    return ret;
  }



static int pulse_write_packet_audio(AVFormatContext *h, AVPacket *pkt)
{
    PulseData *s = h->priv_data;
    int ret;
    int64_t writable_size;

    if (!pkt)
        return pulse_flash_stream(s);

    if (pkt->dts != AV_NOPTS_VALUE)
        s->timestamp = pkt->dts;

    if (pkt->duration) {
        s->timestamp += pkt->duration;
    } else {
        AVStream *st = h->streams[pkt->stream_index];
        AVRational r = { 1, st->codecpar->sample_rate };
        int64_t samples = pkt->size / (av_get_bytes_per_sample(st->codecpar->format) * st->codecpar->ch_layout.nb_channels);
        s->timestamp += av_rescale_q(samples, r, st->time_base);
    }

    pa_threaded_mainloop_lock(s->mainloop);
    if (!PA_STREAM_IS_GOOD(pa_stream_get_state(s->stream))) {
        av_log(s, AV_LOG_ERROR, "PulseAudio stream is in invalid state.\n");
        goto fail;
    }
    while (pa_stream_writable_size(s->stream) < s->minreq) {
        if (s->nonblocking) {
            pa_threaded_mainloop_unlock(s->mainloop);
            return AVERROR(EAGAIN);
        } else
            pa_threaded_mainloop_wait(s->mainloop);
    }

    if ((ret = pa_stream_write(s->stream, pkt->data, pkt->size, NULL, 0, PA_SEEK_RELATIVE)) < 0) {
        av_log(s, AV_LOG_ERROR, "pa_stream_write failed: %s\n", pa_strerror(ret));
        goto fail;
    }
    if ((writable_size = pa_stream_writable_size(s->stream)) >= s->minreq)
        avdevice_dev_to_app_control_message(h, AV_DEV_TO_APP_BUFFER_WRITABLE, &writable_size, sizeof(writable_size));

    pa_threaded_mainloop_unlock(s->mainloop);

    return 0;
  fail:
    pa_threaded_mainloop_unlock(s->mainloop);
    return AVERROR_EXTERNAL;
}


static int pulse_write_packet_video(AVFormatContext *h, AVPacket *pkt) {
    PulseData *s = h->priv_data;
    int64_t current_time = av_gettime();

    av_log(s, AV_LOG_DEBUG, "time:%"PRId64" dts:%"PRId64" size:%d duration:%"PRId64"\n",
           current_time, pkt->dts, pkt->size, pkt->duration);

    return buffer_push(s, pkt);

    return 0;
}


static int pulse_write_packet(AVFormatContext *h, AVPacket *pkt) {
    AVStream *st = h->streams[pkt->stream_index];
    if (st->codecpar->codec_type == AVMEDIA_TYPE_AUDIO)
        return pulse_write_packet_audio(h, pkt);
    else if (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO)
        return pulse_write_packet_video(h, pkt);

    return AVERROR(EIO);
}


static int pulse_write_frame(AVFormatContext *h, int stream_index,
                             AVFrame **frame, unsigned flags)
{
    AVPacket pkt;

    /* Planar formats are not supported yet. */
    if (flags & AV_WRITE_UNCODED_FRAME_QUERY)
        return av_sample_fmt_is_planar(h->streams[stream_index]->codecpar->format) ?
               AVERROR(EINVAL) : 0;

    pkt.data     = (*frame)->data[0];
    pkt.size     = (*frame)->nb_samples * av_get_bytes_per_sample((*frame)->format) * (*frame)->ch_layout.nb_channels;
    pkt.dts      = (*frame)->pkt_dts;
    pkt.duration = (*frame)->duration;
    return pulse_write_packet(h, &pkt);
}


static void pulse_get_output_timestamp(AVFormatContext *h, int stream, int64_t *dts, int64_t *wall)
{
    PulseData *s = h->priv_data;
    pa_usec_t latency;
    int neg;
    pa_threaded_mainloop_lock(s->mainloop);
    pa_stream_get_latency(s->stream, &latency, &neg);
    pa_threaded_mainloop_unlock(s->mainloop);
    if (wall)
        *wall = av_gettime();
    if (dts)
        *dts = s->timestamp - (neg ? -latency : latency);
}

static int pulse_get_device_list(AVFormatContext *h, AVDeviceInfoList *device_list)
{
    PulseData *s = h->priv_data;
    return ff_pulse_audio_get_devices(device_list, s->server, 1);
}

static int pulse_control_message(AVFormatContext *h, int type,
                                 void *data, size_t data_size)
{
    PulseData *s = h->priv_data;
    int ret;

    switch(type) {
    case AV_APP_TO_DEV_PAUSE:
        return pulse_set_pause(s, 1);
    case AV_APP_TO_DEV_PLAY:
        return pulse_set_pause(s, 0);
    case AV_APP_TO_DEV_TOGGLE_PAUSE:
        return pulse_set_pause(s, !pa_stream_is_corked(s->stream));
    case AV_APP_TO_DEV_MUTE:
        if (!s->mute) {
            s->mute = 1;
            return pulse_set_mute(s);
        }
        return 0;
    case AV_APP_TO_DEV_UNMUTE:
        if (s->mute) {
            s->mute = 0;
            return pulse_set_mute(s);
        }
        return 0;
    case AV_APP_TO_DEV_TOGGLE_MUTE:
        s->mute = !s->mute;
        return pulse_set_mute(s);
    case AV_APP_TO_DEV_SET_VOLUME:
        return pulse_set_volume(s, *(double *)data);
    case AV_APP_TO_DEV_GET_VOLUME:
        s->last_volume = PA_VOLUME_INVALID;
        pa_threaded_mainloop_lock(s->mainloop);
        ret = pulse_update_sink_input_info(h);
        pa_threaded_mainloop_unlock(s->mainloop);
        return ret;
    case AV_APP_TO_DEV_GET_MUTE:
        s->mute = -1;
        pa_threaded_mainloop_lock(s->mainloop);
        ret = pulse_update_sink_input_info(h);
        pa_threaded_mainloop_unlock(s->mainloop);
        return ret;
    default:
        break;
    }
    return AVERROR(ENOSYS);
}

#define OFFSET(a) offsetof(PulseData, a)
#define E AV_OPT_FLAG_ENCODING_PARAM
static const AVOption options[] = {
    { "server",          "set PulseAudio server",            OFFSET(server),          AV_OPT_TYPE_STRING, {.str = NULL}, 0, 0, E },
    { "name",            "set application name",             OFFSET(name),            AV_OPT_TYPE_STRING, {.str = LIBAVFORMAT_IDENT},  0, 0, E },
    { "stream_name",     "set stream description",           OFFSET(stream_name),     AV_OPT_TYPE_STRING, {.str = NULL}, 0, 0, E },
    { "device",          "set device name",                  OFFSET(device),          AV_OPT_TYPE_STRING, {.str = NULL}, 0, 0, E },
    { "buffer_size",     "set buffer size in bytes",         OFFSET(buffer_size),     AV_OPT_TYPE_INT,    {.i64 = 0}, 0, INT_MAX, E },
    { "buffer_duration", "set buffer duration in millisecs", OFFSET(buffer_duration), AV_OPT_TYPE_INT,    {.i64 = 0}, 0, INT_MAX, E },
    { "prebuf",          "set pre-buffering size",           OFFSET(prebuf),          AV_OPT_TYPE_INT,    {.i64 = 0}, 0, INT_MAX, E },
    { "minreq",          "set minimum request size",         OFFSET(minreq),          AV_OPT_TYPE_INT,    {.i64 = 0}, 0, INT_MAX, E },
    { NULL }
};

static const AVClass pulse_muxer_class = {
    .class_name     = "PulseAudio outdev + v4l2",
    .item_name      = av_default_item_name,
    .option         = options,
    .version        = LIBAVUTIL_VERSION_INT,
    .category       = AV_CLASS_CATEGORY_DEVICE_AUDIO_OUTPUT,
};

const FFOutputFormat ff_v4pulse_muxer = {
    .p.name               = "v4pulse",
    .p.long_name          = NULL_IF_CONFIG_SMALL("v4l2 + Pulse audio output"),
    .priv_data_size       = sizeof(PulseData),
    .p.audio_codec        = AV_NE(AV_CODEC_ID_PCM_S16BE, AV_CODEC_ID_PCM_S16LE),
    .p.video_codec        = AV_CODEC_ID_RAWVIDEO,
    .write_header         = pulse_write_header,
    .write_packet         = pulse_write_packet,
    .write_trailer        = pulse_write_trailer,
    .get_output_timestamp = pulse_get_output_timestamp,
    .get_device_list      = pulse_get_device_list,
    .control_message      = pulse_control_message,
#if FF_API_ALLOW_FLUSH
    .p.flags              = AVFMT_NOFILE | AVFMT_ALLOW_FLUSH,
#else
    .p.flags              = AVFMT_NOFILE,
#endif
    .p.priv_class         = &pulse_muxer_class,
    .flags_internal       = FF_OFMT_FLAG_ALLOW_FLUSH,
};
