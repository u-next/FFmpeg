/*
 * Copyright (c) 2013 Clément Bœsch
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

#include "libavutil/imgutils.h"
#include "libavutil/pixdesc.h"
#include "libavformat/avformat.h"
#include "libavformat/mux.h"
#include "libavutil/time.h"
#include "v4l2-common.h"
#include <pthread.h>

#define RING_BUFFER_SIZE 5
#define MIN_FRAMES_TO_START 3

typedef struct {
    AVClass *class;
    int fd;
    
    // Buffer management
    AVPacket *ring_buffer[RING_BUFFER_SIZE];
    int buffer_head;
    int buffer_tail;
    int buffer_count;
    
    // Thread management
    pthread_t output_thread;
    pthread_mutex_t buffer_lock;
    pthread_cond_t buffer_cond;
    int thread_running;
    
    // Timing management
    int64_t frame_interval;
    int64_t last_output_time;
} V4L2Context;

static void buffer_init(V4L2Context *ctx) {
    memset(ctx->ring_buffer, 0, sizeof(ctx->ring_buffer));
    ctx->buffer_head = 0;
    ctx->buffer_tail = 0;
    ctx->buffer_count = 0;
    pthread_mutex_init(&ctx->buffer_lock, NULL);
    pthread_cond_init(&ctx->buffer_cond, NULL);
    ctx->thread_running = 0;
    ctx->last_output_time = 0;
}

static void buffer_destroy(V4L2Context *ctx) {
    pthread_mutex_lock(&ctx->buffer_lock);
    for (int i = 0; i < RING_BUFFER_SIZE; i++) {
        if (ctx->ring_buffer[i]) {
            av_packet_free(&ctx->ring_buffer[i]);
        }
    }
    pthread_mutex_unlock(&ctx->buffer_lock);
    pthread_mutex_destroy(&ctx->buffer_lock);
    pthread_cond_destroy(&ctx->buffer_cond);
}

static int buffer_push(V4L2Context *ctx, AVPacket *pkt) {
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

static AVPacket* buffer_pop(V4L2Context *ctx) {
    pthread_mutex_lock(&ctx->buffer_lock);
    
    while (ctx->thread_running && ctx->buffer_count == 0) {
        pthread_cond_wait(&ctx->buffer_cond, &ctx->buffer_lock);
    }
    
    if (!ctx->thread_running) {
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

static void *output_thread_func(void *arg) {
    V4L2Context *ctx = arg;
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
                if (write(ctx->fd, pkt->data, pkt->size) == -1) {
                    av_log(ctx, AV_LOG_ERROR, "Failed to write frame: %s\n", av_err2str(AVERROR(errno)));
                }
                av_packet_free(&pkt);
                av_log(ctx, AV_LOG_VERBOSE, "frame written to device, buffer level: %d/%d, time delta: %"PRId64" us\n", 
                       ctx->buffer_count, RING_BUFFER_SIZE, time_delta);
                next_frame_time += ctx->frame_interval;
                ctx->last_output_time = current_time;
            } else if (ctx->thread_running) {
                // Buffer underrun
                av_log(ctx, AV_LOG_WARNING, "Buffer underrun detected, retrying in %"PRId64" us\n", ctx->frame_interval / 2);
                next_frame_time = current_time + (ctx->frame_interval / 2);
            }
        } else {
            // sleep and wake up 3ms earlier then make short snoozes
            int64_t sleep_time = next_frame_time - current_time;
            if (sleep_time > 3000) {
                av_usleep(sleep_time - 3000); // Wake up 1ms early to account for scheduling
            }
            else {
                av_usleep(300);
            }
        }
    }
    
    return NULL;
}

static av_cold int write_header(AVFormatContext *s1)
{
    int res = 0, flags = O_RDWR;
    struct v4l2_format fmt = {
        .type = V4L2_BUF_TYPE_VIDEO_OUTPUT
    };
    V4L2Context *s = s1->priv_data;
    AVCodecParameters *par;
    uint32_t v4l2_pixfmt;

    if (s1->flags & AVFMT_FLAG_NONBLOCK)
        flags |= O_NONBLOCK;

    s->fd = open(s1->url, flags);
    if (s->fd < 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "Unable to open V4L2 device '%s'\n", s1->url);
        return res;
    }

    if (s1->nb_streams != 1 ||
        s1->streams[0]->codecpar->codec_type != AVMEDIA_TYPE_VIDEO) {
        av_log(s1, AV_LOG_ERROR,
               "V4L2 output device supports only a single raw video stream\n");
        return AVERROR(EINVAL);
    }

    par = s1->streams[0]->codecpar;

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
    s->frame_interval = (int64_t)s1->streams[0]->time_base.num * 1000000LL / s1->streams[0]->time_base.den;
    av_log(s1, AV_LOG_INFO, "Frame interval: %"PRId64" us\n", s->frame_interval);
    
    // Initialize buffer and start thread
    buffer_init(s);
    s->thread_running = 1;
    if (pthread_create(&s->output_thread, NULL, output_thread_func, s) != 0) {
        res = AVERROR(errno);
        av_log(s1, AV_LOG_ERROR, "Failed to create output thread\n");
        return res;
    }

    return res;
}

static int write_packet(AVFormatContext *s1, AVPacket *pkt)
{
    V4L2Context *s = s1->priv_data;
    int64_t current_time = av_gettime();
    
    av_log(s, AV_LOG_DEBUG, "time:%"PRId64" dts:%"PRId64" size:%d duration:%"PRId64"\n",
           current_time, pkt->dts, pkt->size, pkt->duration);
    
    return buffer_push(s, pkt);
}

static int write_trailer(AVFormatContext *s1)
{
    V4L2Context *s = s1->priv_data;
    
    // Stop thread
    s->thread_running = 0;
    pthread_cond_signal(&s->buffer_cond);
    pthread_join(s->output_thread, NULL);
    
    // Cleanup
    buffer_destroy(s);
    close(s->fd);
    return 0;
}

static const AVClass v4l2_class = {
    .class_name = "V4L2 outdev",
    .item_name  = av_default_item_name,
    .version    = LIBAVUTIL_VERSION_INT,
    .category   = AV_CLASS_CATEGORY_DEVICE_VIDEO_OUTPUT,
};

const FFOutputFormat ff_v4l2_muxer = {
    .p.name         = "video4linux2,v4l2",
    .p.long_name    = NULL_IF_CONFIG_SMALL("Video4Linux2 output device"),
    .priv_data_size = sizeof(V4L2Context),
    .p.audio_codec  = AV_CODEC_ID_NONE,
    .p.video_codec  = AV_CODEC_ID_RAWVIDEO,
    .write_header   = write_header,
    .write_packet   = write_packet,
    .write_trailer  = write_trailer,
    .p.flags        = AVFMT_NOFILE,
    .p.priv_class   = &v4l2_class,
};
