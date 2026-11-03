/* Host test for broadcast queue transactions; no device or network access. */
#define main can_application_main
#include "../src/main18.c"
#undef main
#include <assert.h>

static void reset_state(void) {
    pthread_mutex_lock(&bcast_state_mutex);
    reset_broadcast_buffers();
    pthread_mutex_unlock(&bcast_state_mutex);
}

static void *producer(void *arg) {
    uint8_t channel = (uintptr_t)arg;
    PC_ARM_DATA_DATA frames[4] = {0};
    for (uint32_t i = 1; i <= 10000; ++i) {
        for (int j = 0; j < 4; ++j) {
            frames[j].idr = i;
            frames[j].dw1r = htonl(i);
            frames[j].dw2r = htonl(j);
        }
        int rc = enqueue_broadcast_group(channel, frames, 4);
        assert(rc == 0 || rc == -EINVAL || rc == -ENOMEM);
    }
    return NULL;
}

static void *consumer(void *arg) {
    uint8_t channel = (uintptr_t)arg;
    BroadcastFrameGroup group;
    for (int i = 0; i < 10000; ++i) {
        if (take_broadcast_group(channel, &group)) {
            assert(group.frame_count == 4);
            for (int j = 0; j < 4; ++j) {
                assert(group.frames[j].can_id == group.frames[0].can_id);
                assert(group.frames[j].data[0] == group.frames[0].can_id);
                assert(group.frames[j].data[1] == (uint32_t)j);
            }
        }
    }
    return NULL;
}

static void *scheduler(void *arg) {
    (void)arg;
    for (int i = 0; i < 10000; ++i) {
        if (i % 7 == 0) reset_state();
        process_broadcast_pps();
    }
    return NULL;
}

static void *observer(void *arg) {
    (void)arg;
    for (int i = 0; i < 10000; ++i)
        for (uint8_t ch = 0; ch < AXICAN_MAX; ++ch)
            assert(broadcast_buffer_available(ch) <= BCAST_GROUP_QUEUE_DEPTH);
    return NULL;
}

int main(void) {
    assert(ringbuffer_32ch_init(&g_bcast_group_buf_set, sizeof(BroadcastFrameGroup),
                               BCAST_GROUP_QUEUE_DEPTH, AXICAN_MAX) == 0);
    for (uint8_t ch = 0; ch < AXICAN_MAX; ++ch) {
        assert(pthread_mutex_init(&g_bcast_wake[ch].mutex, NULL) == 0);
        assert(pthread_cond_init(&g_bcast_wake[ch].cond, NULL) == 0);
        can_configs[ch].valid = 1;
        can_configs[ch].state = CAN_STATE_OPENED;
    }
    g_bcast_cfg.enable = BCAST_ENABLE;
    g_bcast_cfg.mode = BCAST_MODE_SIMULTANEOUS;
    g_bcast_cfg.active_mask = BCAST_CAN_MASK;
    PC_ARM_DATA_DATA frame = {.idr = 123};
    BroadcastFrameGroup group;
    process_broadcast_pps();
    for (int i = 0; i < BCAST_GROUP_QUEUE_DEPTH; ++i)
        assert(enqueue_broadcast_group(0, &frame, 1) == 0);
    assert(enqueue_broadcast_group(0, &frame, 1) == -ENOMEM);
    assert(broadcast_buffer_available(0) == 0);
    assert(take_broadcast_group(0, &group) == 1);
    assert(group.frame_count == 1 && group.frames[0].can_id == 123);
    assert(take_broadcast_group(0, &group) == 0); /* one group per wake */
    reset_state();
    assert(broadcast_buffer_available(0) == BCAST_GROUP_QUEUE_DEPTH);
    assert(enqueue_broadcast_group(0, &frame, 1) == -EINVAL); /* gate closed */
    /* A wake observed before reset must not consume the next generation. */
    process_broadcast_pps();
    reset_state();
    set_broadcast_rx_gate(1);
    assert(enqueue_broadcast_group(0, &frame, 1) == 0);
    assert(take_broadcast_group(0, &group) == 0);
    process_broadcast_pps();
    assert(take_broadcast_group(0, &group) == 1);
    reset_state();

    /* Existing polling and A/B selection rules. */
    g_bcast_cfg.mode = BCAST_MODE_POLLING;
    g_bcast_cfg.active_mask = 0x05;
    g_bcast_cfg.poll_next_channel = 0;
    assert(select_broadcast_mask_on_pps() == 1);
    assert(select_broadcast_mask_on_pps() == 4);
    assert(select_broadcast_mask_on_pps() == 1);
    g_bcast_cfg.mode = BCAST_MODE_AB_ALTERNATE;
    g_bcast_cfg.group_a_mask = 0x15;
    g_bcast_cfg.group_b_mask = 0x2A;
    g_bcast_cfg.current_group = 0;
    assert(select_broadcast_mask_on_pps() == 0x15);
    assert(select_broadcast_mask_on_pps() == 0x2A);
    g_bcast_cfg.mode = BCAST_MODE_SIMULTANEOUS;
    g_bcast_cfg.active_mask = BCAST_CAN_MASK;

    pthread_t threads[6];
    assert(pthread_create(&threads[0], NULL, producer, (void *)(uintptr_t)0) == 0);
    assert(pthread_create(&threads[1], NULL, producer, (void *)(uintptr_t)1) == 0);
    assert(pthread_create(&threads[2], NULL, consumer, (void *)(uintptr_t)0) == 0);
    assert(pthread_create(&threads[3], NULL, consumer, (void *)(uintptr_t)1) == 0);
    assert(pthread_create(&threads[4], NULL, scheduler, NULL) == 0);
    assert(pthread_create(&threads[5], NULL, observer, NULL) == 0);
    for (int i = 0; i < 6; ++i) assert(pthread_join(threads[i], NULL) == 0);
    reset_state();
    for (uint8_t ch = 0; ch < AXICAN_MAX; ++ch) {
        assert(broadcast_buffer_available(ch) == BCAST_GROUP_QUEUE_DEPTH);
        assert(g_bcast_wake[ch].pending == 0);
        pthread_mutex_destroy(&g_bcast_wake[ch].mutex);
        pthread_cond_destroy(&g_bcast_wake[ch].cond);
    }
    ringbuffer_32ch_destory(&g_bcast_group_buf_set, AXICAN_MAX);
    puts("broadcast concurrency regression passed");
    return 0;
}
