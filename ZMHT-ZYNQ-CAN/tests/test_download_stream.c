/* Production 9012 parser and connection lifecycle; no CAN hardware needed. */
#define main can_application_main
#include "../src/main18.c"
#undef main
#include <assert.h>

static size_t packet_make(uint8_t *out, uint16_t command, const void *data, size_t size) {
    DATA_ReqPacket hdr = {0};
    hdr.sync_word = htonl(PC_ARM_DATA_WORD);
    hdr.data_len = htonl(size);
    hdr.cmd_type = htons(command);
    hdr.checksum = calculate_xor_checksum((const uint8_t *)&hdr, 12);
    memcpy(out, &hdr, sizeof(hdr));
    if (size) memcpy(out + sizeof(hdr), data, size);
    return sizeof(hdr) + size;
}

static void feed(DownloadStream *stream, const uint8_t *data, size_t size) {
    assert(size <= sizeof(stream->bytes) - stream->used);
    memcpy(stream->bytes + stream->used, data, size);
    stream->used += size;
    process_download_data_batch(stream);
}

static void expect_frame(uint32_t id) {
    PC_ARM_DATA_DATA frame;
    assert(ringbuffer_32ch_get(&g_data_buf_set, 0, &frame, 32) == 1);
    assert(frame.idr == id);
    assert(ringbuffer_32ch_len(&g_data_buf_set, 0, 32) == 0);
}

static void send_all(int fd, const uint8_t *data, size_t size) {
    while (size) {
        ssize_t n = send(fd, data, size, 0);
        assert(n > 0);
        data += n;
        size -= n;
    }
}

static pthread_t start_client(int *pair) {
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0);
    int *fd = malloc(sizeof(*fd));
    assert(fd);
    *fd = pair[1];
    pthread_t tid;
    assert(pthread_create(&tid, NULL, tcp_client_handler_9012, fd) == 0);
    /* Wait for registration without unsynchronized global socket reads. */
    int registered = 0;
    for (int i = 0; i < 3000; ++i) {
        pthread_mutex_lock(&data_download_mutex);
        registered = data_download_socket == pair[1];
        pthread_mutex_unlock(&data_download_mutex);
        if (registered) break;
        usleep(1000);
    }
    assert(registered);
    return tid;
}

int main(void) {
    assert(ringbuffer_32ch_init(&g_data_buf_set, sizeof(PC_ARM_DATA_DATA), 16, 32) == 0);
    assert(ringbuffer_32ch_init(&g_simulator_buf_set, sizeof(PC_ARM_SIMULATOR_DATA), 16, 32) == 0);
    assert(ringbuffer_32ch_init(&g_pulse_buf_set, sizeof(SecondPulseData), 16, 32) == 0);
    assert(ringbuffer_32ch_init(&g_bcast_group_buf_set, sizeof(BroadcastFrameGroup), 16, 6) == 0);
    can_configs[0].valid = 1;
    DownloadStream *stream = calloc(1, sizeof(*stream));
    DownloadStream *other = calloc(1, sizeof(*other));
    assert(stream && other);
    PC_ARM_DATA_DATA frame = {0};
    frame.head = PC_STATUS_HEAD;
    frame.length = sizeof(frame);
    frame.type = 1;
    frame.idr = 0x123;
    frame.xor_checksum = calculate_xor_checksum((uint8_t *)&frame, sizeof(frame) - 4);
    uint8_t packet[512], bad[512];
    size_t size = packet_make(packet, 1, &frame, sizeof(frame));

    for (size_t split = 1; split < size; ++split) {
        feed(stream, packet, split);
        assert(ringbuffer_32ch_len(&g_data_buf_set, 0, 32) == 0);
        feed(stream, packet + split, size - split);
        expect_frame(frame.idr);
        assert(stream->used == 0);
    }
    /* Largest accepted outer packet: retain all bytes until the last arrives. */
    memset(stream->bytes, 0, sizeof(stream->bytes));
    DATA_ReqPacket maximum = {0};
    maximum.sync_word = htonl(PC_ARM_DATA_WORD);
    maximum.data_len = htonl(sizeof(stream->bytes) - sizeof(maximum));
    maximum.cmd_type = htons(0xFFFF); /* unknown command has no side effects */
    maximum.checksum = calculate_xor_checksum((uint8_t *)&maximum, 12);
    memcpy(stream->bytes, &maximum, sizeof(maximum));
    stream->used = sizeof(stream->bytes) - 1;
    process_download_data_batch(stream);
    assert(stream->used == sizeof(stream->bytes) - 1);
    const uint8_t last = 0;
    feed(stream, &last, 1);
    assert(stream->used == 0);

    /* Simultaneous half-packet contexts must be independent. */
    feed(stream, packet, size - 1);
    feed(other, packet, size);
    expect_frame(frame.idr);
    assert(stream->used == size - 1 && other->used == 0);
    feed(stream, packet + size - 1, 1);
    expect_frame(frame.idr);

    uint8_t combined[1024];
    memcpy(combined, packet, size);
    memcpy(combined + size, packet, size);
    feed(stream, combined, 2 * size);
    assert(ringbuffer_32ch_len(&g_data_buf_set, 0, 32) == 2);
    PC_ARM_DATA_DATA received;
    assert(ringbuffer_32ch_get(&g_data_buf_set, 0, &received, 32) == 1);
    expect_frame(frame.idr);

    /* Resynchronize after unaligned junk and a checksum-valid absurd length. */
    feed(stream, (const uint8_t *)"junk!", 5);
    DATA_ReqPacket header = {0};
    header.sync_word = htonl(PC_ARM_DATA_WORD);
    header.data_len = htonl(UINT32_MAX);
    header.checksum = calculate_xor_checksum((uint8_t *)&header, 12);
    feed(stream, (uint8_t *)&header, sizeof(header));
    header.checksum ^= 1;
    feed(stream, (uint8_t *)&header, sizeof(header));
    feed(stream, packet, size);
    expect_frame(frame.idr);

    for (uint16_t cmd = 1; cmd <= 5; ++cmd) {
        size_t n = packet_make(bad, cmd, NULL, 0);
        feed(stream, bad, n);
    }
    assert(stream->used == 0);
    /* Legacy non-48-multiple packet must not permit an underflow checksum. */
    uint8_t legacy[49] = {0};
    memcpy(legacy, &frame, sizeof(frame));
    legacy[6] = 0;
    feed(stream, bad, packet_make(bad, 1, legacy, sizeof(legacy)));
    legacy[6] = 252;
    feed(stream, bad, packet_make(bad, 1, legacy, sizeof(legacy)));
    legacy[6] = sizeof(frame);
    legacy[7] = 255;
    ((PC_ARM_DATA_DATA *)legacy)->xor_checksum = calculate_xor_checksum(legacy, sizeof(frame) - 4);
    feed(stream, bad, packet_make(bad, 1, legacy, sizeof(legacy)));
    assert(ringbuffer_32ch_len(&g_data_buf_set, 0, 32) == 0);

    PC_ARM_SIMULATOR_DATA sim = {0};
    sim.head = PC_STATUS_HEAD;
    sim.send_len = sizeof(sim.send_data);
    feed(stream, bad, packet_make(bad, 3, &sim, sizeof(sim)));
    PC_ARM_SIMULATOR_DATA sim_out;
    assert(ringbuffer_32ch_get(&g_simulator_buf_set, 0, &sim_out, 32) == 1);
    assert(memcmp(&sim, &sim_out, sizeof(sim)) == 0);
    sim.send_len++;
    feed(stream, bad, packet_make(bad, 3, &sim, sizeof(sim)));
    sim.send_len = 0;
    sim.channel_id = 255;
    feed(stream, bad, packet_make(bad, 3, &sim, sizeof(sim)));
    assert(ringbuffer_32ch_len(&g_simulator_buf_set, 0, 32) == 0);

    /* Odd-sized legacy pulse followed by CAN exercises unaligned checksums. */
    SecondPulseData pulse = {.timestamp = 123, .pulse_flag = 1}, pulse_out;
    size_t pulse_size = packet_make(combined, 2, &pulse, sizeof(pulse));
    memcpy(combined + pulse_size, packet, size);
    feed(stream, combined, pulse_size + size);
    assert(ringbuffer_32ch_get(&g_pulse_buf_set, 0, &pulse_out, 32) == 1);
    assert(memcmp(&pulse, &pulse_out, sizeof(pulse)) == 0);
    expect_frame(frame.idr);

    g_bcast_cfg.enable = BCAST_ENABLE;
    g_bcast_cfg.active_mask = 1;
    set_broadcast_rx_gate(1);
    frame.type = 2;
    frame.xor_checksum = calculate_xor_checksum((uint8_t *)&frame, sizeof(frame) - 4);
    feed(stream, bad, packet_make(bad, 2, &frame, sizeof(frame)));
    BroadcastFrameGroup group;
    assert(ringbuffer_32ch_get(&g_bcast_group_buf_set, 0, &group, 6) == 1);
    assert(group.frame_count == 1 && group.frames[0].can_id == frame.idr);

    /* Real handler replacement: old partial bytes cannot prefix new data. */
    int first[2], second[2];
    pthread_t old = start_client(first);
    send_all(first[0], packet, size - 1);
    pthread_t current = start_client(second);
    assert(pthread_join(old, NULL) == 0);
    send_all(second[0], packet, size);
    shutdown(second[0], SHUT_WR);
    assert(pthread_join(current, NULL) == 0);
    expect_frame(0x123);
    assert(data_download_socket == -1);
    close(first[0]);
    close(second[0]);
    free(stream);
    free(other);
    ringbuffer_32ch_destory(&g_data_buf_set, 32);
    ringbuffer_32ch_destory(&g_simulator_buf_set, 32);
    ringbuffer_32ch_destory(&g_pulse_buf_set, 32);
    ringbuffer_32ch_destory(&g_bcast_group_buf_set, 6);
    puts("9012 download stream regression passed");
    return 0;
}
