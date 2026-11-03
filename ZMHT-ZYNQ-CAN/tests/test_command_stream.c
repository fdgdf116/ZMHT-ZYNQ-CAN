/* Host regression: exercise the production parser and 9009 connection thread.
 * No CAN device is opened; version replies use a local socket pair.
 */
#define main can_application_main
#include "../src/main18.c"
#undef main
#include <assert.h>

static size_t make_packet(uint8_t *buffer, uint16_t command, uint32_t length) {
    ReqPacket header = {0};
    header.sync_word = htonl(REQ_SYNC_WORD);
    header.data_len = htonl(length);
    header.counter = htons(7);
    header.cmd_type = htons(command);
    memcpy(buffer, &header, sizeof(header));
    uint32_t words[3];
    memcpy(words, buffer, sizeof(words));
    header.checksum = words[0] ^ words[1] ^ words[2];
    memcpy(buffer, &header, sizeof(header));
    return sizeof(header);
}

static void send_bytes(int fd, const uint8_t *data, size_t size) {
    while (size) {
        ssize_t n = send(fd, data, size, 0);
        if (n <= 0) perror("socket transfer");
        assert(n > 0);
        data += n;
        size -= n;
    }
}

static uint32_t receive_result(int fd) {
    VersionQueryResponse response;
    size_t received = 0;
    while (received < sizeof(response)) {
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        assert(poll(&pfd, 1, 3000) == 1);
        ssize_t n = recv(fd, (uint8_t *)&response + received,
                         sizeof(response) - received, MSG_DONTWAIT);
        if (n <= 0) perror("socket transfer");
        assert(n > 0);
        received += n;
    }
    assert(ntohl(response.CAN_DATA_RE_HEAD) == ACK_SYNC_WORD);
    assert(ntohl(response.CAN_DATA_RE_LENGTH) == sizeof(response.version));
    assert(ntohs(response.CAN_DATA_RE_COMMOND) == CMD_VERSION_QUERY);
    assert(ntohs(response.CAN_DATA_RE_COUNT) == 7);
    return ntohl(response.version);
}

static void receive_version(int fd) {
    assert(receive_result(fd) == ARM_APP_VERSION);
}

int main(void) {
    uint8_t packet[20];
    make_packet(packet, CMD_VERSION_QUERY, 4);
    uint32_t query = htonl(VERSION_QUERY_ARM);
    memcpy(packet + sizeof(ReqPacket), &query, sizeof(query));
    ReqPacket *parsed;
    for (size_t n = 0; n < sizeof(packet); ++n) {
        assert(parse_request_packet(packet, n, &parsed) == PARSE_NEED_MORE);
        assert(parsed == NULL);
    }
    assert(parse_request_packet(packet, sizeof(packet), &parsed) == PARSE_SUCCESS);
    uint8_t bad[sizeof(ReqPacket)];
    make_packet(bad, CMD_CAN_CONFIG, UINT32_MAX);
    assert(parse_request_packet(bad, sizeof(bad), &parsed) == PARSE_ERR_LEN);
    bad[12] ^= 1;
    assert(parse_request_packet(bad, sizeof(bad), &parsed) == PARSE_ERR_CHECKSUM);
    for (uint16_t cmd = CMD_CAN_BUF_QUERY; cmd <= CMD_CAN_BROADCAST_CONFIG; ++cmd)
        assert(command_min_payload_size(cmd) > 0);

    int commands[2], replies[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, commands) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, replies) == 0);
    data_upload_socket = replies[0];
    int *fd = malloc(sizeof(*fd));
    assert(fd);
    *fd = commands[1];
    pthread_t thread;
    assert(pthread_create(&thread, NULL, tcp_client_handler_9009, fd) == 0);

    /* Every possible split, with no response until the remaining bytes arrive. */
    for (size_t split = 1; split < sizeof(packet); ++split) {
        send_bytes(commands[0], packet, split);
        struct pollfd pfd = {.fd = replies[1], .events = POLLIN};
        assert(poll(&pfd, 1, 10) == 0);
        send_bytes(commands[0], packet + split, sizeof(packet) - split);
        receive_version(replies[1]);
    }
    uint8_t combined[60];
    for (int i = 0; i < 3; ++i) memcpy(combined + i * 20, packet, 20);
    send_bytes(commands[0], combined, sizeof(combined));
    for (int i = 0; i < 3; ++i) receive_version(replies[1]);

    /* A burst larger than the receive buffer must retain all packet boundaries. */
    uint8_t burst[4400];
    for (size_t i = 0; i < sizeof(burst); i += sizeof(packet))
        memcpy(burst + i, packet, sizeof(packet));
    send_bytes(commands[0], burst, sizeof(burst));
    for (int i = 0; i < 220; ++i) receive_version(replies[1]);

    /* Junk, oversize header, bad checksum, and short known commands must not
     * prevent the following valid query from being dispatched. */
    send_bytes(commands[0], (const uint8_t *)"junk!", 5);
    make_packet(bad, CMD_CAN_CONFIG, UINT32_MAX);
    send_bytes(commands[0], bad, sizeof(bad));
    bad[12] ^= 1;
    send_bytes(commands[0], bad, sizeof(bad));
    for (uint16_t cmd = CMD_CAN_BUF_QUERY; cmd <= CMD_CAN_BROADCAST_CONFIG; ++cmd) {
        make_packet(bad, cmd, 0);
        send_bytes(commands[0], bad, sizeof(bad));
    }
    send_bytes(commands[0], packet, sizeof(packet));
    receive_version(replies[1]);

    /* 0xAC reports whether the running ARM application is CAN or CAN FD. */
    make_packet(packet, CMD_VERSION_QUERY, 4);
    query = htonl(VERSION_QUERY_ARM_PROGRAM);
    memcpy(packet + sizeof(ReqPacket), &query, sizeof(query));
    send_bytes(commands[0], packet, sizeof(packet));
    assert(receive_result(replies[1]) == ARM_PROGRAM_TYPE);

    /* A disconnected half packet must never be dispatched. */
    send_bytes(commands[0], packet, 19);
    shutdown(commands[0], SHUT_WR);
    assert(pthread_join(thread, NULL) == 0);
    uint8_t extra;
    assert(recv(replies[1], &extra, 1, MSG_DONTWAIT) == -1);
    assert(errno == EAGAIN || errno == EWOULDBLOCK);
    close(commands[0]);
    close(replies[0]);
    close(replies[1]);
    data_upload_socket = -1;
    puts("9009 command stream regression passed");
    return 0;
}
