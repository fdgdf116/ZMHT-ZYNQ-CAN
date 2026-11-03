/* Exercise the actual information-query reply over local sockets. */
#define main can_application_main
#include "../src/main18.c"
#undef main
#include <assert.h>

static uint32_t reference_xor(const uint8_t *bytes, size_t count) {
    assert(count % 4 == 0);
    uint32_t result = 0;
    for (size_t i = 0; i < count; i += 4) {
        uint32_t word;
        memcpy(&word, bytes + i, sizeof(word));
        result ^= word;
    }
    return result;
}

static void read_exact(int fd, void *buffer, size_t size) {
    uint8_t *next = buffer;
    while (size) {
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        assert(poll(&pfd, 1, 3000) == 1);
        ssize_t n = recv(fd, next, size, MSG_DONTWAIT);
        assert(n > 0);
        next += n;
        size -= n;
    }
}

int main(void) {
    int commands[2], upload[2];
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, commands) == 0);
    assert(socketpair(AF_UNIX, SOCK_STREAM, 0, upload) == 0);
    data_upload_socket = upload[0];
    for (uint8_t ch = 0; ch < AXICAN_MAX; ++ch) {
        can_configs[ch].can_id = ch;
        can_configs[ch].baud_rate = 125000U * (ch + 1);
        can_configs[ch].mode = ch % 2;
        can_configs[ch].can_type = ch % 3;
        channel_frame_count[ch] = 100 + ch;
    }
    uint8_t query[sizeof(ReqPacket) + sizeof(uint32_t)] = {0};
    ReqPacket *request = (ReqPacket *)query;
    request->data_len = htonl(sizeof(uint32_t));
    request->cmd_type = htons(CMD_CAN_INFO_QUERY);
    memset(request->data, 0xFF, sizeof(uint32_t));
    for (int pass = 0; pass < 2; ++pass) {
        if (pass) { /* Subsequent queries must reflect the changed channel. */
            can_configs[3].baud_rate = 1000000;
            can_configs[3].mode = 3;
        }
        handle_can_info_query(commands[0], request);
        uint8_t packet[sizeof(CAN_Send_Response_r) + AXICAN_MAX * sizeof(CanConfigParam)];
        read_exact(upload[1], packet, sizeof(packet));
        CAN_Send_Response_r *reply = (CAN_Send_Response_r *)packet;
        assert(ntohl(reply->CAN_DATA_RE_HEAD) == ACK_SYNC_WORD);
        assert(ntohl(reply->CAN_DATA_RE_LENGTH) == sizeof(packet));
        assert(ntohs(reply->CAN_DATA_RE_COMMOND) == CMD_CAN_INFO_QUERY);
        for (uint8_t ch = 0; ch < AXICAN_MAX; ++ch) {
            CanConfigParam row;
            memcpy(&row, reply->CAN_RE_DATA + ch * sizeof(row), sizeof(row));
            assert(row.head == PC_STATUS_HEAD && row.direction == 1);
            assert(row.ch_id == ch && row.length == sizeof(row));
            assert(row.baud_rate == can_configs[ch].baud_rate);
            assert(row.mode == can_configs[ch].mode);
            assert(row.type == can_configs[ch].can_type);
            assert(row.CAN_DATA_MODE == can_configs[ch].can_type);
            assert(row.ch_count == (uint32_t)(101 + ch + pass));
            assert(row.xor_checksum == reference_xor((uint8_t *)&row, sizeof(row) - 4));
        }
        const char expected[] = "CAN info query processed, result sent via port 9010";
        char ack[sizeof(expected) - 1];
        read_exact(commands[1], ack, sizeof(ack));
        assert(memcmp(ack, expected, sizeof(ack)) == 0);
    }
    data_upload_socket = -1;
    close(commands[0]); close(commands[1]);
    close(upload[0]); close(upload[1]);
    puts("CAN information query regression passed");
    return 0;
}
