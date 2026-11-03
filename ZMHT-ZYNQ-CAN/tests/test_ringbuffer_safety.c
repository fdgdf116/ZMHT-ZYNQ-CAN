/* Fault injection uses the production allocator call sites, without hardware. */
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int fail_at = -1, calls, live;
static void *allocations[64];
static void *test_malloc(size_t bytes) {
    if (calls++ == fail_at) {
        errno = ENOMEM;
        return NULL;
    }
    void *ptr = malloc(bytes);
    assert(ptr);
    for (int i = 0; i < 64; ++i) {
        if (!allocations[i]) {
            allocations[i] = ptr;
            live++;
            return ptr;
        }
    }
    abort();
}
static void test_free(void *ptr) {
    if (!ptr) return;
    for (int i = 0; i < 64; ++i) {
        if (allocations[i] == ptr) {
            allocations[i] = NULL;
            live--;
            free(ptr);
            return;
        }
    }
    assert(!"free of untracked allocation");
}
#define malloc test_malloc
#define free test_free
#include "../src/ringbuffer_channels.c"
#undef malloc
#undef free

static void expect_invalid(RingBuffer32ChSet *set, uint8_t ch, uint8_t limit) {
    uint32_t value = 0x123;
    assert(ringbuffer_32ch_put(set, ch, &value, limit) == 0);
    assert(ringbuffer_32ch_get(set, ch, &value, limit) == 0);
    assert(value == 0x123);
    assert(ringbuffer_32ch_len(set, ch, limit) == 0);
    assert(ringbuffer_32ch_avail(set, ch, limit) == 0);
    ringbuffer_32ch_reset(set, ch, limit);
}

int main(void) {
    RingBuffer32ChSet set = {0};
    expect_invalid(NULL, 0, 32);
    expect_invalid(&set, 0, 32);
    assert(rounddown_pow_of_two(UINT32_MAX) == (1U << 31));
    assert(rounddown_pow_of_two(100) == 64);
    assert(ringbuffer_32ch_init(&set, UINT32_MAX, 2, 6) == -1);
    assert(errno == EOVERFLOW && live == 0 && calls == 0);
    assert(set.channels == NULL && set.max_channel == 0);
    /* One channel-array allocation and two allocations per channel. */
    for (fail_at = 0; fail_at < 13; ++fail_at) {
        calls = 0;
        assert(ringbuffer_32ch_init(&set, sizeof(uint32_t), 8, 6) == -1);
        assert(live == 0);
        assert(set.channels == NULL && set.max_channel == 0 && set.buf_capacity == 0);
        expect_invalid(&set, 0, 32);
        ringbuffer_32ch_destory(&set, 32);
    }
    fail_at = -1;
    for (int cycle = 0; cycle < 100; ++cycle) {
        assert(ringbuffer_32ch_init(&set, sizeof(uint32_t), 10, 6) == 0);
        assert(set.buf_capacity == 8); /* preserve round-down behavior */
        expect_invalid(&set, 6, 32);
        expect_invalid(&set, 255, 255);
        expect_invalid(&set, 1, 1); /* caller limit still respected */
        for (uint8_t ch = 0; ch < 6; ++ch) {
            for (uint32_t n = 0; n < 8; ++n)
                assert(ringbuffer_32ch_put(&set, ch, &n, 32) == 1);
            assert(ringbuffer_32ch_avail(&set, ch, 32) == 0);
            for (uint32_t n = 0; n < 8; ++n) {
                uint32_t value;
                assert(ringbuffer_32ch_get(&set, ch, &value, 32) == 1);
                assert(value == n);
            }
            assert(ringbuffer_32ch_avail(&set, ch, 32) == 8);
        }
        ringbuffer_32ch_destory(&set, 32);
        ringbuffer_32ch_destory(&set, 32);
        expect_invalid(&set, 0, 32);
        assert(live == 0);
    }
    puts("ringbuffer bounds and allocation rollback regression passed");
    return 0;
}
