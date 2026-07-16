#ifndef AXICAN_H_
#define AXICAN_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "zmuav_axican.h"

typedef enum {
    AXICAN_WRITE_FLAG = 0,
    AXICAN_READ_FLAG,
    AXICAN_ERROR_FLAG
} axican_flag_e;


struct axican_fd {
    int fd;
    int flags;
};

struct axican {
    int id;
    struct axican_fd f_rd;
    struct axican_fd f_wr;
    struct axican_frame rx_frame;
    struct axican_frame tx_frame;
    int count ;
    int w_count ;
};

int axican_poll(int fd, int flags);
int axican_set_baud(int fd, unsigned int baud);
int axican_set_mode(int fd, unsigned int mode);
int axican_get_frame_count(int fd);
int axican_read(int fd, int flags, unsigned char *buffer, unsigned int lenght);
int axican_poll2(int fd, int flags, int poll_timeout_ms);

#ifdef __cplusplus
}
#endif

#endif