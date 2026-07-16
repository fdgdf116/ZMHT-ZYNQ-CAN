#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <poll.h>
#include <sys/ioctl.h>

#include "axican.h"

//flags:0 write 1:read
int axican_poll(int fd, int flags)
{
    int pollrc;
    struct pollfd fds[1];

    if ((flags != 0) && (flags != 1)) {
        printf("axican poll flag:%d error \n", flags);
        return -1;
    }

    fds[0].fd = fd;
    if (flags) {
        fds[0].events = POLLIN | POLLRDNORM;
    } else {
        fds[0].events = POLLOUT;
    }
    
    while (1) {
        pollrc = poll(&fds[0],1,-1);
        if (pollrc > 0) {
            return 0;
        } else if (pollrc == 0) {
            printf("poll false positive pass \n");
            return -1;
        } else {
            printf("poll error \n");
            return -1;
        }
    }

    return -1;
}


int axican_poll2(int fd, int flags, int poll_timeout_ms)
{
    int pollrc;
    struct pollfd fds[1];

    if ((flags != 0) && (flags != 1)) {
        printf("axican poll flag:%d error \n", flags);
        return -1;
    }

    fds[0].fd = fd;
    if (flags) {
        fds[0].events = POLLIN | POLLRDNORM;
    } else {
        fds[0].events = POLLOUT;
    }
    
    while (1) {
        pollrc = poll(&fds[0],1,poll_timeout_ms);
        if (pollrc > 0) {
            return 0;

        } else {
            return -1;
        }
    }

    return -1;
}


int axican_set_baud(int fd, unsigned int baud)
{
    unsigned int set_baud;
    set_baud = baud;

    return ioctl(fd, ZMUAV_AXICAN_SET_BAUD, &set_baud);
}


int axican_set_mode(int fd, unsigned int mode)
{
    unsigned int set_mode;
    set_mode = mode;

    return ioctl(fd, ZMUAV_AXICAN_SET_MODE, &set_mode);
}

int axican_get_frame_count(int fd)
{
    int rc;
    unsigned int frame_count;

    rc = ioctl(fd, ZMUAV_AXICAN_GET_KFIFO_COUNT, &frame_count);
    if (rc) {
        return 0;
    }

    return frame_count;
}

int axican_read(int fd, int flags, unsigned char *buffer, unsigned int lenght)
{
    int ret;
    struct axican_frame *frame;

    if (flags) {
        return read(fd, buffer, lenght);
    } else {
        frame = (struct axican_frame *)buffer;
        return ioctl(fd, ZMUAV_AXICAN_GET_KFIFO_DATA, frame);
    }

    return -1;
}

