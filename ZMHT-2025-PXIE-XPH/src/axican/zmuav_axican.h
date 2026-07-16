#ifndef ZMUAV_AXICAN_H_
#define ZMUAV_AXICAN_H_


/************************** Constant Definitions *****************************/
#define ZMUAV_XCAN_MODE_CONFIG	    0x00000001 /**< Configuration mode */
#define ZMUAV_XCAN_MODE_NORMAL	    0x00000002 /**< Normal mode */
#define ZMUAV_XCAN_MODE_LOOPBACK	0x00000004 /**< Loop Back mode */
#define ZMUAV_XCAN_MODE_SLEEP		0x00000008 /**< Sleep mode */


struct axican_frame {
	unsigned int    can_id;
	unsigned int   	can_dlc;
	unsigned int   	data[2];
	long long		timestamp;
};


#define ZMUAV_AXICAN_IOCTL_MAGIC 'C'
#define ZMUAV_AXICAN_NUM_IOCTLS 4

#define ZMUAV_AXICAN_GET_KFIFO_COUNT  _IOR(ZMUAV_AXICAN_IOCTL_MAGIC, 0, unsigned int)
#define ZMUAV_AXICAN_SET_MODE         _IOW(ZMUAV_AXICAN_IOCTL_MAGIC, 1, unsigned int)
#define ZMUAV_AXICAN_SET_BAUD         _IOW(ZMUAV_AXICAN_IOCTL_MAGIC, 2, unsigned int)

#define ZMUAV_AXICAN_GET_KFIFO_DATA   _IOR(ZMUAV_AXICAN_IOCTL_MAGIC, 3, struct axican_frame)

#endif