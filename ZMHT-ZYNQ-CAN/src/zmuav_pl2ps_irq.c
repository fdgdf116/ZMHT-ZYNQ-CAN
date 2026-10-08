#include <linux/kernel.h>
#include <linux/wait.h>
#include <linux/spinlock_types.h>
#include <linux/device.h>
#include <linux/cdev.h>
#include <linux/init.h>
#include <linux/poll.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/io.h>
#include <linux/moduleparam.h>
#include <linux/interrupt.h>
#include <linux/param.h>
#include <linux/fs.h>
#include <linux/types.h>
#include <linux/uaccess.h>
#include <linux/jiffies.h>

#include <linux/kfifo.h>
#include <linux/of_address.h>
#include <linux/of_device.h>
#include <linux/of_platform.h>
#include <linux/delay.h>


#include "zmuav_pl2ps_irq.h"

#define DRIVER_NAME "zmuav_pl2ps_irq_2"
#define ZMUAV_PL2PS_IRQ_VERSION "V1.0"

struct zmuav_pl2ps_irq{
    int irq;

    atomic_t irq_count;
	atomic_t irq_occurred;
	wait_queue_head_t  irq_wait;

    struct device *dt_device;
    struct device *device;
    dev_t devt;
    struct cdev char_device;
};

static struct class *zmuav_pl2ps_irq_driver_class;

static unsigned int zmuav_pl2ps_irq_poll(struct file *f, poll_table *wait)
{
    unsigned int mask;
    struct zmuav_pl2ps_irq *lp = (struct zmuav_pl2ps_irq *)f->private_data;

    mask = 0;

	poll_wait(f, &lp->irq_wait, wait);
	if (atomic_read(&lp->irq_occurred)) {
		atomic_set(&lp->irq_occurred, 0);
		mask |= POLLIN | POLLRDNORM;
	}
    
    return mask;
}


static int zmuav_pl2ps_irq_open(struct inode *inod, struct file *f)
{
    struct zmuav_pl2ps_irq *lp = (struct zmuav_pl2ps_irq *)container_of(inod->i_cdev,
                    struct zmuav_pl2ps_irq, char_device);
    f->private_data = lp;
    
    return 0;
}


static int zmuav_pl2ps_irq_close(struct inode *inod, struct file *f)
{
    f->private_data = NULL;

    return 0;
}

static long zmuav_pl2ps_irq_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
	long rc;
    unsigned int count;
	void *__user arg_ptr;
	struct zmuav_pl2ps_irq *lp = (struct zmuav_pl2ps_irq *)f->private_data;

	arg_ptr = (void __user *)arg;

	if (_IOC_TYPE(cmd) != ZMUAV_PL2PS_IRQ_IOCTL_MAGIC) {
        dev_err(lp->dt_device, "IOCTL command magic number does not match.\n");
        return -ENOTTY;
    } else if (_IOC_NR(cmd) >= ZMUAV_PL2PS_IRQ_NUM_IOCTLS) {
        dev_err(lp->dt_device, "IOCTL command is out of range for this device.\n");
        return -ENOTTY;
    }

    switch (cmd) {
    case ZMUAV_PL2PS_IRQ_GET_IRQ_COUNT:
        count = (unsigned int)atomic_read(&lp->irq_count);
        if (copy_to_user(arg_ptr, &count, sizeof(count))) {
            dev_err(lp->dt_device, "unable to copy irq count to userspace\n");
            return -EFAULT;
        }

        rc = 0;
    break;

    case ZMUAV_PL2PS_IRQ_CLEAN_IRQ_COUNT:
        atomic_set(&lp->irq_count, 0);
        rc = 0;
    break;

    default:
        return -ENOTTY;
	}

    return rc;
}

static const struct file_operations fops = {
    .owner = THIS_MODULE,
    .open = zmuav_pl2ps_irq_open,
    .release = zmuav_pl2ps_irq_close,
    .unlocked_ioctl = zmuav_pl2ps_irq_ioctl,
    .poll = zmuav_pl2ps_irq_poll
};

static irqreturn_t zmuav_pl2ps_irq_irq(int irq, void *dw)
{
	struct zmuav_pl2ps_irq *lp = (struct zmuav_pl2ps_irq *)dw;

    atomic_inc(&lp->irq_count);
	atomic_set(&lp->irq_occurred, 1);
	wake_up_interruptible(&lp->irq_wait);
    return IRQ_HANDLED;
}

static int zmuav_pl2ps_irq_probe(struct platform_device *pdev)
{
    int irq;
    const char *device_name;
    struct device *dev = &pdev->dev;
    struct zmuav_pl2ps_irq *lp = NULL;
 
    int rc = 0;

    lp = devm_kmalloc(dev, sizeof(*lp), GFP_KERNEL);
    if (!lp)
        return -ENOMEM;

    dev_set_drvdata(dev, lp);
    lp->dt_device = dev;

    device_name = dev_name(lp->dt_device);

	atomic_set(&lp->irq_occurred, 0);
    atomic_set(&lp->irq_count, 0);
	init_waitqueue_head(&(lp->irq_wait));

    irq = platform_get_irq(pdev, 0);
    if (irq < 0) {
        if (irq != -EPROBE_DEFER)
            dev_err(lp->dt_device, "no IRQ found (error %i)\n", irq);
        rc = irq;
        goto err_initial;
    }

    lp->irq = irq;
	
    rc = request_irq(lp->irq, &zmuav_pl2ps_irq_irq, 0, DRIVER_NAME, lp);
    if (rc) {
        dev_err(lp->dt_device, "couldn't allocate interrupt %i\n",
            lp->irq);
        goto err_initial;
    }

    rc = alloc_chrdev_region(&lp->devt, 0, 1, DRIVER_NAME);
    if (rc < 0)
        goto err_irq;

    lp->device = device_create(zmuav_pl2ps_irq_driver_class, NULL, lp->devt,
                     NULL, device_name);
    if (IS_ERR(lp->device)) {
        dev_err(lp->dt_device,
            "couldn't create driver file\n");
        rc = PTR_ERR(lp->device);
        goto err_chrdev_region;
    }
    

    cdev_init(&lp->char_device, &fops);
    rc = cdev_add(&lp->char_device, lp->devt, 1);
    if (rc < 0) {
        dev_err(lp->dt_device, "couldn't create character device\n");
        goto err_dev;
    }

	dev_set_drvdata(lp->device, lp);
    dev_info(lp->dt_device, "irq=%i, major=%i, minor=%i version %s \n", lp->irq, MAJOR(lp->devt), MINOR(lp->devt), ZMUAV_PL2PS_IRQ_VERSION);

    return 0;

err_dev:
    device_destroy(zmuav_pl2ps_irq_driver_class, lp->devt);
err_chrdev_region:
    unregister_chrdev_region(lp->devt, 1);
err_irq:
    free_irq(lp->irq, lp);
err_initial:
    dev_set_drvdata(dev, NULL);
    return rc;
}

static int zmuav_pl2ps_irq_remove(struct platform_device *pdev)
{
    struct device *dev = &pdev->dev;
    struct zmuav_pl2ps_irq *lp = dev_get_drvdata(dev);

    cdev_del(&lp->char_device);
    dev_set_drvdata(lp->device, NULL);
    device_destroy(zmuav_pl2ps_irq_driver_class, lp->devt);
    unregister_chrdev_region(lp->devt, 1);
    free_irq(lp->irq, lp);
    dev_set_drvdata(dev, NULL);
    return 0;
}

static const struct of_device_id zmuav_pl2ps_irq_of_match[] = {
    { .compatible = "zmuav_pl2ps_irq_v1.0", },
    {},
};
MODULE_DEVICE_TABLE(of, zmuav_pl2ps_irq_of_match);

static struct platform_driver zmuav_pl2ps_irq_driver = {
    .driver = {
        .name = DRIVER_NAME,
        .owner = THIS_MODULE,
        .of_match_table = zmuav_pl2ps_irq_of_match,
    },
    .probe      = zmuav_pl2ps_irq_probe,
    .remove     = zmuav_pl2ps_irq_remove,
};

static int __init zmuav_pl2ps_irq_init(void)
{
    zmuav_pl2ps_irq_driver_class = class_create(THIS_MODULE, DRIVER_NAME);
    if (IS_ERR(zmuav_pl2ps_irq_driver_class))
        return PTR_ERR(zmuav_pl2ps_irq_driver_class);
    return platform_driver_register(&zmuav_pl2ps_irq_driver);
}

module_init(zmuav_pl2ps_irq_init);

static void __exit zmuav_pl2ps_irq_exit(void)
{
    platform_driver_unregister(&zmuav_pl2ps_irq_driver);
    class_destroy(zmuav_pl2ps_irq_driver_class);
}

module_exit(zmuav_pl2ps_irq_exit);


MODULE_LICENSE("GPL");
MODULE_AUTHOR("ZMVISION");
MODULE_DESCRIPTION("zmauv pl2ps irq driver");
