

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <errno.h>

#include <libzfs.h>
#include <spl/sys/time.h>
#include <sys/zio.h>
#include <libzfs_impl.h>



int main(int argc, char** argv) {
        
    int res = 0;
    char dataset[256];

    int fd = open("/dev/zfs", O_RDONLY);
    if (!fd) {
        printf("[zgen] ERROR opening /dev/zfs\n");
        return 1;
    }

    zfs_cmd_t zc = {"\0"};

    while (1) {

        zc.zc_obj = 0;
        res = scanf("%s %lu", dataset, &zc.zc_obj);
        if (!zc.zc_obj) {
            fprintf(stdout, "[zgen] ERROR while reading\n");
            // fprintf(stderr, "[zgen] ERROR while reading\n");
            return 2;
        }
        strncpy(zc.zc_name, dataset, sizeof (zc.zc_name));

        fprintf(stdout, "%s %lu ", dataset, zc.zc_obj);
        // fprintf(stderr, "%s %lu ", dataset, zc.zc_obj);

        res = ioctl(fd, ZFS_IOC_OBJ_TO_STATS, &zc);
        if (res < 0) {
            fprintf(stdout, "ERROR %d %s\n", errno, strerror(errno));
            // fprintf(stderr, "ERROR %d %s\n", errno, strerror(errno));
        } else {
            fprintf(stdout, "%lu\n", zc.zc_stat.zs_gen);
            // fprintf(stderr, "%lu\n", zc.zc_stat.zs_gen);
        }

        fflush(stdout);

    }

    return 0;

}

