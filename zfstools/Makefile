
.PHONY : build
.DEFAULT : build

build : zgen

ZFS=/home/mikeboers/dev/zfs

% : %.c
	$(CC) -o $@ -I$(ZFS)/include -I$(ZFS)/include/spl -I/usr/include/libzfs -I/usr/include/libspl  $^

