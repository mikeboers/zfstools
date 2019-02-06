

#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>


typedef enum { 
    B_FALSE, 
    B_TRUE 
} boolean_t;


#define ZFS_MAX_DATASET_NAME_LEN 256
#define MAXNAMELEN 256
#define MAXPATHLEN          4096


typedef struct zfs_share {
    uint64_t    z_exportdata;
    uint64_t    z_sharedata;
    uint64_t    z_sharetype;    /* 0 = share, 1 = unshare */
    uint64_t    z_sharemax;  /* max length of share string */
} zfs_share_t;

typedef enum dmu_objset_type {
    DMU_OST_NONE,
    DMU_OST_META,
    DMU_OST_ZFS,
    DMU_OST_ZVOL,
    DMU_OST_OTHER,          /* For testing only! */
    DMU_OST_ANY,            /* Be careful! */
    DMU_OST_NUMTYPES
} dmu_objset_type_t;

typedef struct dmu_objset_stats {
    uint64_t dds_num_clones; /* number of clones of this */
    uint64_t dds_creation_txg;
    uint64_t dds_guid;
    dmu_objset_type_t dds_type;
    uint8_t dds_is_snapshot;
    uint8_t dds_inconsistent;
    char dds_origin[ZFS_MAX_DATASET_NAME_LEN];
} dmu_objset_stats_t;

struct drr_begin {
    uint64_t drr_magic;
    uint64_t drr_versioninfo; /* was drr_version */
    uint64_t drr_creation_time;
    dmu_objset_type_t drr_type;
    uint32_t drr_flags;
    uint64_t drr_toguid;
    uint64_t drr_fromguid;
    char drr_toname[MAXNAMELEN];
};

typedef struct zinject_record {
    uint64_t    zi_objset;
    uint64_t    zi_object;
    uint64_t    zi_start;
    uint64_t    zi_end;
    uint64_t    zi_guid;
    uint32_t    zi_level;
    uint32_t    zi_error;
    uint64_t    zi_type;
    uint32_t    zi_freq;
    uint32_t    zi_failfast;
    char        zi_func[MAXNAMELEN];
    uint32_t    zi_iotype;
    int32_t     zi_duration;
    uint64_t    zi_timer;
    uint64_t    zi_nlanes;
    uint32_t    zi_cmd;
    uint32_t    zi_pad;
} zinject_record_t;

typedef struct zfs_stat {
    uint64_t    zs_gen;
    uint64_t    zs_mode;
    uint64_t    zs_links;
    uint64_t    zs_ctime[2];
} zfs_stat_t;

typedef struct zfs_cmd {
    char        zc_name[MAXPATHLEN];    /* name of pool or dataset */
    uint64_t    zc_nvlist_src;      /* really (char *) */
    uint64_t    zc_nvlist_src_size;
    uint64_t    zc_nvlist_dst;      /* really (char *) */
    uint64_t    zc_nvlist_dst_size;
    boolean_t   zc_nvlist_dst_filled;   /* put an nvlist in dst? */
    int     zc_pad2;

    /*
     * The following members are for legacy ioctls which haven't been
     * converted to the new method.
     */
    uint64_t    zc_history;     /* really (char *) */
    char        zc_value[MAXPATHLEN * 2];
    char        zc_string[MAXNAMELEN];
    uint64_t    zc_guid;
    uint64_t    zc_nvlist_conf;     /* really (char *) */
    uint64_t    zc_nvlist_conf_size;
    uint64_t    zc_cookie;
    uint64_t    zc_objset_type;
    uint64_t    zc_perm_action;
    uint64_t    zc_history_len;
    uint64_t    zc_history_offset;
    uint64_t    zc_obj;
    uint64_t    zc_iflags;      /* internal to zfs(7fs) */
    zfs_share_t zc_share;
    dmu_objset_stats_t zc_objset_stats;
    struct drr_begin zc_begin_record;
    zinject_record_t zc_inject_record;
    uint32_t    zc_defer_destroy;
    uint32_t    zc_flags;
    uint64_t    zc_action_handle;
    int     zc_cleanup_fd;
    uint8_t     zc_simple;
    uint8_t     zc_pad[3];      /* alignment */
    uint64_t    zc_sendobj;
    uint64_t    zc_fromobj;
    uint64_t    zc_createtxg;
    zfs_stat_t  zc_stat;
} zfs_cmd_t;



int main(int argc, char** argv) {
    
    return 0;
}
