#ifndef helpers_h
#define helpers_h 1

#include <zookeeper.h>
#include <pthread.h>

typedef struct _watch_data {
    int connection_state;
    int event_type;
    char *event_path;
    void *watch_context;
    struct _watch_data *next;
} watch_data;

typedef struct _completion_data {
    pthread_mutex_t mutex;
    void *data;
} completion_data;

completion_data* create_completion_data();
void destroy_completion_data(completion_data *data);
void wait_for_completion(completion_data *data);

watch_data *wait_for_watch();
void destroy_watch_data(watch_data *data);

// Cgo doesn't like to use function addresses as variables.
extern watcher_fn watch_handler;
extern void_completion_t handle_void_completion;

// The precise GC in Go 1.4+ doesn't like it when we cast arbitrary
// integers to unsafe.Pointer to pass to the void* context parameter.
// Below are helper functions that perform the cast in C so the Go GC
// doesn't try to interpret it as a pointer.

zhandle_t *zookeeper_init_int(const char *host, watcher_fn fn,
		int recv_timeout, const clientid_t *clientid, unsigned long context, int flags);
int zoo_wget_int(zhandle_t *zh, const char *path,
		watcher_fn watcher, unsigned long watcherCtx,
		char *buffer, int* buffer_len, struct Stat *stat);
int zoo_wget_children2_int(zhandle_t *zh, const char *path,
		watcher_fn watcher, unsigned long watcherCtx,
		struct String_vector *strings, struct Stat *stat);
int zoo_wexists_int(zhandle_t *zh, const char *path,
		watcher_fn watcher, unsigned long watcherCtx, struct Stat *stat);

#endif

// vim:ts=4:sw=4:et
