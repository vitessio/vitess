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

#endif

// vim:ts=4:sw=4:et
