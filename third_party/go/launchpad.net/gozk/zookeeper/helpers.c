
#include <zookeeper.h>
#include <pthread.h>
#include <string.h>
#include "helpers.h"


static pthread_mutex_t watch_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  watch_available = PTHREAD_COND_INITIALIZER;

static watch_data *first_watch = NULL;

completion_data* create_completion_data() {
    completion_data *data = malloc(sizeof(completion_data));
    pthread_mutex_init(&data->mutex, NULL);
    pthread_mutex_lock(&data->mutex);
    return data;
}

void destroy_completion_data(completion_data *data) {
    pthread_mutex_destroy(&data->mutex);
    free(data);
}

void wait_for_completion(completion_data *data) {
    pthread_mutex_lock(&data->mutex);
}

void _handle_void_completion(int rc, const void *data_) {
    completion_data *data = (completion_data*)data_;
    data->data = (void*)(size_t)rc;
    pthread_mutex_unlock(&data->mutex);
}

void _watch_handler(zhandle_t *zh, int event_type, int connection_state, 
                    const char *event_path, void *watch_context)
{
    pthread_mutex_lock(&watch_mutex);
    {
        watch_data *data = malloc(sizeof(watch_data)); // XXX Check data.
        data->connection_state = connection_state;
        data->event_type = event_type;
        data->event_path = strdup(event_path); // XXX Check event_path.
        data->watch_context = watch_context;
        data->next = NULL;

        if (first_watch == NULL) {
            first_watch = data;
        } else {
            watch_data *last_watch = first_watch;
            while (last_watch->next != NULL) {
                last_watch = last_watch->next;
            }
            last_watch->next = data;
        }

        pthread_cond_signal(&watch_available);
    }
    pthread_mutex_unlock(&watch_mutex);
}

watch_data *wait_for_watch() {
    watch_data *data = NULL;

    pthread_mutex_lock(&watch_mutex);
    {
        while (first_watch == NULL) {
            pthread_cond_wait(&watch_available, &watch_mutex);
        }
        data = first_watch;
        first_watch = first_watch->next;
        data->next = NULL;  // Just in case.
    }
    pthread_mutex_unlock(&watch_mutex);

    return data;
}

void destroy_watch_data(watch_data *data) {
    free(data->event_path);
    free(data);
}


// Cgo doesn't like to use function addresses as variables.
watcher_fn watch_handler = _watch_handler;
void_completion_t handle_void_completion = _handle_void_completion;


// vim:ts=4:sw=4:et
