#define _DEFAULT_SOURCE
#define _XOPEN_SOURCE 700
#include <stdint.h>   
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdarg.h>

/* Directions in which the train is travelling (East/West) */
typedef enum {
    EAST = 0,
    WEST = 1
} direction_t; 

/* Train Object - (Parsed from the input and used by train thread + dispatcher) */
typedef struct {
    int id;           
    direction_t dir; 
    int high_priority; 
    int loading_time;    
    int crossing_time;   
    int64_t ready_time_ns; 
    pthread_t tid;      
    pthread_cond_t cv; 
    int my_turn;       
} train_t;

/*Linked List Node to build Ready Queues*/
typedef struct ready_node{
    int idx;
    int64_t ready_ns;
    struct ready_node *next;
} ready_node;

static struct timespec start; //Start Time 
static train_t *trains = NULL; //Pointer to train array (Dynamic Allocation)
static int n_trains = 0; //Number of trains in the array

static pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER; //For writing to output to prevent lines from getting mixed
static FILE *outf = NULL; //Output File 
static pthread_mutex_t scheduling_mutex = PTHREAD_MUTEX_INITIALIZER; //Shared Scheduling state (ready queues, track state, counters)
static pthread_cond_t ready_cv = PTHREAD_COND_INITIALIZER; //Signalled when train is ready to move or the track is free (Dispatcher waits for this)

/*Queues*/
static ready_node *east_high = NULL;
static ready_node *east_low = NULL;
static ready_node *west_high = NULL;
static ready_node *west_low = NULL;

/*Track Status*/
static int track_in_use = 0;
static int trains_finished = 0;
static int have_ever_crossed = 0;
static direction_t last_dir = EAST; //Arbitrary Value
static int same_dir_streak = 0;

/* Parsing & Loading Function Prototypes*/
static int    load_trains(const char *path);
static int    parse_line(const char *line, int id, train_t *t);

/* Train Thread and Dispatcher Function Protoypes*/
static void*  train_thread(void *arg);
static void*  dispatcher_main(void *arg);

/* Scheduler Helpers Function Prototypes*/
static int    any_ready(void);
static inline int     peek_idx(ready_node *h);
static inline int64_t peek_ns(ready_node *h);
static int    choose_from_pair(ready_node **A, ready_node **B);
static int    choose_next_idx_full(void);

/* Ready-Queue Utilities */
static int    ready_comes_before(int idxA, int64_t nsA, int idxB, int64_t nsB);
static void   queue_push(ready_node **head, int idx, int64_t ready_ns);
static int    queue_pop(ready_node **head);

/* Timing and Outputs Function Prototypes */
static int64_t nano_seconds_difference(const struct timespec *now, const struct timespec *then);
void format_elapsed(char *buf, size_t n);
void write_linef(const char *fmt, ...);
static const char* dir_text(direction_t d);

int main(int argc, char **argv){
    if(argc != 2){
        fprintf(stderr, "Usage: %s input.txt\n", argv[0]);
        return 1;
    }

    outf = fopen("output.txt","w");
    if(!outf){
        perror("output.txt");
        return 1;
    }

    if(load_trains(argv[1]) != 0){
        fprintf(stderr, "Failed to parse input file: %s\n", argv[1]);
        fclose(outf);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &start);

    pthread_t dispatcher_tid;
    if(pthread_create(&dispatcher_tid, NULL, dispatcher_main, NULL) != 0){
        perror("pthread_create(dispatcher)");
        fclose(outf);
        free(trains);
        return 1;
    }

    for(int i = 0; i < n_trains; i++){
        if(pthread_create(&trains[i].tid, NULL, train_thread, &trains[i]) != 0){
            perror("pthread_create(train)");
            //cleanup already-started trains
            for (int j = 0; j < i; j++) {
                pthread_join(trains[j].tid, NULL);
                pthread_cond_destroy(&trains[j].cv);
            }
            pthread_cancel(dispatcher_tid);
            pthread_join(dispatcher_tid, NULL);
            fclose(outf);
            free(trains);
            return 1;
        }
    }

    for (int i = 0; i < n_trains; i++){
        pthread_join(trains[i].tid, NULL);
        pthread_cond_destroy(&trains[i].cv);
    }

    pthread_join(dispatcher_tid, NULL);

    fclose(outf);
    free(trains);
    return 0;
}

/*
    Read the input file and build the trains[] array.
    Counts lines first to know how many trains to allocate, then re-reads and parses each line.
    On success sets n_trains and returns 0; on error returns -1
*/

static int load_trains(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) { 
        perror("fopen"); 
        return -1; 
    }

    int count = 0;
    char buf[256];

    //Count Number of Lines/Trains
    while (fgets(buf, sizeof(buf), f)){      
        char *p = buf;
        while (*p == ' ' || *p == '\t'){
            p++;
        }
        if (*p == '\0' || *p == '\n'){
            continue;   // skip blank lines 
        }
        count++;
    }
    
    //If the number of lines (count) is 0, then its an empty file*/
    if (count == 0) {                                  
        fclose(f);
        n_trains = 0;
        return 0;
    }

    //Dynammic Memory Allocation of the Trains (with the size as count)
    rewind(f);
    trains = (train_t*)calloc((size_t)count, sizeof(train_t));
    if (!trains) {
        fclose(f);
        fprintf(stderr, "Calloc failed in load_trains\n");
        return -1;
    }

    int id = 0;
    int ok = 1; //Flagging for failed parse
    while (id < count && fgets(buf, sizeof(buf), f)){
        if (parse_line(buf, id, &trains[id]) != 0){
            fprintf(stderr, "Parse error on line %d: %s", id + 1, buf);
            ok = 0;
            break;
        }
        id++;         
    }
    fclose(f);
    if (!ok) {
        free(trains);
        trains = NULL;
        n_trains = 0;
        return -1;
    }
    n_trains = id;
    return 0;
}

/*
    Parse one train description line (e.g. "E 3 4") into a train_t record.
    Validates direction and that loading/crossing times are in 1..99, then initializes
    the per-train fields (id, dir, priority, times) and its condition variable.
*/

static int parse_line(const char *line, int id, train_t *t) {
    char c; 
    int load, cross;
    if (sscanf(line, " %c %d %d", &c, &load, &cross) != 3){
        return -1;
    }
    if (load < 1 || load > 99 || cross < 1 || cross > 99){
        return -1;
    }

    int high = 0;
    switch (c) {
        case 'e': high = 0; t->dir = EAST; break;
        case 'E': high = 1; t->dir = EAST; break;
        case 'w': high = 0; t->dir = WEST; break;
        case 'W': high = 1; t->dir = WEST; break;
        default: return -1;
    }

    t->id = id;
    t->high_priority = high;
    t->loading_time = load;
    t->crossing_time = cross;
    t->ready_time_ns = -1;
    t->my_turn = 0;
    pthread_cond_init(&t->cv, NULL);
    return 0;
}

/* 
    Thread routine for a single train.
    Simulates loading, announces “ready”, enqueues itself in the correct
    priority/direction queue, then waits until the dispatcher chooses it.
    When dispatched, it logs ON/OFF around the crossing, updates global
    scheduling state, and wakes the dispatcher to pick the next train.
 */ 

static void* train_thread(void *arg){
    //Cast void* -> train_t* to use its fields
    train_t *t = (train_t*)arg;

    //Simulate Loading
    usleep(t->loading_time * 100000);

    //Stamp Ready time
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    t->ready_time_ns = nano_seconds_difference(&now, &start);

    //Log the Train Ready Line
    char timestamp[32];
    format_elapsed(timestamp, sizeof timestamp);
    write_linef("%s Train %2d is ready to go %4s\n", timestamp, t->id, dir_text(t->dir));

    //Enqueue, notify dispatcher and wait
    pthread_mutex_lock(&scheduling_mutex);
    ready_node **q = NULL;
    if(t->dir == EAST){
        q = (t->high_priority ? &east_high : &east_low);
    }
    else{
        q = (t->high_priority ? &west_high : &west_low);
    }
    queue_push(q, t->id, t->ready_time_ns);
    pthread_cond_signal(&ready_cv);
    while (!t->my_turn) {
        pthread_cond_wait(&t->cv, &scheduling_mutex);
    }
    //Enter track
    pthread_mutex_unlock(&scheduling_mutex);

    //ON -> Cross -> Off
    format_elapsed(timestamp, sizeof timestamp);
    write_linef("%s Train %2d is ON the main track going %4s\n", timestamp, t->id, dir_text(t->dir));

    usleep(t->crossing_time * 100000);

    format_elapsed(timestamp, sizeof timestamp);
    write_linef("%s Train %2d is OFF the main track after going %4s\n", timestamp, t->id, dir_text(t->dir));

    //Free track and wake dispatcher 
    pthread_mutex_lock(&scheduling_mutex);
    track_in_use = 0;
    pthread_cond_signal(&ready_cv);

    // update streak + counters WHILE holding the lock
    have_ever_crossed = 1;
    if (t->dir == last_dir){
        same_dir_streak++;
    }
    else { 
        last_dir = t->dir; same_dir_streak = 1; 
    }
    trains_finished++;
    pthread_cond_broadcast(&ready_cv);
    pthread_mutex_unlock(&scheduling_mutex);
    return NULL;
}

/* 
    Dispatcher thread.
    Waits until at least one train is ready and the track is free, then
    selects the next train according to the scheduling rules (priority,
    direction balancing, tie-breaking), marks the track as in use, and
    signals exactly that train’s condition variable. Runs until all trains
    have finished.
 */


static void* dispatcher_main(void *arg) {
    (void)arg;
    pthread_mutex_lock(&scheduling_mutex);
    while (trains_finished < n_trains) {
        while ((!any_ready() || track_in_use) && trains_finished < n_trains) {
            pthread_cond_wait(&ready_cv, &scheduling_mutex);
        }
        if (trains_finished >= n_trains){
            break;
        }

        int idx = choose_next_idx_full();
        if (idx >= 0) {
            track_in_use = 1;
            trains[idx].my_turn = 1;
            pthread_cond_signal(&trains[idx].cv);
        }
    }
    pthread_mutex_unlock(&scheduling_mutex);
    return NULL;
}

void format_elapsed(char *buf, size_t n){
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    
    int64_t ns = nano_seconds_difference(&now, &start);
    int64_t total_ms = ns / 1000000LL; //milliseconds

    long hours = (long)(total_ms / 3600000LL); //60 * 60 * 1000
    total_ms = total_ms % 3600000LL;

    long mins = (long)(total_ms / 60000LL); //60 * 1000
    total_ms = total_ms % 60000LL;

    long secs   = (long)(total_ms / 1000LL);
    long tenths = (long)((total_ms % 1000LL) / 100LL); // 0..9

    snprintf(buf, n, "%02ld:%02ld:%02ld.%1ld", hours, mins, secs, tenths);
}

static int64_t nano_seconds_difference(const struct timespec *now, const struct timespec *then){
     return (((int64_t)now->tv_sec  - (int64_t)then->tv_sec)  * 1000000000LL + ((int64_t)now->tv_nsec - (int64_t)then->tv_nsec));
}

void write_linef(const char *fmt, ...) {
    char buf[550];                    
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (n < 0){
        return;
    }                 
    size_t len = (size_t)((n < (int)sizeof(buf)) ? n : (int)sizeof(buf)-1);
    pthread_mutex_lock(&output_mutex);
    fwrite(buf, 1, len, outf);      
    fflush(outf);                   
    pthread_mutex_unlock(&output_mutex);
}

static const char* dir_text(direction_t d){ 
    if(d == EAST){
        return "East";
    }
    else{
        return "West";
    }
}

/*Push (idx, ready_ns) into a singly linked list, keeping it sorted. */
static void queue_push(ready_node **head, int idx, int64_t ready_ns) {
    ready_node *n = (ready_node*)malloc(sizeof(ready_node));
    n->idx = idx;
    n->ready_ns = ready_ns;
    n->next = NULL;

    ready_node *cur = *head, *prev = NULL;
    while (cur && !ready_comes_before(idx, ready_ns, cur->idx, cur->ready_ns)) {
        prev = cur;
        cur = cur->next;
    }
    if (prev == NULL) {  //insert at head
        n->next = *head;
        *head = n;
    } else {  // insert after prev
        n->next = prev->next;
        prev->next = n;
    }
}

/* Pop head, returns idx or -1 if empty. */
static int queue_pop(ready_node **head) {
    if (*head == NULL){
        return -1;
    }
    ready_node *n = *head;
    int idx = n->idx;
    *head = n->next;
    free(n);
    return idx;
}

/* Returns 1 if (idxA, nsA) should appear before (idxB, nsB) in the queue. */
static int ready_comes_before(int idxA, int64_t nsA, int idxB, int64_t nsB) {
    if (nsA < nsB){
        return 1;
    }
    if (nsA > nsB){
        return 0;
    }
    return idxA < idxB;
}

static int any_ready(void){
    return (east_high || east_low || west_high || west_low);
}

static int choose_from_pair(ready_node **A, ready_node **B) {
    int ai = peek_idx(*A);
    int bi = peek_idx(*B);
    if (ai >= 0 && bi >= 0) {
        if (ready_comes_before(ai, peek_ns(*A), bi, peek_ns(*B))){
            return queue_pop(A);
        }
        else{
            return queue_pop(B);
        }
    }
    else if (ai >= 0){
        return queue_pop(A);
    }
    else if (bi >= 0){
        return queue_pop(B);
    }
    return -1;
}

static int choose_next_idx_full(void) {
    // First train ever: prefer WEST if any ready
    if (!have_ever_crossed) {
        if (west_high || west_low) {
            if (west_high){
                return queue_pop(&west_high);
            }
            if (west_low){
                return queue_pop(&west_low);
            }
        }
        //else fall back to EAST normally below
    }

    //Direction balancing after two same-direction trains
    int want_opposite = (same_dir_streak >= 2);

    if (want_opposite) {
        if (last_dir == EAST) {
            if (west_high || west_low) {
                if (west_high){
                    return queue_pop(&west_high);
                }
                if (west_low){
                    return queue_pop(&west_low);
                }
            }
        } else {
            if (east_high || east_low) {
                if (east_high){
                    return queue_pop(&east_high);
                }
                if (east_low){
                    return queue_pop(&east_low);
                }
            }
        }
        //if opposite empty, fall through
    }

    // Normal priority + tie rules
    if (east_high || west_high){
        return choose_from_pair(&east_high, &west_high);
    }
    if (east_low  || west_low ){
        return choose_from_pair(&east_low,  &west_low );
    }
    return -1;
}

/* Queue Peak Helper Functions */
static inline int peek_idx(ready_node *h){
    return (h ? h->idx : -1);
}

static inline int64_t peek_ns(ready_node *h){
    return (h ? h->ready_ns : INT64_MAX);
}

