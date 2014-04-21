#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include <errno.h>
#include <wchar.h>
#include <sys/param.h>
#include "perl.h"
#define MAX_PACKET_LEN 512
#define MIN_SCORE 500
#define MAX_QUERY_TERMS 5
#define NOW(x) ((float)clock()/CLOCKS_PER_SEC)

#define FORMAT(fmt,arg...) fmt " [%s()]\n",##arg,__func__
#define D(fmt,arg...) fprintf(stderr,FORMAT(fmt,##arg))
#define sayx(fmt,arg...)                            \
do {                                                \
    die(FORMAT(fmt,##arg));                         \
} while(0)

#define saypx(fmt,arg...) sayx(fmt " { %s(%d) }",##arg,errno ? strerror(errno) : "undefined error",errno);

#define STARTUP_ALLOC 10
#define MIN3(a, b, c) ((a) < (b) ? ((a) < (c) ? (a) : (c)) : ((b) < (c) ? (b) : (c)))
#define RVAL(s,index) ((s)->runes[(index)].value.u32)
#define RBYTE(s,index) ((s)->runes[(index)].value.u8[0])
#define RPTR(s,index) (&(s)->runes[(index)])
#define RLEN(s,index) ((s)->runes[(index)].len)

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;

struct rune {
    u8 len;
    union value {
        u32 u32;
        u8 u8[4];
    } value;
};

typedef struct rune rune;

struct rstring {
    u32 rlen;
    u32 alen;
    u32 key;
    u16 local;
    rune *runes;
    struct rstring *next;
};
typedef struct rstring rstring;

struct ranked_term {
    rstring *s;
    u16 score;
};
typedef struct ranked_term ranked_term;

struct ranked_result {
    u16 score;
    ranked_term ranked_terms[MAX_QUERY_TERMS];
};

typedef struct ranked_result ranked_result;

struct query {
    rstring *s;
    ranked_result max;
    pthread_mutex_t lock;
    float start;
    int done;

    // end point
    struct sockaddr_in dest_sa;
    int dest_fd;
};

typedef struct query query;

struct task {
    query *query;
    rstring *shard;
    struct task *next;
};

struct task_queue {
    struct task *head;
    struct task *tail;
    int cap;
    int n_shards;
    int max_docs_per_shard;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    rstring **shards;
};

void *x_realloc(void *x, size_t n) {
    void *b = realloc(x,n);
    if (b == NULL)
        sayx("unable to allocate %zu bytes",n);

    return b;
}

void *x_malloc(size_t n) {
    return x_realloc(NULL,n);
}

static u32 HASH_P = 5381;
static u32 DELIM = ' ';
static u8 U_MASK[] = {192, 224, 240};
// http://zaemis.blogspot.nl/2011/06/reading-unicode-utf-8-by-hand-in-c.html
int rune_bread(char *buf, off_t *off, size_t blen, rune *dest) {
    dest->len = 0;
    dest->value.u32 = 0;

    if (blen - *off < 1)
        return 0;
    dest->value.u8[0] = buf[*off];
    dest->len = 1;

    if ((dest->value.u8[0] & U_MASK[0]) == U_MASK[0]) dest->len++;
    if ((dest->value.u8[0] & U_MASK[1]) == U_MASK[1]) dest->len++;
    if ((dest->value.u8[0] & U_MASK[2]) == U_MASK[2]) dest->len++;

    if (dest->len > 1) {
        if (blen - *off < dest->len) {
            memcpy(&dest->value.u8[1],&buf[*off],dest->len - 1);
        }
    }
    *off += dest->len;
    return dest->len;
}

void rstring_prepare(rstring *s) {
    if (s->alen == s->rlen) {
        s->alen += STARTUP_ALLOC;
        s->runes = x_realloc(s->runes,sizeof(*s->runes) * s->alen);
    }
}

rstring *rstring_new(void) {
    rstring *s = x_malloc(sizeof(*s));
    s->next = NULL;
    s->runes = NULL;
    s->alen = 0;
    s->rlen = 0;
    s->key = HASH_P;
    rstring_prepare(s);
    return s;
}

void rstring_rerune(rstring *dst, rstring *src) {
    free(dst->runes);
    dst->runes = src->runes;
    dst->alen = src->alen;
    dst->rlen = src->rlen;
    dst->key = src->key;
}

void rstring_free(rstring *s) {
    if (s->next)
        rstring_free(s->next);
    free(s->runes);
    free(s);
}

void rstring_dump(rstring *s,int follow) {
    rstring *p;
    int i;
    for (p = s; p ; p = p->next) {
        fprintf(stderr,"NEXT: %d: ",p->rlen);
        for(i = 0; i < p->rlen; i++) {
            fwprintf(stderr,L"%c",RVAL(p,i));
        }
        fprintf(stderr," [ %p(%p next: %p) ]\n",s,p,p->next);
        if (!follow)
            break;
    }
}

rstring * rstring_radd(rstring *s, rune *r) {
    s->rlen++;
    rstring_prepare(s);
    s->runes[s->rlen - 1] = *r;
    // build a hash key as we are building the string
    s->key = (s->key << 5) + HASH_P;
    return s;
}

int rstring_equal(rstring *a, rstring *b) {
    if (a->key != b->key || a->rlen != b->rlen)
        return 0;

    int i = 0;
    for (i = 0; i < a->rlen; i++) {
        if (RVAL(a,i) != RVAL(b,i))
            return 0;
    }
    return 1;
}

int rstring_cmp(rstring *a, rstring *b) {
    int i;
    for (i = 0; i < a->rlen; i++) {
        if (RVAL(a,i) != RVAL(b,i))
            return (RVAL(a,i) > RVAL(b,i)) ? 1 : -1;
    }
    return 0;
}

//http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C
int rstring_levenshtein(rstring *s1, rstring *s2) {
    unsigned int x, y, lastdiag, olddiag;
    unsigned int column[s1->rlen+1];
    for (y = 1; y <= s1->rlen; y++)
        column[y] = y;
    for (x = 1; x <= s2->rlen; x++) {
        column[0] = x;
        for (y = 1, lastdiag = x-1; y <= s1->rlen; y++) {
            olddiag = column[y];
            column[y] = MIN3(column[y] + 1, column[y-1] + 1, lastdiag + (RVAL(s1,y-1) == RVAL(s2,x-1) ? 0 : 1));
            lastdiag = olddiag;
        }
    }
    return(column[s1->rlen]);
}

rstring *rstring_tokenize_into_chain(char *buf, size_t blen,u32 delim) {
    off_t off = 0;
    rstring *s = rstring_new(), *tmp;
    rune r;
    while (rune_bread(buf,&off,blen,&r) > 0) {
        if (r.value.u32 != delim) {
            rstring_radd(s,&r);
        } else {
            if (s->rlen > 0) {
                tmp = rstring_new();
                tmp->next = s;
                s = tmp;
            }
        }
    }
    return s;
}

rstring *rstring_chain_reverse(rstring *s) {
    rstring *sn = NULL,*next;

    while (s) {
        next = s->next;
        s->next = sn;
        sn = s;
        s = next;
    }
    return sn;
}

int rstring_to_char(rstring *s, char *dest, int n) {
    int i;
    int off = 0;
    for(i = 0; i < s->rlen; i++) {
        if (off + s->runes[i].len > n)
            return off;
        memcpy(dest + off,&RVAL(s,i), RLEN(s,i));
        off += s->runes[i].len;
    }
    return off;
}

void rstring_into_sv(rstring *s, SV* dest) {
    int i;
    for(i = 0; i < s->rlen; i++) {
        sv_catpvn(dest,(char *) &RVAL(s,i),RLEN(s,i));
    }
}

void tq_wait_for_work(struct task_queue *tq) {
    pthread_mutex_lock(&tq->lock);
    // if there is still work, just return, otherwise wait on the condition
    // in case a task was added shortly after we thought we are done
    if (tq->head == NULL)
        pthread_cond_wait(&tq->cond,&tq->lock);
    pthread_mutex_unlock(&tq->lock);
}
#define Q_APPEND(head,tail,elem)                                \
    do {                                                        \
        if ((head) == NULL)                                     \
            (head) = (elem);                                    \
        else                                                    \
            (tail)->next = (elem);                              \
        (tail) = (elem);                                        \
    } while(0);
static int tq_enqueue(struct task_queue *tq,struct query *q) {
    if (tq->cap < 1)
        return -1;
    int j;
    struct task *head = NULL, *tail = NULL;

    for (j = 0; j < tq->n_shards; j++) {
        struct task *t = x_malloc(sizeof(*t));
        t->shard = tq->shards[j];
        t->query = q;
        t->next = NULL;
        Q_APPEND(head,tail,t);
    }

    pthread_mutex_lock(&tq->lock);
    tq->cap -= tq->n_shards;
    Q_APPEND(tq->head,tq->tail,head);
    pthread_cond_signal(&tq->cond);
    pthread_mutex_unlock(&tq->lock);
    return 0;
}

struct task *tq_dequeue(struct task_queue *tq) {
    struct task *t = NULL;
    pthread_mutex_lock(&tq->lock);
    if ((t = tq->head) != NULL) {
        tq->head = t->next;
        tq->cap++;
    }
    pthread_mutex_unlock(&tq->lock);
    return t;
}

static void query_destroy(query *q) {
    rstring_free(q->s);
    pthread_mutex_destroy(&q->lock);
    free(q);
}

void execute_query(struct task_queue *tq, char *buf,int blen,struct sockaddr_in sa,int fd) {
    struct query *q = x_malloc(sizeof(*q));
    q->s = rstring_chain_reverse(rstring_tokenize_into_chain(buf,blen,DELIM));
    pthread_mutex_init(&q->lock,NULL);
    q->dest_fd = fd;
    q->dest_sa = sa;
    q->start = NOW();
    q->done = tq->n_shards;
    q->max.score = 0;

    int rc = tq_enqueue(tq,q);
    if (rc != 0)
        query_destroy(q);
}

u16 jscore(rstring *a, rstring *b) {
    if (a->rlen == 0 || b->rlen == 0)
        return 0;

    if (rstring_equal(a,b))
        return 1000;

    u32 dist = rstring_levenshtein(a,b);
    u32 len = MAX(a->rlen,b->rlen);
    return (((len - dist) * 1000) / (len + dist));
}

void shard_search(rstring *terms, query *q, ranked_result *ranked) {
    int i;
    rstring *qs,*ts;
    ranked_result *r;

    u16 score = 0;
    int max = -1;
    for (qs = q->s, i = 0; qs != NULL && i < MAX_QUERY_TERMS; qs = qs->next, i++) {
        rune *last = NULL;
        for (ts = terms; ts; ts = ts->next) {
            if (RBYTE(ts,0) != RBYTE(qs,0))
                continue;
            if (last == NULL || last != ts->runes) {
                score = jscore(qs,ts);
                last = ts->runes;
            }
            if (score == 0)
                continue;

            r = &ranked[ts->local];
            if (r->score == 0) {
                memset(r,0,sizeof(*r));
            }
            if (r->ranked_terms[i].score < score) {
                r->score -= r->ranked_terms[i].score;
                r->ranked_terms[i].s = ts;
                r->ranked_terms[i].score = score;
                r->score += score;
            }
            if (max == -1 || ranked[max].score < score) {
                max = ts->local;
            }
        }
    }
    // if we are the last query, send the result
    pthread_mutex_lock(&q->lock);
    if (max != -1 && q->max.score < ranked[max].score)
        q->max = ranked[max];

    if (--q->done == 0) {
        pthread_mutex_unlock(&q->lock);
        D("took: %.5f",NOW() - q->start);
        char buf[MAX_PACKET_LEN];
        int siz = sizeof(buf) - 1;
        int off = 0;
        if (q->max.score > 0) {
            for (i = 0; i < MAX_QUERY_TERMS; i++) {
                if (q->max.ranked_terms[i].score > 0) {
                    off += rstring_to_char(q->max.ranked_terms[i].s,buf + off,siz - off);
                    if (off < siz) {
                        buf[off] = ' ';
                        off++;
                    }
                }
            }
        } else {
            buf[0] = 0;
            off = 2; // we send only off - 1, so it makes sense to send one 0 if there are no results
        }
        sendto(q->dest_fd,buf,MIN(off - 1,siz),0,(struct sockaddr *)&q->dest_sa,sizeof(q->dest_sa));
        query_destroy(q);
    } else {
        pthread_mutex_unlock(&q->lock);
    }
}

void *shard_worker(void *p) {
    struct task_queue *tq = (struct task_queue *) p;
    struct task *t;
    size_t ranked_size = sizeof(struct ranked_result) * tq->max_docs_per_shard;
    ranked_result *ranked = x_malloc(ranked_size);
    D("ping! allocated: %zu ranked_result buffer for %d max_docs_per_shard",ranked_size,tq->max_docs_per_shard);
    int i;
    for (;;) {
        while ((t = tq_dequeue(tq)) != NULL) {
            for (i = 0; i < tq->max_docs_per_shard; i++)
                ranked[i].score = 0;
            shard_search(t->shard,t->query,ranked);
            free(t);
        }
        tq_wait_for_work(tq);
    }
    pthread_exit(NULL);
}

void shard_spawn_workers(int n, struct task_queue *tq) {
    int i;
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0)
        saypx("attr init");
    if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0)
        saypx("setdetachstate");

    for (i = 0; i < n; i++) {
        pthread_t tid;
        if (pthread_create(&tid,&attr,shard_worker,tq) != 0)
            saypx("pthread: failed to create thread");
    }
    pthread_attr_destroy(&attr);
}

void ms2tv(struct timeval *result, unsigned long interval_ms) {
    result->tv_sec = (interval_ms / 1000);
    result->tv_usec = ((interval_ms % 1000) * 1000);
}

// http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.c
rstring *listsort(rstring *list) {
    rstring *p, *q, *e, *tail, *oldhead;
    int insize, nmerges, psize, qsize, i;
    if (!list)
	return NULL;

    insize = 1;

    while (1) {
        p = list;
        list = NULL;
        tail = NULL;

        nmerges = 0;  /* count number of merges we do in this pass */

        while (p) {
            nmerges++;  /* there exists a merge to be done */
            /* step `insize' places along from p */
            q = p;
            psize = 0;
            for (i = 0; i < insize; i++) {
                psize++;
                if (!(q = q->next))
                    break;
            }

            /* if q hasn't fallen off end, we have two lists to merge */
            qsize = insize;

            /* now we have two lists; merge them */
            while (psize > 0 || (qsize > 0 && q)) {
                /* decide whether next rstring of merge comes from p or q */
                if (psize == 0) {
		    /* p is empty; e must come from q. */
		    e = q; q = q->next; qsize--;
		} else if (qsize == 0 || !q) {
		    /* q is empty; e must come from p. */
		    e = p; p = p->next; psize--;
		} else if (rstring_cmp(p,q) <= 0) {
		    /* First rstring of p is lower (or same);
		     * e must come from p. */
		    e = p; p = p->next; psize--;
		} else {
		    /* First rstring of q is lower; e must come from q. */
		    e = q; q = q->next; qsize--;
		}

                /* add the next rstring to the merged list */
		if (tail) {
		    tail->next = e;
		} else {
		    list = e;
		}
		tail = e;
            }

            /* now p has stepped `insize' places along, and q has too */
            p = q;
        }
        tail->next = NULL;

        /* If we have done only one merge, we're finished. */
        if (nmerges <= 1)   /* allow for nmerges==0, the empty list case */
            return list;

        /* Otherwise repeat, merging lists twice the size */
        insize *= 2;
    }
}
