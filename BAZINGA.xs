#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <strings.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include "shard.h"
#include "ppport.h"

MODULE = BAZINGA		PACKAGE = BAZINGA		

SV *
query(SV *server, unsigned short port, SV *typo, int timeout_ms)
    CODE:
    RETVAL = &PL_sv_undef;
    if (SvOK(server) && SvOK(typo)) {
        struct sockaddr_in servaddr;
        int sockfd = socket(AF_INET,SOCK_DGRAM,0);
        if (sockfd > 0) {
            struct timeval tv;
            ms2tv(&tv,timeout_ms);
            if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) == 0) {
                bzero(&servaddr,sizeof(servaddr));
                servaddr.sin_family      = AF_INET;
                servaddr.sin_addr.s_addr = inet_addr(SvPV_nolen(server));
                servaddr.sin_port        = htons(port);
                STRLEN len;
                char *buf = SvPV(typo,len);
                if (sendto(sockfd,buf,len,0,(struct sockaddr *)&servaddr,sizeof(servaddr)) == len) {
                    char buf[MAX_PACKET_LEN];
                    int n;
                    if ((n = recvfrom(sockfd,buf,sizeof(buf),0,NULL,NULL)) > 0) {
                        SV *ret = newSVpvn(buf, n);
                        SvUTF8_on(ret);
                        RETVAL = ret;
                    }
                } else {
                    die("failed to send %zu bytes",len);
                }
            }
            close(sockfd);
        }
    } else {
        RETVAL = &PL_sv_undef;
    }
    OUTPUT:
        RETVAL

SV *
index_and_serve(unsigned short port, unsigned short n_workers,int max_docs_per_shard, SV *rdocs, int background)
    CODE:
    if (!SvROK(rdocs) || SvTYPE(SvRV(rdocs)) != SVt_PVAV) {
        croak("expected array ref of documents");
    }
    AV *docs = (AV *) SvRV(rdocs);
    int len = av_len(docs),i;
    int n = 1 + (len / max_docs_per_shard);

    struct task_queue *tq = tq_new(n);
    tq->cap = 10000;
    tq->max_docs_per_shard = max_docs_per_shard;
    tq->n_workers = n_workers;
    int sockfd = socket(AF_INET,SOCK_DGRAM,0);
    if (sockfd <= 0)
        saypx("socket");
    int op = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &op, sizeof(int)) != 0 )
        saypx("setsockopt");

    tq->sockfd = sockfd;
    struct sockaddr_in servaddr;
    bzero(&servaddr,sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
    servaddr.sin_port=htons(port);
    if (bind(tq->sockfd,(struct sockaddr *)&servaddr,sizeof(servaddr)) != 0)
        saypx("bind");

    HV* dup = newHV();
    SV* bsv = newSVpvn("",0);
    // FIXME: this whole thing must be rewritten
    // at the moment it checks for uniqueness using the dup hash
    // and points strings to use same rune pointers
    // so we can quickly check if a term is equal to another term
    // just by checking its runes pointer
    for (i = 0; i <= len; i++) {
        SV **svp = av_fetch(docs,i,0);
        
        if (svp == NULL || !SvOK(*svp))
            continue;

        STRLEN blen;
        char *buf = SvPV(*svp,blen);
        int id = i % n;
        rstring *tokens = rstring_tokenize_into_chain(buf,blen,DELIM), *ts;
        rstring *tmp;
        for (ts = tokens; ts;) {
            // since we have some state in the rstring
            // we will copy it into SV, and check for duplication
            // so we can reuse the same rune *pointer
            rstring_into_sv(ts,bsv);
            STRLEN len;
            char *key = SvPV(bsv, len);
            SV **existing = hv_fetch(dup,key,len,0);
            if (existing != NULL && SvOK(*existing)) {
                rstring_rerune(ts,(rstring *) SvPV_nolen(*existing));
            } else {
                hv_store(dup,key,len,newSVpvn((char *)ts,sizeof(*ts)),0);
            }
            sv_setpvn(bsv,"", 0);

            ts->local = tq->shards[id].ndocs;
            tmp = ts->next;
            ts->next = tq->shards[id].terms[RPREFIX(ts)];
            tq->shards[id].terms[RPREFIX(ts)] = ts;
            ts = tmp;
        }
        tq->shards[id].ndocs++;
    }
    hv_undef(dup);

    int j;
    for (i = 0; i < n; i++) {
        for (j = 0; j < PREFIXES; j++) {
            tq->shards[i].terms[j] = listsort(tq->shards[i].terms[j]);
        }
    }
    tq_start(tq,background);

    RETVAL = newSViv(sockfd);
OUTPUT:
    RETVAL
