#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <omm/rv_submgr.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>
#include <raimd/md_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>
#include <raimd/sass.h>
#include <raimd/app_a.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace omm;
using namespace md;

static const char *pref[] = {
  "_TIC.",
  "_TIB.",
  "_RVM.",
  "_RWF."
};
static const size_t FMT_PREF_LEN = 5;

enum {
  SASS_PREFIX    = 0,
  TIBMSG_PREFIX  = 1,
  RVMSG_PREFIX   = 2,
  RWFMSG_PREFIX  = 3
};
static inline int pref_match( const Subscription &s,
                              const char *&sub,  size_t &sublen,
                              uint32_t &h ) {
  const char * val = s.value;
  sub    = &val[ FMT_PREF_LEN ];
  sublen = s.len - FMT_PREF_LEN;
  h      = kv_crc_c( sub, sublen, 0 );
  switch ( val[ 3 ] ) {
    default:
    case 'C': return SASS_PREFIX;
    case 'B': return TIBMSG_PREFIX;
    case 'M': return RVMSG_PREFIX;
    case 'F': return RWFMSG_PREFIX;
  }
}

static const uint32_t fmt_type_id[] = {
  TIB_SASS_TYPE_ID,
  TIBMSG_TYPE_ID,
  RVMSG_TYPE_ID,
  RWF_MSG_TYPE_ID
};

RvOmmSubmgr::RvOmmSubmgr( kv::EvPoll &p,  sassrv::EvRvClient &c,
                          OmmDict &d ) noexcept
  : EvSocket( p, p.register_type( "omm_submgr" ) ),
    client( c ), sub_route( c.sub_route ), sub_db( c, this ), dict( d ),
    coll_ht( 0 ), sub( 0 ), sub_count( 0 ), is_subscribed( false )
{
  c.fwd_all_msgs = 0;
  this->sock_opts = OPT_NO_POLL;
  if ( omm_debug )
    this->sub_db.mout = &this->dbg_out;
}

struct FmtSub {
  char * value;
  size_t len;
  char   buf[ 256 ];

  FmtSub() { this->value = this->buf; }
  FmtSub( int fmt,  const char *s,  size_t l ) {
    this->value = this->buf;
    this->set( fmt, s, l );
  }
  FmtSub( int fmt,  const Subscription &sub ) {
    this->value = this->buf;
    this->set( fmt, sub );
  }
  ~FmtSub() {
    if ( this->value != this->buf )
      ::free( this->value );
  }
  void set( int fmt,  const Subscription &sub ) {
    const char * val = sub.value;
    this->set( fmt, &val[ FMT_PREF_LEN ], sub.len - FMT_PREF_LEN );
  }
  void set( int fmt,  const char *s,  size_t l ) {
    this->len = l + FMT_PREF_LEN;
    if ( this->len >= sizeof( this->buf ) ) {
      char * p = ( this->value == this->buf ? NULL : this->value );
      this->value = (char *) ::realloc( p, this->len + 1 );
    }
    ::memcpy( this->value, pref[ fmt ], FMT_PREF_LEN );
    ::memcpy( &this->value[ FMT_PREF_LEN ], s, l );
    this->value[ this->len ] = '\0';
  }
  uint32_t hash( uint32_t seed ) const {
    return kv_crc_c( this->value, this->len, seed );
  }
};

/* called after daemon responds with CONNECTED message */
void
RvOmmSubmgr::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  size_t count = ( this->sub_count == 0 ? 1 : this->sub_count );
  for ( size_t i = 0; i < count; i++ ) {
    if ( i < this->sub_count && ::strlen( this->sub[ i ] ) > 255-FMT_PREF_LEN )
      fprintf( stderr, "sub %s too long\n", this->sub[ i ] );
    else {
      for ( int j = 0; j < MAX_FMT_PREFIX; j++ ) {
        const char *s = ( i == this->sub_count ? ">" : this->sub[ i ] );
        FmtSub tmp( j, s, ::strlen( s ) );
        this->sub_db.add_wildcard( tmp.value );
      }
    }
  }
  this->start_subscriptions();
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvOmmSubmgr::start_subscriptions( void ) noexcept
{
  int sfd = this->poll.get_null_fd();
  this->PeerData::init_peer( this->poll.get_next_id(), sfd, -1, NULL,
                             "omm_submgr" );
  this->PeerData::set_name( "omm_submgr", 10 );
  this->poll.add_sock( this );
  this->sub_db.start_subscriptions( false );
  this->poll.timer.add_timer_seconds( this->fd, 3, 1, 0 );
}

void
RvOmmSubmgr::on_unsubscribe( void ) noexcept
{
  this->sub_db.stop_subscriptions();
}

bool
RvOmmSubmgr::on_rv_msg( kv::EvPublish &pub ) noexcept
{
  this->sub_db.process_pub( pub );
  return true;
}

static bool
is_rwf_solicited( EvPublish &pub ) noexcept
{
  if ( pub.msg_enc != RWF_MSG_TYPE_ID )
    return false;
  uint8_t msg_class = RwfMsgPeek::get_msg_class( pub.msg, pub.msg_len );
  if ( msg_class != REFRESH_MSG_CLASS )
    return false;
  uint16_t msg_flags = RwfMsgPeek::get_msg_flags( pub.msg, pub.msg_len );
  return ( msg_flags & RWF_REFRESH_SOLICITED ) != 0;
}

template<class Writer>
void append_hdr( Writer &w,  MDFormClass *form,  uint16_t msg_type,
                 uint16_t rec_type,  uint16_t seqno,  uint16_t status,
                 const char *subj,  size_t sublen )
{
  if ( msg_type != INITIAL_TYPE || form == NULL ) {
    w.append_uint( SASS_MSG_TYPE  , SASS_MSG_TYPE_LEN  , msg_type );
    if ( rec_type != 0 )
      w.append_uint( SASS_REC_TYPE, SASS_REC_TYPE_LEN  , rec_type );
    w.append_uint( SASS_SEQ_NO    , SASS_SEQ_NO_LEN    , seqno )
     .append_uint( SASS_REC_STATUS, SASS_REC_STATUS_LEN, status );
  }
  else {
    const MDFormEntry * e = form->entries;
    MDLookup by;
    if ( form->get( by.nm( SASS_MSG_TYPE, SASS_MSG_TYPE_LEN ) ) == &e[ 0 ] )
      w.append_uint( by.fname, by.fname_len, msg_type );
    if ( form->get( by.nm( SASS_REC_TYPE, SASS_REC_TYPE_LEN ) ) == &e[ 1 ] )
      w.append_uint( by.fname, by.fname_len, rec_type );
    if ( form->get( by.nm( SASS_SEQ_NO, SASS_SEQ_NO_LEN ) ) == &e[ 2 ] )
      w.append_uint( by.fname, by.fname_len, seqno );
    if ( form->get( by.nm( SASS_REC_STATUS, SASS_REC_STATUS_LEN ) ) == &e[ 3 ] )
      w.append_uint( by.fname, by.fname_len, status );
    if ( form->get( by.nm( SASS_SYMBOL, SASS_SYMBOL_LEN ) ) == &e[ 4 ] )
      w.append_string( by.fname, by.fname_len, subj, sublen );
  }
}

int
RvOmmSubmgr::convert_to_msg( EvPublish &pub,  uint32_t type_id,
                             FlistEntry &flist ) noexcept
{
  if ( this->dict.rdm_dict == NULL || pub.msg_enc != RWF_MSG_TYPE_ID )
    return Err::NO_DICTIONARY;

  if ( type_id == RWF_MSG_TYPE_ID )
    return 0;

  this->cvt_mem.reuse();
  RwfMsg * m = RwfMsg::unpack_message( (void *) pub.msg, 0, pub.msg_len,
                                       RWF_MSG_TYPE_ID, this->dict.rdm_dict,
                                       this->cvt_mem );
  if ( m == NULL )
    return Err::INVALID_MSG;

  int          status;
  uint64_t     seq_num  = ( m->msg.test( X_HAS_SEQ_NUM ) ? m->msg.seq_num : 0 );
  RwfMsg     * fields   = m->get_container_msg();
  const char * name     = NULL;
  size_t       name_len = 0;
  uint16_t     msg_type = UPDATE_TYPE;

  if ( m->msg.msg_class == REFRESH_MSG_CLASS ) {
    name     = m->msg.msg_key.name;
    name_len = m->msg.msg_key.name_len;
    msg_type = INITIAL_TYPE;
  }
  else {
    msg_type = rwf_to_sass_msg_type( *m );
  }
  if ( fields == NULL || fields->base.type_id != RWF_FIELD_LIST )
    return Err::BAD_SUB_MSG;

  if ( ( fields->fields.flags & RwfFieldListHdr::HAS_FIELD_LIST_INFO ) != 0 ) {
    if ( flist.flist != fields->fields.flist ) {
      flist.flist = fields->fields.flist;

      if ( flist.flist != 0 && this->dict.flist_dict != NULL &&
           this->dict.cfile_dict != NULL ) {
        MDLookup by( flist.flist );
        if ( this->dict.flist_dict->lookup( by ) ) {
          MDLookup fc( by.fname, by.fname_len );
          if ( this->dict.cfile_dict->get( fc ) && fc.ftype == MD_MESSAGE ) {
            flist.rec_type = fc.fid;
            if ( fc.map_num != 0 )
              flist.form = this->dict.cfile_dict->get_form_class( fc );
          }
        }
      }
    }
  }
  size_t sz = pub.msg_len + 1024;
  void * buf_ptr = this->cvt_mem.make( sz );

  if ( type_id == RVMSG_TYPE_ID ) {
    RvMsgWriter w( this->cvt_mem, buf_ptr, sz );
    append_hdr<RvMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                             seq_num, 0, name, name_len );
    if ( (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = RVMSG_TYPE_ID;
    return 0;
  }
  if ( type_id == TIBMSG_TYPE_ID ) {
    TibMsgWriter w( this->cvt_mem, buf_ptr, sz );
    append_hdr<TibMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                              seq_num, 0, name, name_len );
    if ( (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = TIBMSG_TYPE_ID;
    return 0;
  }
  if ( type_id == TIB_SASS_TYPE_ID ) {
    TibSassMsgWriter w( this->cvt_mem, this->dict.cfile_dict, buf_ptr, sz );
    append_hdr<TibSassMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                                  seq_num, 0, name, name_len );
    if ( (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = TIB_SASS_TYPE_ID;
    return 0;
  }
  return Err::NO_MSG_IMPL;
}

static const char   BCAST[]   = "bcast";
static const size_t BCAST_LEN = sizeof( BCAST ) - 1;

bool
RvOmmSubmgr::on_msg( EvPublish &pub ) noexcept
{
  RouteLoc     loc;
  FlistEntry * flist =
    this->flist_tab.upsert( pub.subj_hash, pub.subject, pub.subject_len, loc );
  if ( loc.is_new ) {
    flist->init();
  }
  uint32_t bcast_initial = 0;
  if ( is_rwf_solicited( pub ) ) {
    const char * reply;
    size_t       reply_len;
    RouteLoc     loc;
    size_t       pos;

    for ( int i = 0; i < MAX_FMT_PREFIX; i++ ) {
      ReplyEntry * entry =
        this->reply_tab[ i ].find( pub.subj_hash, pub.subject, pub.subject_len,
                                   loc );
      if ( entry == NULL )
        continue;

      EvPublish pub2( pub );
      bool      cvt    = false;
      int       status = 0;

      for ( bool b = entry->first_reply( pos, reply, reply_len ); b;
            b = entry->next_reply( pos, reply, reply_len ) ) {
        if ( reply_len != BCAST_LEN ||
             ::memcmp( reply, BCAST, BCAST_LEN ) != 0 ) {
          if ( ! cvt ) {
            cvt    = true;
            status = this->convert_to_msg( pub2, fmt_type_id[ i ], *flist );
          }
          if ( status == 0 ) {
            pub2.subject     = reply;
            pub2.subject_len = reply_len;
            pub2.subj_hash   = 0;
            printf( "on_initial %.*s\n", (int) pub.subject_len, pub.subject );
            this->client.on_msg( pub2 );
          }
        }
        else { /* assert start without inbox */
          bcast_initial |= 1 << i;
        }
      }
      this->reply_tab[ i ].remove( loc );

      if ( status != 0 )
        fprintf( stderr, "failed to convert msg %.*s to %s, status %d\n",
                 (int) pub.subject_len, pub.subject, pref[ i ], status );
    }
    if ( bcast_initial == 0 )
      return true;
  }
  uint32_t refs = bcast_initial,
           hash[ MAX_FMT_PREFIX ];
  FmtSub   sub[ MAX_FMT_PREFIX ];
  int      i;

  for ( i = 0; i < MAX_FMT_PREFIX; i++ ) {
    sub[ i ].set( i, pub.subject, pub.subject_len );
    hash[ i ] = sub[ i ].hash( 0 );
  }
  if ( bcast_initial == 0 ) {
    for ( uint8_t cnt = 0; cnt < pub.prefix_cnt; cnt++ ) {
      uint32_t h = pub.hash[ cnt ];
      if ( pub.subj_hash == h ) {
        for ( i = 0; i < MAX_FMT_PREFIX; i++ ) {
          Subscription * entry;
          entry = this->sub_db.sub_tab.find( hash[ i ], sub[ i ].value,
                                             sub[ i ].len );
          if ( entry != NULL && entry->refcnt != 0 )
            refs |= 1 << i;
        }
      }
      else {
        uint16_t pref_len = pub.prefix[ cnt ];
        WildEntry * rt = this->wild_tab.find( h, pub.subject, pref_len );
        if ( rt != NULL ) {
          for ( i = 0; i < MAX_FMT_PREFIX; i++ )
            if ( rt->refcnt[ i ] != 0 )
              refs |= 1 << i;
        }
      }
    }
  }
  for ( int i = 0; i < MAX_FMT_PREFIX; i++ ) {
    if ( ( refs & ( 1 << i ) ) != 0 ) {
      EvPublish pub2( pub );
      int status = this->convert_to_msg( pub2, fmt_type_id[ i ], *flist );
      if ( status != 0 ) {
        fprintf( stderr, "failed to convert msg %.*s to %s, status %d\n",
                 (int) pub.subject_len, pub.subject, pref[ i ], status );
      }
      else {
        pub2.subject     = sub[ i ].value;
        pub2.subject_len = sub[ i ].len;
        pub2.subj_hash   = hash[ i ];
        this->client.on_msg( pub2 );
      }
    }
  }
  return true;
}

/* timer expired, process rv sub_db events */
bool
RvOmmSubmgr::timer_expire( uint64_t ,  uint64_t ) noexcept
{
  this->sub_db.process_events();
  return true; /* return false to disable recurrent timer */
}
void RvOmmSubmgr::write( void ) noexcept {}
void RvOmmSubmgr::read( void ) noexcept {}
void RvOmmSubmgr::process( void ) noexcept {}
void RvOmmSubmgr::release( void ) noexcept {}
void RvOmmSubmgr::on_write_ready( void ) noexcept {}

void
RvOmmSubmgr::on_listen_start( StartListener &add ) noexcept
{
  if ( add.reply_len == 0 ) {
    printf( "%sstart %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  else {
    printf( "%sstart %.*s reply %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.reply_len, add.reply, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  const char * sub;
  size_t       sublen;
  uint32_t     h;
  int          fmt = pref_match( add.sub, sub, sublen, h );

  if ( ! SubscriptionDB::is_rv_wildcard( sub, sublen ) ) {
    ReplyTab & tab    = this->reply_tab[ fmt ];
    NotifySub  nsub( sub, sublen, NULL, 0, h, 0, 'V', *this );
    RouteLoc   loc;
    uint32_t   refcnt = this->sub_refcnt( fmt, add.sub );

    if ( add.reply_len > 0 || ! add.is_listen_start ) {
      ReplyEntry * entry = tab.upsert( h, sub, sublen + 1, loc );
      if ( loc.is_new ) {
        entry->sublen = sublen;
        entry->value[ sublen ] = '\0';
      }
      size_t  reply_len = ( add.is_listen_start ? add.reply_len : BCAST_LEN );
      const char *reply = ( add.is_listen_start ? add.reply : BCAST );
      size_t off = entry->len;
      entry = tab.resize( h, entry, off, off + reply_len + 1, loc );
      if ( entry != NULL ) {
        ::memcpy( &entry->value[ off ], reply, reply_len );
        entry->value[ off + reply_len ] = '\0';
      }
      nsub.notify_type = NOTIFY_IS_INITIAL;
    }
    else {
      /* in case where snap arrives before subscribe */
      if ( tab.find( h, sub, sublen + 1, loc ) != NULL )
        nsub.notify_type = NOTIFY_IS_INITIAL;
    }
    if ( refcnt == 1 ) {
      nsub.hash_collision = this->sub_route.is_sub_member( h, this->fd );
      if ( nsub.hash_collision )
        this->add_collision( h );
      this->sub_route.add_sub( nsub );
    }
    else {
      nsub.sub_count = refcnt;
      this->sub_route.notify_sub( nsub );
    }
  }
  else {
    PatternCvt cvt;
    RouteLoc   loc;
    uint32_t   hcnt, cnt;
  
    if ( cvt.convert_rv( sub, sublen ) != 0 ) {
      fprintf( stderr, "bad rv pattern %.*s\n", (int) sublen, sub );
      return;
    }
    h = kv_crc_c( sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );

    WildEntry *rt = this->wild_tab.upsert2( h, sub, cvt.prefixlen, loc, hcnt );
    if ( loc.is_new )
      rt->init();

    printf( "notify_pattern %.*s\n", (int) cvt.prefixlen, sub );
    NotifyPattern npat( cvt, sub, sublen, NULL, 0, h, hcnt > 0, 'V', *this );
    if ( add.sub.refcnt == 1 )
      rt->add( fmt );

    if ( (cnt = rt->count()) == 1 )
      this->sub_route.add_pat( npat );
    else {
      npat.sub_count = cnt;
      this->sub_route.notify_pat( npat );
    }
  }
}

void
RvOmmSubmgr::on_listen_stop( StopListener &rem ) noexcept
{
  printf( "%sstop %.*s refs %u from %.*s%s\n",
    rem.is_listen_stop ? "listen_" : "assert_",
    rem.sub.len, rem.sub.value, rem.sub.refcnt,
    rem.session.len, rem.session.value, rem.is_orphan ? " orphan" : "" );

  const char * sub;
  size_t       sublen;
  uint32_t     h;
  int          fmt = pref_match( rem.sub, sub, sublen, h );

  if ( ! SubscriptionDB::is_rv_wildcard( sub, sublen ) ) {
    ReplyTab & tab    = this->reply_tab[ fmt ];
    NotifySub  nsub( sub, sublen, NULL, 0, h, 0, 'V', *this );
    uint32_t   refcnt = this->sub_refcnt( fmt, rem.sub );

    if ( refcnt == 0 ) {
      tab.remove( h, sub, sublen );
      nsub.hash_collision = this->rem_collision( h );
      this->sub_route.del_sub( nsub );
      /* maybe don't remove in case of bcast feed or replay feed */
      if ( this->wild_tab.is_empty() )
        this->flist_tab.remove( h, sub, sublen );
    }
    else {
      nsub.sub_count = refcnt;
      this->sub_route.notify_unsub( nsub );
    }
  }
  else {
    PatternCvt cvt;
    RouteLoc   loc;
    uint32_t   hcnt, cnt;
  
    if ( cvt.convert_rv( sub, sublen ) != 0 ) {
      fprintf( stderr, "bad rv pattern %.*s\n", (int) sublen, sub );
      return;
    }
    h = kv_crc_c( sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );

    WildEntry *rt = this->wild_tab.find2( h, sub, cvt.prefixlen, loc, hcnt );
    if ( rt == NULL ) {
      fprintf( stderr, "rv pattern not found %.*s\n", (int) sublen, sub );
      return;
    }
    NotifyPattern npat( cvt, sub, sublen, NULL, 0, h, hcnt > 1, 'V', *this );
    if ( rem.sub.refcnt == 0 )
      rt->sub( fmt );
    if ( (cnt = rt->count()) == 0 ) {
      this->wild_tab.remove( loc );
      this->sub_route.del_pat( npat );
    }
    else {
      npat.sub_count = cnt;
      this->sub_route.notify_pat( npat );
    }
  }
}

void
RvOmmSubmgr::add_collision( uint32_t h ) noexcept
{
  size_t   pos;
  uint32_t val;
  if ( this->coll_ht == NULL )
    this->coll_ht = UIntHashTab::resize( NULL );
  if ( this->coll_ht->find( h, pos, val ) )
    this->coll_ht->set( h, pos, val + 1 );
  else
    this->coll_ht->set_rsz( this->coll_ht, h, pos, 1 );
}

bool
RvOmmSubmgr::rem_collision( uint32_t h ) noexcept
{
  size_t   pos;
  uint32_t val;
  if ( this->coll_ht != NULL && this->coll_ht->find( h, pos, val ) ) {
    if ( val == 1 )
      this->coll_ht->remove( pos );
    else {
      this->coll_ht->set( h, pos, val - 1 );
      return true;
    }
  }
  return false;
}

void
RvOmmSubmgr::on_snapshot( SnapListener &snp ) noexcept
{
  printf( "snap %.*s reply %.*s refs %u flags %u\n",
    snp.sub.len, snp.sub.value, snp.reply_len, snp.reply, snp.sub.refcnt,
    snp.flags );

  if ( snp.reply_len == 0 )
    return;

  const char * sub;
  size_t       sublen;
  uint32_t     h;
  int          fmt    = pref_match( snp.sub, sub, sublen, h );
  ReplyTab   & tab    = this->reply_tab[ fmt ];
  uint32_t     refcnt = this->sub_refcnt( fmt, snp.sub );
  RouteLoc     loc;
  ReplyEntry * entry  = tab.upsert( h, sub, sublen + 1, loc );

  if ( loc.is_new ) {
    entry->sublen = sublen;
    entry->value[ sublen ] = '\0';
  }
  size_t off = entry->len;
  entry = tab.resize( h, entry, off, off + snp.reply_len + 1, loc );
  if ( entry != NULL ) {
    ::memcpy( &entry->value[ off ], snp.reply, snp.reply_len );
    entry->value[ off + snp.reply_len ] = '\0';
  }
  /* could start a subscription here, need a timeout to unsub */
  if ( refcnt != 0 ) {
    NotifySub nsub( sub, sublen, NULL, 0, h, 0, 'V', *this );
    nsub.notify_type = NOTIFY_IS_INITIAL;
    nsub.sub_count = refcnt;
    this->sub_route.notify_sub( nsub );
  }
}

uint32_t
RvOmmSubmgr::sub_refcnt( int fmt,  Subscription &sub ) noexcept
{
  uint32_t       refcnt = sub.refcnt;
  FmtSub         next( ( fmt + 1 ) % MAX_FMT_PREFIX, sub );
  Subscription * entry;

  entry = this->sub_db.sub_tab.find( next.hash( 0 ), next.value, next.len );
  if ( entry != NULL )
    refcnt += entry->refcnt;
  for ( int i = fmt + 2; i < fmt + MAX_FMT_PREFIX; i++ ) {
    next.set( i % MAX_FMT_PREFIX, sub );
    entry = this->sub_db.sub_tab.find( next.hash( 0 ), next.value, next.len );
    if ( entry != NULL )
      refcnt += entry->refcnt;
  }
  return refcnt;
}

/* when client connection stops */
void
RvOmmSubmgr::on_shutdown( EvSocket &conn,  const char *err,
                          size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

