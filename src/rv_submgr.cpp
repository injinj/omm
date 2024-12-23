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

uint32_t
Insub::hash( void ) const noexcept
{
  return kv_crc_c( this->sub, this->sublen, 0 );
}

uint32_t
Outsub::hash( void ) const noexcept
{
  return kv_crc_c( this->value, this->len, 0 );
}

static const uint32_t fmt_type_id[] = {
  TIB_SASS_TYPE_ID,
  TIBMSG_TYPE_ID,
  RVMSG_TYPE_ID,
  RWF_MSG_TYPE_ID
};

RvOmmSubmgr::RvOmmSubmgr( kv::EvPoll &p,  sassrv::EvRvClient &c,
                          OmmDict &d,  OmmSourceDB &db,  uint32_t ft_weight,
                          const char *prefix,  const char *msg_fmt ) noexcept
  : EvSocket( p, p.register_type( "omm_submgr" ) ),
    client( c ), sub_route( c.sub_route ), sub_db( c, this ),
    ft( c, this ), dict( d ), source_db( db ), coll_ht( 0 ), active_ht( 0 ),
    fmt( 0 ), update_fmt( 0 ), pref_len( 0 ), ft_rank( 0 ), is_stopped( false ),
    is_running( false ), is_finished( false ), tid( 0 ), feed( 0 ),
    feed_count( 0 ), feed_map_service( 0 )
{
  if ( prefix != NULL ) {
    for ( this->pref_len = 0; this->pref_len < 15; ) {
      if ( *prefix == '\0' )
        break;
      this->pref[ this->pref_len++ ] = *prefix++;
    }
    if ( this->pref_len > 0 && this->pref[ this->pref_len - 1 ] != '.' )
      this->pref[ this->pref_len++ ] = '.';
    this->pref[ this->pref_len ] = '\0';
  }
  md_init_auto_unpack();
  if ( msg_fmt != NULL && ::strcasecmp( "TIB", msg_fmt ) == 0 ) {
    this->fmt = TIB_SASS_TYPE_ID;
    this->update_fmt = TIBMSG_TYPE_ID;
  }
  else if ( msg_fmt != NULL ) {
    uint32_t i;
    MDMatch * m;
    bool found = false;
    for ( m = MDMsg::first_match( i ); m; m = MDMsg::next_match( i ) ) {
      if ( ::strcasecmp( m->name, msg_fmt ) == 0 ) {
        this->fmt = m->hint[ 0 ];
        found = true;
        break;
      }
      if ( ! found )
        goto not_found;
    }
    switch ( this->fmt ) {
      case RVMSG_TYPE_ID:
      case RWF_MSG_TYPE_ID:
      case TIBMSG_TYPE_ID:
      case TIB_SASS_TYPE_ID:
        break;
      default:
      not_found:;
        fprintf( stderr, "format \"%s\" %s, types:\n", msg_fmt,
                 m == NULL ? "not found" : "converter not implemented" );
        for ( m = MDMsg::first_match( i ); m; m = MDMsg::next_match( i ) ) {
          if ( m->hint[ 0 ] != 0 )
            fprintf( stderr, "  %s : %x\n", m->name, m->hint[ 0 ] );
        }
        this->fmt = 0;
        break;
    }
    if ( this->fmt == 0 )
      this->fmt = TIB_SASS_TYPE_ID;
    this->update_fmt = this->fmt;
  }
  c.fwd_all_msgs = 0;

  this->sock_opts = OPT_NO_POLL;
  if ( omm_debug )
    this->sub_db.mout = &this->dbg_out;

  this->ft_param.weight = ft_weight;
  this->active_ht = ActiveHT::resize( NULL );
  db.add_source_listener( this );
}

void
RvOmmSubmgr::on_src_change( void ) noexcept
{
  OmmSourceDB &db = this->source_db;
  uint64_t     mapped = 0;
  size_t       i, j;
  if ( this->poll.quit )
    return;
  if ( this->feed_count != 0 ) {
    if ( this->feed_map_service == NULL ) {
      size_t sz = sizeof( this->feed_map_service[ 0 ] ) * this->feed_count;
      this->feed_map_service = (uint32_t *) ::malloc( sz );
      ::memset( this->feed_map_service, 0, sz );
    }
    for ( i = 0; i < this->feed_count; i++ ) {
      const char * feed     = this->feed[ i ];
      size_t       feed_len = ::strlen( feed );
      for ( j = 0; j < db.source_list.count; j++ ) {
        for ( OmmSource * src = db.source_list.ptr[ j ].hd; src != NULL;
              src = src->next ) {
          const char * svc     = src->info.service_name;
          size_t       svc_len = src->info.service_name_len;

          if ( feed_len == svc_len &&
               ::memcmp( feed, svc, feed_len ) == 0 ) {
            if ( this->feed_map_service[ i ] != src->service_id ) {
              printf( "Feed: " );
              if ( this->pref_len > 0 )
                printf( "%.*s", (int) this->pref_len, this->pref );
              printf( "%.*s.> using service %u\n",
                      (int) feed_len, feed, src->service_id );
              this->feed_map_service[ i ] = src->service_id;
            }
            mapped |= (uint64_t) 1 << i;
          }
        }
      }
    }
    for ( i = 0; i < this->feed_count; i++ ) {
      if ( ( mapped & ( (uint64_t) 1 << i ) ) == 0 ) {
        const char * feed     = this->feed[ i ];
        size_t       feed_len = ::strlen( feed );
        printf( "Feed: " );
        if ( this->pref_len > 0 )
          printf( "%.*s", (int) this->pref_len, this->pref );
        printf( "%.*s.> does not map to a service\n",
                (int) feed_len, feed );
        this->feed_map_service[ i ] = 0;
      }
    }
    if ( mapped != 0 ) {
      if ( ! this->is_running && ! this->is_finished )
        this->ft.activate();
    }
    else {
      if ( this->is_running )
        this->ft.deactivate();
    }
  }
}

void
RvOmmSubmgr::release( void ) noexcept
{
  this->sub_db.release();
  this->ft.release();
  this->active_ht->clear_all();
  this->reply_tab.release();
  if ( this->coll_ht != NULL ) {
    delete this->coll_ht;
    this->coll_ht = NULL;
  }
}

void
RvOmmSubmgr::on_ft_change( uint8_t action ) noexcept
{
  printf( "FT action: %s ", RvFt::action_str[ action ] );
  this->ft.ft_queue.print();
  printf( "\n" );

  if ( action == RvFt::ACTION_FINISH ) {
    this->is_finished = true;
    this->is_running  = false;
    this->release();
  }
  else if ( action == RvFt::ACTION_ACTIVATE ||
            action == RvFt::ACTION_DEACTIVATE ) {
    this->is_running = true;
  }
  else if ( action == RvFt::ACTION_LISTEN ) {
    this->is_running = false;
  }
  if ( this->is_running || this->ft_rank > 0 ) {
    uint32_t new_rank = this->ft.me.pos;
    if ( this->ft_rank != new_rank ) {
      this->ft_rank = new_rank;
      if ( new_rank == 1 )
        this->activate_subs();
      else
        this->deactivate_subs();
      if ( new_rank == 0 && this->ft.state_count.member_count() == 0 ) {
        printf( "No other FT members\n" );
      }
    }
    if ( new_rank == 1 && this->ft.ft_queue.count > 1 &&
         action == RvFt::ACTION_UPDATE ) {
      FtPeer * p = this->ft.ft_queue.ptr[ this->ft.ft_queue.count - 1 ];
      if ( p->state == RvFt::STATE_JOIN ) {
        this->poll.timer.add_timer_millis( this->fd, SYNC_JOIN_MS,
                                           this->tid + 1, p->start_ns );
      }
    }
  }
}

void
RvOmmSubmgr::on_ft_sync( EvPublish &pub ) noexcept
{
  MDMsgMem mem;
  RvMsg *m = RvMsg::unpack_rv( (void *) pub.msg, 0, pub.msg_len, 0, NULL, mem );
  if ( m != NULL )
    this->sub_db.update_sync( *m );
}

/* called after daemon responds with CONNECTED message */
void
RvOmmSubmgr::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "RvClient connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  size_t count = ( this->feed_count == 0 ? 1 : this->feed_count );
  for ( size_t i = 0; i < count; i++ ) {
    const char *s = ( i == this->feed_count ? ">" : this->feed[ i ] );
    Outsub tmp;
    tmp.set( this->pref, this->pref_len, s, ::strlen( s ), this->cvt_mem );
    this->sub_db.add_wildcard( tmp.value );
  }
  this->on_start();
}
/* start listening for subs, start ft */
void
RvOmmSubmgr::on_start( void ) noexcept
{
  bool is_restart;
  if ( ! this->in_list( IN_ACTIVE_LIST ) ) {
    int sfd = this->poll.get_null_fd();
    this->PeerData::init_peer( this->poll.get_next_id(), sfd, -1, NULL,
                               "omm_submgr" );
    this->PeerData::set_name( "omm_submgr", 10 );
    this->poll.add_sock( this );
    is_restart = false;
  }
  else {
    is_restart = true;
    this->ft_param.join_ms = 0;
  }
  if ( this->client.userid_len > 0 ) {
    this->ft_param.user = this->client.userid;
    this->ft_param.user_len = ::strlen( this->client.userid );
  }
  if ( this->feed_count != 0 ) { /* wait for feed to map to a src */
    this->ft_param.join_ms = 0;
    const char *s   = this->feed[ 0 ];
    size_t      len = ::strlen( s );
    if ( len > 2 && ::strcmp( &s[ len - 2 ], ".>" ) == 0 )
      len -= 2;
    char * ftsub = (char *) ::malloc( 4 + len + 1 );
    ::memcpy( ftsub, "_FT.", 4 );
    ::memcpy( &ftsub[ 4 ], s, len );
    ftsub[ 4 + len ] = '\0';
    this->ft_param.ft_sub = ftsub;
    this->ft_param.ft_sub_len = 4 + len; /* _FT.FEED */
  }
  printf( "Start FT subject=\"%.*s\"\n", (int) this->ft_param.ft_sub_len,
          this->ft_param.ft_sub );
  this->is_finished = false;
  this->is_stopped = false;
  this->is_running = false;
  this->ft_rank = 0;
  this->ft.start( this->ft_param );
  this->sub_db.start_subscriptions( false );
  this->poll.update_time_ns();
  this->tid = this->poll.now_ns;
  if ( is_restart )
    this->on_src_change();
  this->poll.timer.add_timer_seconds( this->fd, PROCESS_EVENTS_SECS,
                                      this->tid, PROCESS_EVENTS );
}

void
RvOmmSubmgr::on_stop( void ) noexcept
{
  if ( this->ft_rank == 1 )
    this->deactivate_subs();
  this->is_stopped = true;
  this->tid = 0;
  this->ft_rank = 0;
  this->sub_db.stop_subscriptions();
  if ( this->ft.stop() == 0 )
    this->is_finished = true;
}

bool
RvOmmSubmgr::on_rv_msg( kv::EvPublish &pub ) noexcept
{
  if ( ! this->ft.process_pub( pub ) )
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
                 uint16_t rec_type,  bool has_seq,  uint16_t seqno,
                 uint16_t status,  const char *subj,  size_t sublen ) noexcept
{
  if ( msg_type != INITIAL_TYPE || form == NULL ) {
    w.append_uint( SASS_MSG_TYPE  , SASS_MSG_TYPE_LEN  , msg_type );
    if ( rec_type != 0 )
      w.append_uint( SASS_REC_TYPE, SASS_REC_TYPE_LEN  , rec_type );
    if ( has_seq )
      w.append_uint( SASS_SEQ_NO  , SASS_SEQ_NO_LEN    , seqno );
    w.append_uint( SASS_REC_STATUS, SASS_REC_STATUS_LEN, status );
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

template<class Writer>
void append_status( Writer &w,  uint16_t msg_type,  uint16_t status ) noexcept
{
  w.append_uint( SASS_MSG_TYPE  , SASS_MSG_TYPE_LEN  , msg_type );
  w.append_uint( SASS_REC_STATUS, SASS_REC_STATUS_LEN, status );
}

void
RvOmmSubmgr::update_field_list( FlistEntry &flist,  uint16_t flist_no ) noexcept
{
  flist.flist = flist_no;
  if ( flist_no != 0 && this->dict.flist_dict != NULL &&
       this->dict.cfile_dict != NULL ) {
    MDLookup by( flist_no );
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

int
RvOmmSubmgr::convert_to_msg( EvPublish &pub,  uint32_t type_id,
                             FlistEntry &flist,  bool &flist_updated ) noexcept
{
  if ( this->dict.rdm_dict == NULL || pub.msg_enc != RWF_MSG_TYPE_ID )
    return Err::NO_DICTIONARY;

  if ( type_id == RWF_MSG_TYPE_ID )
    return 0;

  RwfMsg * m = RwfMsg::unpack_message( (void *) pub.msg, 0, pub.msg_len,
                                       RWF_MSG_TYPE_ID, this->dict.rdm_dict,
                                       this->cvt_mem );
  if ( m == NULL )
    return Err::INVALID_MSG;

  int          status;
  bool         has_seq    = m->msg.test( X_HAS_SEQ_NUM );
  uint64_t     seq_num    = ( has_seq ?  m->msg.seq_num : 0 );
  RwfMsg     * fields     = m->get_container_msg();
  const char * name       = NULL;
  size_t       name_len   = 0;
  uint16_t     msg_type   = UPDATE_TYPE,
               rec_status = OK_STATUS;

  if ( m->msg.msg_class == REFRESH_MSG_CLASS ) {
    name     = m->msg.msg_key.name;
    name_len = m->msg.msg_key.name_len;
    msg_type = INITIAL_TYPE;
    if ( fields != NULL ) {
      MDFieldReader rd( *fields );
      MDName nm;
      if ( rd.first( nm ) && nm.equals( SASS_MSG_TYPE, SASS_MSG_TYPE_LEN ) ) {
        rd.get_uint( msg_type );
        for ( int i = 0; i < 3 && rd.next( nm ); i++ ) {
          if ( nm.equals( SASS_REC_STATUS, SASS_REC_STATUS_LEN ) ) {
            char buf[ 32 ];
            size_t buflen = sizeof( buf );
            if ( rd.get_string( buf, sizeof( buf ), buflen ) ) {
              rec_status = sass_rec_status_val( buf, buflen );
              if ( rec_status == NOT_FOUND_STATUS && msg_type == VERIFY_TYPE ) {
                has_seq  = false;
                msg_type = TRANSIENT_TYPE;
              }
            }
            break;
          }
        }
      }
    }
  }
  else if ( m->msg.msg_class == STATUS_MSG_CLASS ) {
    if ( m->msg.state.stream_state != STREAM_STATE_OPEN )
      msg_type = DROP_TYPE;
    else
      msg_type = TRANSIENT_TYPE;
    rec_status = rwf_code_to_sass_rec_status( *m );
  }
  else {
    msg_type = rwf_to_sass_msg_type( *m );
  }
  if ( fields != NULL ) {
    if ( fields->base.type_id == RWF_FIELD_LIST &&
       ( fields->fields.flags & RwfFieldListHdr::HAS_FIELD_LIST_INFO ) != 0 ) {
      if ( flist.flist != fields->fields.flist ) {
        this->update_field_list( flist, fields->fields.flist );
        flist_updated = true;
      }
    }
  }
  size_t sz = pub.msg_len + 1024;
  void * buf_ptr = this->cvt_mem.make( sz );

  if ( type_id == RVMSG_TYPE_ID ) {
    RvMsgWriter w( this->cvt_mem, buf_ptr, sz );
    append_hdr<RvMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                             has_seq, seq_num, rec_status, name, name_len );
    if ( fields != NULL && (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = RVMSG_TYPE_ID;
    return w.err;
  }
  if ( type_id == TIBMSG_TYPE_ID ) {
    TibMsgWriter w( this->cvt_mem, buf_ptr, sz );
    append_hdr<TibMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                              has_seq, seq_num, rec_status, name, name_len );
    if ( fields != NULL && (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = TIBMSG_TYPE_ID;
    return w.err;
  }
  /*if ( type_id == TIB_SASS_TYPE_ID ) {*/
  if ( fields != NULL && fields->base.type_id != RWF_FIELD_LIST ) {
    fprintf( stderr, "unable to convert type %u to sass qform\n",
             fields->base.type_id );
    return Err::BAD_SUB_MSG;
  }
    TibSassMsgWriter w( this->cvt_mem, this->dict.cfile_dict, buf_ptr, sz );
    append_hdr<TibSassMsgWriter>( w, flist.form, msg_type, flist.rec_type,
                                  has_seq, seq_num, rec_status, name, name_len);
    if ( fields != NULL && (status = w.convert_msg( *fields, true )) != 0 )
      return status;
    pub.msg     = w.buf;
    pub.msg_len = w.update_hdr();
    pub.msg_enc = TIB_SASS_TYPE_ID;
    return w.err;
/*  }
  return Err::NO_MSG_IMPL;*/
}

static const char   BCAST[]   = "bcast";
static const size_t BCAST_LEN = sizeof( BCAST ) - 1;

bool
RvOmmSubmgr::on_msg( EvPublish &pub ) noexcept
{
  EvPublish  pub2( pub );
  FlistEntry flist;
  size_t     pos   = 0;
  uint32_t   hash,
             cvt_fmt;
  Outsub     sub;
  int        status = 0;
  bool       flist_updated = false,
             is_sub   = false,
             is_wild  = false,
             is_inbox = false,
             is_cvt   = false;

  this->cvt_mem.reuse();
  sub.set( this->pref, this->pref_len, pub.subject, pub.subject_len,
           this->cvt_mem );
  hash = sub.hash();
  RvSubscription *entry = this->sub_db.sub_tab.find( hash, sub.value, sub.len );
  if ( entry != NULL && entry->refcnt != 0 )
    is_sub = true;
  else {
    for ( uint8_t cnt = 0; ! is_wild && cnt < pub.prefix_cnt; cnt++ ) {
      uint32_t h = pub.hash[ cnt ];
      if ( pub.subj_hash != h ) {
        uint16_t pref_len = pub.prefix[ cnt ];
        WildEntry * rt = this->wild_tab.find( h, pub.subject, pref_len );
        if ( rt != NULL && rt->refcnt != 0 )
          is_wild = true;
      }
    }
  }
  if ( entry != NULL )
    this->active_ht->find( entry->subject_id, pos, flist );

  if ( is_rwf_solicited( pub ) ) {
    const char * reply;
    size_t       reply_len;
    RouteLoc     loc;
    size_t       pos;

    cvt_fmt = this->fmt;
    InboxReplyEntry * rentry =
      this->reply_tab.find( pub.subj_hash, pub.subject, pub.subject_len, loc );
    if ( rentry == NULL || flist.bcast ) {
      if ( flist.bcast ) {
        flist.bcast   = 0;
        flist_updated = true;
      }
    }
    else if ( rentry != NULL ) {
      for ( bool b = rentry->first_reply( pos, reply, reply_len ); b;
            b = rentry->next_reply( pos, reply, reply_len ) ) {
        is_inbox = true;
        if ( ! is_cvt ) {
          is_cvt = true;
          status = this->convert_to_msg( pub2, cvt_fmt, flist, flist_updated );
        }
        if ( status == 0 ) {
          pub2.subject     = reply;
          pub2.subject_len = reply_len;
          pub2.subj_hash   = 0;
          this->client.on_msg( pub2 );
        }
      }
    }
    if ( rentry != NULL )
      this->reply_tab.remove( loc );
  }
  else {
    cvt_fmt = this->update_fmt;
  }
  if ( ! is_inbox && ( is_sub || is_wild ) ) {
    if ( ! is_cvt ) {
      is_cvt = true;
      status = this->convert_to_msg( pub2, cvt_fmt, flist, flist_updated );
    }
    if ( status == 0 ) {
      pub2.subject     = sub.value;
      pub2.subject_len = sub.len;
      pub2.subj_hash   = hash;
      this->client.on_msg( pub2 );
    }
  }
  if ( entry != NULL && flist_updated )
    this->active_ht->set_rsz( this->active_ht, entry->subject_id, pos, flist );
  if ( status != 0 ) {
    fprintf( stderr, "failed to convert msg %.*s to %s, status %d\n",
            (int) pub.subject_len, pub.subject, this->pref, status );
  }
  return true;
}

/* timer expired, process rv sub_db events */
bool
RvOmmSubmgr::timer_expire( uint64_t id,  uint64_t ev ) noexcept
{
  if ( id == this->tid ) {
    if ( ev == PROCESS_EVENTS ) {
      this->sub_db.process_events();
      return true;
    }
  }
  else {
    uint64_t h = id - ( this->tid + 1 );
    if ( h > (uint64_t) 0xffffffffU )
      return false;

    MDMsgMem    mem;
    RvMsgWriter w( mem, mem.make( 1024 ), 1024 );
    if ( ! this->sub_db.make_host_sync( w, h ) )
      return false;
    w.update_hdr();

    FtPeer * p = NULL;
    for ( size_t i = this->ft.ft_queue.count - 1; ; i -= 1 ) {
      p = this->ft.ft_queue.ptr[ i ];
      if ( p->start_ns == ev )
        break;
      if ( i == 0 )
        return false;
    }
    EvPublish pub( p->sync_inbox, ::strlen( p->sync_inbox ),
                   NULL, 0, w.buf, w.off, this->client.sub_route,
                   this->client, 0, RVMSG_TYPE_ID );
    this->client.publish( pub );
    this->client.idle_push_write();
    this->poll.timer.add_timer_millis( this->fd, SYNC_JOIN_MS,
                                       id+1, p->start_ns );
    return false;
  }
  return false;
}
void RvOmmSubmgr::write( void ) noexcept {}
void RvOmmSubmgr::read( void ) noexcept {}
void RvOmmSubmgr::process( void ) noexcept {}
void RvOmmSubmgr::on_write_ready( void ) noexcept {}

void
RvOmmSubmgr::on_listen_start( Start &add ) noexcept
{
  if ( add.reply_len == 0 ) {
    printf( "[%u] %sstart %.*s refs %u from %.*s\n",
      this->ft_rank, add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  else {
    printf( "[%u] %sstart %.*s reply %.*s refs %u from %.*s\n",
      this->ft_rank, add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.reply_len, add.reply, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  if ( this->ft_rank != 1 )
    return;

  this->start_sub( add.sub, ! add.is_listen_start, false,
                   add.reply, add.reply_len );
}

void
RvOmmSubmgr::activate_subs( void ) noexcept
{
  RouteLoc loc;
  for ( RvSubscription *sub = this->sub_db.sub_tab.first( loc ); sub != NULL;
        sub = this->sub_db.sub_tab.next( loc ) ) {
    if ( sub->refcnt != 0 )
      this->start_sub( *sub, true, true, NULL, 0 );
  }
}

void
RvOmmSubmgr::start_sub( RvSubscription &sub,  bool is_bcast_reply,
                        bool is_ft_activate,  const void *reply,
                        size_t reply_len ) noexcept
{
  Insub m( sub, this->pref_len );

  if ( ! is_rv_wildcard( m.sub, m.sublen ) ) {
    uint32_t  h      = m.hash(),
              refcnt = sub.refcnt;
    NotifySub nsub( m.sub, m.sublen, NULL, 0, h, 0, 'V', *this );
    RouteLoc  loc;

    if ( is_bcast_reply ) {
      nsub.notify_type = NOTIFY_IS_INITIAL;
    }
    else if ( reply_len > 0 ) {
      size_t off, len;

      InboxReplyEntry * entry =
        this->reply_tab.upsert( h, m.sub, m.sublen, loc );
      if ( loc.is_new ) {
        entry->sublen = m.sublen;
        off = m.sublen;
        len = off + 1;
      }
      else {
        off = entry->len;
        len = off;
      }
      len  += reply_len + 1;
      entry = this->reply_tab.resize( h, entry, off, len, loc );
      if ( entry != NULL ) {
        if ( loc.is_new )
          entry->value[ off++ ] = '\0';
        ::memcpy( &entry->value[ off ], reply, reply_len );
        entry->value[ off + reply_len ] = '\0';
      }
      nsub.notify_type = NOTIFY_IS_INITIAL;
    }
    else {
      /* in case where snap arrives before subscribe */
      if ( this->reply_tab.find( h, m.sub, m.sublen, loc ) != NULL )
        nsub.notify_type = NOTIFY_IS_INITIAL;
    }

    FlistEntry flist( sub.hash );
    size_t pos;
    if ( ! this->active_ht->find( sub.subject_id, pos, flist ) ) {
      if ( is_bcast_reply )
        flist.bcast = 1;
      this->active_ht->set_rsz( this->active_ht, sub.subject_id, pos,
                                flist );

      nsub.hash_collision = this->sub_route.is_sub_member( h, this->fd );
      if ( nsub.hash_collision )
        this->add_collision( h );
      this->sub_route.add_sub( nsub );
    }
    else {
      if ( is_bcast_reply && ! flist.bcast ) {
        flist.bcast = 1;
        this->active_ht->set( sub.subject_id, pos, flist );
      }
      nsub.sub_count = refcnt;
      this->sub_route.notify_sub( nsub );
    }
  }
  else {
    PatternCvt cvt;
    RouteLoc   loc;
    uint32_t   hcnt, h;
  
    if ( cvt.convert_rv( m.sub, m.sublen ) != 0 ) {
      fprintf( stderr, "bad rv pattern %.*s\n", (int) m.sublen, m.sub );
      return;
    }
    h = kv_crc_c( m.sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );

    WildEntry *rt = this->wild_tab.upsert2( h, m.sub, cvt.prefixlen, loc, hcnt);
    if ( loc.is_new )
      rt->init();

    printf( "notify_pattern %.*s\n", (int) cvt.prefixlen, m.sub );
    NotifyPattern npat( cvt, m.sub, m.sublen, NULL, 0, h, hcnt>0, 'V', *this );
    if ( is_ft_activate || sub.refcnt == 1 )
      rt->refcnt++;

    if ( rt->refcnt == 1 )
      this->sub_route.add_pat( npat );
    else {
      npat.sub_count = rt->refcnt;
      this->sub_route.notify_pat( npat );
    }
  }
}

void
RvOmmSubmgr::on_listen_stop( Stop &rem ) noexcept
{
  printf( "[%u] %sstop %.*s refs %u from %.*s%s\n",
    this->ft_rank, rem.is_listen_stop ? "listen_" : "assert_",
    rem.sub.len, rem.sub.value, rem.sub.refcnt,
    rem.session.len, rem.session.value, rem.is_orphan ? " orphan" : "" );

  if ( this->ft_rank != 1 )
    return;
  this->stop_sub( rem.sub, false );
}

void
RvOmmSubmgr::deactivate_subs( void ) noexcept
{
  RouteLoc loc;
  if ( this->ft_rank == 0 && this->ft.state_count.member_count() == 0 )
    this->feed_down_subs();
  for ( RvSubscription *sub = this->sub_db.sub_tab.first( loc ); sub != NULL;
        sub = this->sub_db.sub_tab.next( loc ) ) {
    if ( sub->refcnt == 0 )
      continue;
    this->stop_sub( *sub, true );
  }
  /*this->active_ht->clear_all();
  this->reply_tab.release();*/
}

void
RvOmmSubmgr::feed_down_subs( void ) noexcept
{
  RouteLoc loc;
  uint32_t type_id = this->update_fmt;
  for ( RvSubscription *sub = this->sub_db.sub_tab.first( loc ); sub != NULL;
        sub = this->sub_db.sub_tab.next( loc ) ) {
    if ( sub->refcnt == 0 )
      continue;
    if ( is_rv_wildcard( sub->value, sub->len ) )
      continue;

    this->cvt_mem.reuse();
    size_t sz = 64;
    void * buf_ptr = this->cvt_mem.make( sz );
    EvPublish pub( sub->value, sub->len, NULL, 0, NULL, 0,
                   this->client.sub_route, this->client, 0, type_id );

    if ( type_id == RVMSG_TYPE_ID ) {
      RvMsgWriter w( this->cvt_mem, buf_ptr, sz );
      append_status<RvMsgWriter>( w, TRANSIENT_TYPE, FEED_DOWN_STATUS );
      pub.msg     = w.buf;
      pub.msg_len = w.update_hdr();
    }
    else if ( type_id == TIBMSG_TYPE_ID ) {
      TibMsgWriter w( this->cvt_mem, buf_ptr, sz );
      append_status<TibMsgWriter>( w, TRANSIENT_TYPE, FEED_DOWN_STATUS );
      pub.msg     = w.buf;
      pub.msg_len = w.update_hdr();
    }
    else {
      TibSassMsgWriter w( this->cvt_mem, this->dict.cfile_dict, buf_ptr, sz );
      append_status<TibSassMsgWriter>( w, TRANSIENT_TYPE, FEED_DOWN_STATUS );
      pub.msg     = w.buf;
      pub.msg_len = w.update_hdr();
    }
    this->client.publish( pub );
  }
}

void
RvOmmSubmgr::stop_sub( sassrv::RvSubscription &sub,  
                       bool is_ft_deactivate ) noexcept
{
  Insub m( sub, this->pref_len );

  if ( ! is_rv_wildcard( m.sub, m.sublen ) ) {
    uint32_t  h      = m.hash(),
              refcnt = sub.refcnt;
    NotifySub nsub( m.sub, m.sublen, NULL, 0, h, 0, 'V', *this );

    if ( refcnt == 0 || is_ft_deactivate ) {
      this->reply_tab.remove( h, m.sub, m.sublen );
      nsub.hash_collision = this->rem_collision( h );
      this->sub_route.del_sub( nsub );
      this->active_ht->find_remove_rsz( this->active_ht, sub.subject_id );
    }
    else {
      nsub.sub_count = refcnt;
      this->sub_route.notify_unsub( nsub );
    }
  }
  else {
    PatternCvt cvt;
    RouteLoc   loc;
    uint32_t   hcnt, h;
  
    if ( cvt.convert_rv( m.sub, m.sublen ) != 0 ) {
      fprintf( stderr, "bad rv pattern %.*s\n", (int) m.sublen, m.sub );
      return;
    }
    h = kv_crc_c( m.sub, cvt.prefixlen,
                  this->sub_route.prefix_seed( cvt.prefixlen ) );

    WildEntry *rt = this->wild_tab.find2( h, m.sub, cvt.prefixlen, loc, hcnt );
    if ( rt == NULL ) {
      fprintf( stderr, "rv pattern not found %.*s\n", (int) m.sublen, m.sub );
      return;
    }
    NotifyPattern npat( cvt, m.sub, m.sublen, NULL, 0, h, hcnt>1, 'V', *this );
    if ( sub.refcnt == 0 || is_ft_deactivate )
      rt->refcnt--;
    if ( rt->refcnt == 0 ) {
      this->wild_tab.remove( loc );
      this->sub_route.del_pat( npat );
    }
    else {
      npat.sub_count = rt->refcnt;
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
RvOmmSubmgr::on_snapshot( Snap &snp ) noexcept
{
  printf( "[%u] snap %.*s reply %.*s refs %u flags %u\n",
    this->ft_rank, snp.sub.len, snp.sub.value, snp.reply_len, snp.reply,
    snp.sub.refcnt, snp.flags );

  if ( this->ft_rank != 1 )
    return;
  if ( snp.reply_len == 0 )
    return;

  Insub     m( snp.sub, this->pref_len );
  uint32_t  h      = m.hash(),
            refcnt = snp.sub.refcnt;
  RouteLoc  loc;

  InboxReplyEntry * entry = this->reply_tab.upsert( h, m.sub, m.sublen, loc );
  const void      * reply     = snp.reply;
  size_t            reply_len = snp.reply_len,
                    off, len;
  if ( loc.is_new ) {
    entry->sublen = m.sublen;
    off = m.sublen;
    len = off + 1;
  }
  else {
    off = entry->len;
    len = off;
  }
  len  += reply_len + 1;
  entry = this->reply_tab.resize( h, entry, off, len, loc );

  if ( entry != NULL ) {
    if ( loc.is_new )
      entry->value[ off++ ] = '\0';
    ::memcpy( &entry->value[ off ], reply, reply_len );
    entry->value[ off + reply_len ] = '\0';
  }
  /* could start a subscription here, need a timeout to unsub */
  if ( refcnt != 0 ) {
    NotifySub nsub( m.sub, m.sublen, NULL, 0, h, 0, 'V', *this );
    nsub.notify_type = NOTIFY_IS_INITIAL;
    nsub.sub_count = refcnt;
    this->sub_route.notify_sub( nsub );
  }
}

/* when client connection stops */
void
RvOmmSubmgr::on_shutdown( EvSocket &conn,  const char *err,
                          size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "RvClient shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  this->ft.finish_ms = 0;
  if ( ! this->is_stopped )
    this->on_stop();
#if 0
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
#endif
}

