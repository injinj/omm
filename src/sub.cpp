#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raimd/rwf_msg.h>
#include <raimd/md_dict.h>
#include <raimd/app_a.h>
#include <omm/ev_omm.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

bool
EvOmmClient::send_subscribe( const char *sub,  size_t sub_len,
                             bool is_initial ) noexcept
{
  RouteLoc     loc;
  OmmSource  * src;
  OmmRoute   * rt;
  const char * ric     = sub;
  size_t       ric_len = sub_len;
  uint32_t     stream_id,
               h       = kv_crc_c( sub, sub_len, 0 );
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain,
                                         this->start_ns )) == NULL )
    return false;

  rt = this->sub_tab.upsert( h, sub, sub_len, loc );
  if ( loc.is_new ) {
    stream_id      = this->next_stream_id++;
    rt->service_id = src->service_id;
    rt->domain     = domain;
    rt->stream_id  = stream_id;
    rt->msg_cnt    = 0;
    rt->stream_type = ( is_initial ? IS_SOLICITED : IS_NONE );
    this->stream_ht->upsert_rsz( this->stream_ht, stream_id, h );
  }
  else {
    if ( ! is_initial ) /* already subscribed */
      return true;
    rt->stream_type = IS_SOLICITED;
    stream_id = rt->stream_id;
  }
  TempBuf      temp_buf = this->mktemp( 128 );
  MDMsgMem     mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    REQUEST_MSG_CLASS, (RdmDomainType) domain, stream_id );
  msg.set( X_STREAMING );
  if ( rt->stream_type != IS_SOLICITED )
    msg.set( X_NO_REFRESH );
  msg.add_priority( 1, 1 )
     .add_qos( src->info.qos[ 0 ] )
     .add_msg_key()
       .service_id( src->service_id )
       .name( ric, ric_len )
       .name_type( NAME_TYPE_RIC )
  .end_msg();

  this->send_msg( "subscribe", msg, temp_buf );
  return true;
}

bool
EvOmmClient::send_snapshot( const char *sub,  size_t sub_len ) noexcept
{
  RouteLoc     loc;
  OmmSource  * src;
  OmmRoute   * rt;
  const char * ric     = sub;
  size_t       ric_len = sub_len;
  uint32_t     stream_id,
               h       = kv_crc_c( sub, sub_len, 0 );
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain,
                                         this->start_ns )) == NULL )
    return false;
  /* a snapshot always gets its own stream, alongside any subscription of
   * the same subject (find_stream walks the hash collisions) */
  stream_id = this->next_stream_id++;
  rt = this->sub_tab.insert( h, sub, sub_len, loc );
  rt->service_id  = src->service_id;
  rt->domain      = domain;
  rt->stream_id   = stream_id;
  rt->msg_cnt     = 0;
  rt->stream_type = IS_SNAPSHOT;
  this->stream_ht->upsert_rsz( this->stream_ht, stream_id, h );

  TempBuf      temp_buf = this->mktemp( 128 );
  MDMsgMem     mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    REQUEST_MSG_CLASS, (RdmDomainType) domain, stream_id );
  msg.add_priority( 1, 1 )
     .add_qos( src->info.qos[ 0 ] )
     .add_msg_key()
       .service_id( src->service_id )
       .name( ric, ric_len )
       .name_type( NAME_TYPE_RIC )
  .end_msg();

  this->send_msg( "snapshot", msg, temp_buf );
  return true;
}

/* the snapshot stream is done: a real ADS closes a non-streaming stream
 * itself (stream_state NON_STREAMING / CLOSED in the refresh); a provider
 * that treated it as streaming gets a CLOSE.  Either way the route goes */
void
EvOmmClient::close_snapshot( OmmSubjRoute &sub_rt,  RwfMsg &msg ) noexcept
{
  OmmRoute * rt = sub_rt.rt;
  if ( msg.msg.state.stream_state == STREAM_STATE_OPEN ) {
    TempBuf      temp_buf = this->mktemp( 128 );
    MDMsgMem     mem;
    RwfMsgWriter cls( mem, NULL, temp_buf.msg, temp_buf.len,
                      CLOSE_MSG_CLASS, (RdmDomainType) rt->domain,
                      rt->stream_id );
    cls.add_msg_key()
       .service_id( rt->service_id )
       .name( rt->value, rt->len )
       .name_type( NAME_TYPE_RIC )
    .end_msg();
    this->send_msg( "close snapshot", cls, temp_buf );
    this->idle_push_write();
  }
  size_t pos;
  if ( this->stream_ht->find( rt->stream_id, pos ) )
    this->stream_ht->remove_rsz( this->stream_ht, pos );
  this->sub_tab.remove( sub_rt.loc );
}

bool
EvOmmClient::send_unsubscribe( const char *sub,  size_t sub_len ) noexcept
{
  RouteLoc     loc;
  OmmSource  * src;
  OmmRoute   * rt;
  const char * ric     = sub;
  size_t       ric_len = sub_len;
  uint32_t     h       = kv_crc_c( sub, sub_len, 0 );
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain,
                                         this->start_ns )) == NULL )
    return false;
  
  rt = this->sub_tab.find( h, sub, sub_len, loc );
  while ( rt != NULL && rt->stream_type == IS_SNAPSHOT ) /* skip snapshots */
    rt = this->sub_tab.find_next( h, sub, sub_len, loc );
  if ( rt == NULL )
    return false;

  TempBuf      temp_buf = this->mktemp( 128 );
  MDMsgMem     mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    CLOSE_MSG_CLASS, (RdmDomainType) rt->domain,
                    rt->stream_id );
  msg.add_msg_key()
     .service_id( rt->service_id )
     .name( ric, ric_len )
     .name_type( NAME_TYPE_RIC )
  .end_msg();

  this->send_msg( "unsubscribe", msg, temp_buf );

  size_t pos;
  if ( this->stream_ht->find( rt->stream_id, pos ) )
    this->stream_ht->remove_rsz( this->stream_ht, pos );
  this->sub_tab.remove( loc );
  return true;
}

void
EvOmmClient::forward_msg( RwfMsg &msg ) noexcept
{
  if ( is_omm_debug )
    debug_print( "forward_msg", msg );

  OmmSubjRoute sub_rt;
  if ( this->find_stream( msg.msg.stream_id, sub_rt, false ) ) {
    if ( this->cb == NULL )
      this->publish_msg( msg, sub_rt );
    else
      this->cb->on_omm_msg( sub_rt.rt->value, sub_rt.rt->len, sub_rt.hash, msg);
    /* a snapshot stream ends with its refresh (or a status closing it) */
    if ( sub_rt.rt->stream_type == IS_SNAPSHOT ) {
      bool done = false;
      if ( msg.msg.msg_class == REFRESH_MSG_CLASS )
        done = msg.msg.test( X_REFRESH_COMPLETE );
      else if ( msg.msg.msg_class == STATUS_MSG_CLASS )
        done = ( msg.msg.test( X_HAS_STATE ) &&
                 msg.msg.state.stream_state != STREAM_STATE_OPEN );
      if ( done )
        this->close_snapshot( sub_rt, msg );
    }
  }
}

/* provider publish path: a message for a subject goes to EVERY stream a
 * consumer has open on it (an ADS stamps each stream id), with the rules:
 *  - solicited refresh: only to streams that asked (IS_SOLICITED), and to
 *    snapshot streams, which it also closes (state -> NON_STREAMING)
 *  - unsolicited refresh (the source re-sent the image): to all streams;
 *    it satisfies a pending solicited request and closes snapshots too
 *  - update: streaming, unpaused streams only
 *  - status: all streams; a closing stream state drops them */
bool
EvOmmConn::on_msg( EvPublish &pub ) noexcept
{
  if ( pub.msg_enc != RWF_MSG_TYPE_ID )
    return true;
  RouteLoc   loc;
  OmmRoute * rt = this->sub_tab.find( pub.subj_hash, pub.subject,
                                      pub.subject_len, loc );
  if ( rt == NULL )
    return true;

  const uint8_t * m         = (const uint8_t *) pub.msg;
  uint8_t         msg_class = RwfMsgPeek::get_msg_class( m, pub.msg_len );
  uint16_t        msg_flags = RwfMsgPeek::get_msg_flags( m, pub.msg_len );
  size_t          state_off = 0; /* refresh: where the state byte is */
  bool            solicited = false,
                  closing   = false;

  if ( msg_class == REFRESH_MSG_CLASS ) {
    solicited = ( msg_flags & RWF_REFRESH_SOLICITED ) != 0;
    /* hdr_size(2) class(1) domain(1) stream(4) flags(u15) container(1)
     * [seq_num(4)] state */
    state_off = 8 + ( m[ 8 ] < 0x80 ? 1 : 2 ) + 1 +
                ( ( msg_flags & RWF_REFRESH_HAS_SEQ_NUM ) != 0 ? 4 : 0 );
    if ( state_off >= pub.msg_len )
      state_off = 0;
  }
  else if ( msg_class == STATUS_MSG_CLASS ) {
    if ( ( msg_flags & RWF_STATUS_HAS_STATE ) != 0 ) {
      /* hdr(8) flags container [state] */
      size_t off = 8 + ( m[ 8 ] < 0x80 ? 1 : 2 ) + 1;
      if ( off < pub.msg_len )
        closing = ( ( m[ off ] >> 3 ) != STREAM_STATE_OPEN );
    }
  }

  uint32_t closed_ids[ 64 ];
  uint32_t ncl = 0;
  for ( ; rt != NULL;
        rt = this->sub_tab.find_next( pub.subj_hash, pub.subject,
                                      pub.subject_len, loc ) ) {
    bool   send = true, drop = false;
    size_t st   = 0;
    switch ( msg_class ) {
      case REFRESH_MSG_CLASS:
        if ( rt->stream_type == IS_SNAPSHOT ) {
          st   = state_off; /* rewrite OPEN -> NON_STREAMING */
          drop = true;
        }
        else if ( solicited ) {
          if ( rt->stream_type != IS_SOLICITED )
            send = false;
          else
            rt->stream_type = IS_NONE;
        }
        else if ( rt->stream_type == IS_SOLICITED )
          rt->stream_type = IS_NONE; /* the image arrived unsolicited */
        break;
      case UPDATE_MSG_CLASS:
        if ( ( rt->rt_flags & RT_STREAMING ) == 0 ||
             ( rt->rt_flags & RT_PAUSED ) != 0 || rt->stream_type == IS_SNAPSHOT )
          send = false;
        break;
      case STATUS_MSG_CLASS:
        drop = closing;
        break;
      default:
        break;
    }
    if ( send ) {
      rt->msg_cnt++;
      this->send_stream_msg( *rt, m, pub.msg_len, st );
    }
    if ( drop && ncl < 64 )
      closed_ids[ ncl++ ] = rt->stream_id;
  }
  /* drop the streams that ended; the subject's last one unsubscribes */
  for ( uint32_t i = 0; i < ncl; i++ ) {
    OmmSubjRoute sub_rt;
    if ( this->find_stream( closed_ids[ i ], sub_rt, false ) ) {
      bool last = this->remove_stream( sub_rt );
      if ( last ) {
        NotifySub nsub( pub.subject, pub.subject_len, NULL, 0, pub.subj_hash,
                        false, 'O', *this );
        this->sub_route.del_sub( nsub );
      }
    }
  }
  this->idle_push_write();
  return true;
}

/* copy msg to the connection with rt's stream id; state_off != 0 rewrites
 * the refresh state to NON_STREAMING (a snapshot's refresh ends it) */
void
EvOmmConn::send_stream_msg( OmmRoute &rt,  const void *msg,  size_t msg_len,
                            size_t state_off ) noexcept
{
  size_t len = msg_len + 3;
  if ( len > this->max_frag_size ) {
    /* fragment_msg stamps the stream id; state rewrite needs a copy */
    if ( state_off == 0 )
      this->fragment_msg( (const uint8_t *) msg, msg_len, rt.stream_id );
    else {
      uint8_t * tmp = (uint8_t *) this->alloc_temp( msg_len );
      ::memcpy( tmp, msg, msg_len );
      tmp[ state_off ] = (uint8_t) ( ( STREAM_STATE_NON_STREAMING << 3 ) |
                                     ( tmp[ state_off ] & 7 ) );
      this->fragment_msg( tmp, msg_len, rt.stream_id );
    }
    return;
  }
  uint8_t * buf = (uint8_t *) this->alloc( len );
  ::memcpy( &buf[ 3 ], msg, msg_len );
  set_u32<MD_BIG>( &buf[ 3 + 4 ], rt.stream_id );
  if ( state_off != 0 )
    buf[ 3 + state_off ] = (uint8_t) ( ( STREAM_STATE_NON_STREAMING << 3 ) |
                                       ( buf[ 3 + state_off ] & 7 ) );
  buf[ 0 ] = (uint8_t) ( ( len >> 8 ) & 0xff );
  buf[ 1 ] = (uint8_t) ( len & 0xff );
  buf[ 2 ] = IPC_DATA;
  this->sz += len;
}

uint32_t
EvOmmConn::count_subject_routes( uint32_t hash,  const char *sub,
                                 size_t sub_len,
                                 const OmmRoute *except ) noexcept
{
  RouteLoc   loc;
  uint32_t   n  = 0;
  OmmRoute * rt = this->sub_tab.find( hash, sub, sub_len, loc );
  for ( ; rt != NULL; rt = this->sub_tab.find_next( hash, sub, sub_len, loc ) )
    if ( rt != except && rt->domain != 0 )
      n++;
  return n;
}

bool
EvOmmConn::remove_stream( OmmSubjRoute &sub_rt ) noexcept
{
  OmmRoute * rt   = sub_rt.rt;
  bool       last = ( this->count_subject_routes( rt->hash, rt->value, rt->len,
                                                  rt ) == 0 );
  size_t pos;
  if ( this->stream_ht->find( rt->stream_id, pos ) )
    this->stream_ht->remove_rsz( this->stream_ht, pos );
  this->sub_tab.remove( sub_rt.loc );
  return last;
}

bool
EvOmmConn::find_stream( uint32_t stream_id,  OmmSubjRoute &sub_rt,
                        bool check_coll ) noexcept
{
  if ( this->stream_ht->find( stream_id, sub_rt.pos, sub_rt.hash ) ) {
    sub_rt.rt = this->sub_tab.find_by_hash( sub_rt.hash, sub_rt.loc );
    sub_rt.hcnt = 0;
    while ( sub_rt.rt != NULL ) {
      sub_rt.hcnt++;
      if ( sub_rt.rt->stream_id == stream_id ) {
        if ( check_coll && sub_rt.hcnt == 1 ) {
          RouteLoc tmp_loc = sub_rt.loc;
          if ( this->sub_tab.find_next_by_hash( sub_rt.hash, tmp_loc ) )
            sub_rt.hcnt++;
        }
        return true;
      }
      sub_rt.rt = this->sub_tab.find_next_by_hash( sub_rt.hash, sub_rt.loc );
    }
  }
  return false;
}

/* connection going away: every stream closes; one unsubscribe per subject */
void
EvOmmConn::close_streams( void ) noexcept
{
  OmmSubjRoute sub_rt;
  uint32_t     stream_id;

  for ( bool b = this->stream_ht->first( sub_rt.pos ); b;
        b = this->stream_ht->next( sub_rt.pos ) ) {
    this->stream_ht->get( sub_rt.pos, stream_id, sub_rt.hash );
    sub_rt.rt = this->sub_tab.find_by_hash( sub_rt.hash, sub_rt.loc );
    while ( sub_rt.rt != NULL ) {
      if ( sub_rt.rt->stream_id == stream_id ) {
        if ( sub_rt.rt->domain != 0 ) {
          OmmRoute & rt = *sub_rt.rt;
          rt.domain = 0; /* closed */
          if ( this->count_subject_routes( rt.hash, rt.value, rt.len ) == 0 ) {
            NotifySub nsub( rt.value, rt.len, NULL, 0, rt.hash, false, 'O',
                            *this );
            this->sub_route.del_sub( nsub );
          }
        }
        break;
      }
      sub_rt.rt = this->sub_tab.find_next_by_hash( sub_rt.hash, sub_rt.loc );
    }
  }
}

bool
EvOmmConn::msg_key_to_sub( RwfMsgHdr &hdr,  OmmSubject &subj ) noexcept
{
  RwfMsgKey & msg_key = hdr.msg_key;
  OmmSource * src     = NULL;

  if ( hdr.test( X_HAS_MSG_KEY ) && msg_key.test( X_HAS_SERVICE_ID ) ) {
    src = this->source_db.find_source( msg_key.service_id, 0 );
    for ( ; src != NULL; src = src->next ) {
      if ( src->info.capability_exists( hdr.domain_type ) )
        break;
    }
  }
  subj.sub     = NULL;
  subj.sub_len = 0;
  subj.hash    = 0;
  if ( (subj.src = src) == NULL ) {
    fprintf( stderr, "No such service %u domain %u\n", msg_key.service_id,
             hdr.domain_type );
    return false;
  }
  if ( msg_key.test( X_HAS_NAME ) ) {
    const char * sector  = rdm_sector_str[ hdr.domain_type ];
    size_t       svc_len = src->info.service_name_len;

    subj.sub = this->alloc_temp( svc_len + msg_key.name_len + 16 );

    CatPtr cat( subj.sub );

    subj.sub_len = cat.b( src->info.service_name, svc_len ).s( "." )
                      .b( sector, rdm_sector_strlen( sector ) ).s( "." )
                      .b( msg_key.name, msg_key.name_len ).end();
    subj.hash = kv_crc_c( cat.start, subj.sub_len, 0 );
    return true;
  }
  return false;
}

/* open a stream for a request / a publisher refresh.  The stream id is the
 * key: a new id always gets its own route (several per subject are fine),
 * an open id with the same subject is a reissue (loc.is_new false), an open
 * id with another subject is refused.  hcnt = routes the subject already
 * had on this connection (0 -> first, the route table gets an add_sub) */
bool
EvOmmConn::add_subj_stream( RwfMsgHdr &hdr,  OmmSubject &subj,
                            OmmSubjRoute &sub_rt ) noexcept
{
  if ( this->find_stream( hdr.stream_id, sub_rt, false ) ) {
    OmmRoute & rt = *sub_rt.rt;
    if ( rt.hash != subj.hash || rt.len != subj.sub_len ||
         ::memcmp( rt.value, subj.sub, subj.sub_len ) != 0 )
      return false; /* stream id busy with another item */
    sub_rt.loc.is_new = false;
    sub_rt.hcnt = this->count_subject_routes( subj.hash, subj.sub,
                                              subj.sub_len, &rt );
    return true;
  }
  sub_rt.hcnt = this->count_subject_routes( subj.hash, subj.sub, subj.sub_len );
  sub_rt.rt   = this->sub_tab.insert( subj.hash, subj.sub, subj.sub_len,
                                      sub_rt.loc );
  if ( sub_rt.rt == NULL )
    return false;
  sub_rt.loc.is_new = true;
  OmmRoute & rt   = *sub_rt.rt;
  rt.service_id   = subj.src->service_id;
  rt.domain       = hdr.domain_type;
  rt.stream_id    = hdr.stream_id;
  rt.msg_cnt      = 0;
  rt.stream_type  = IS_NONE;
  rt.rt_flags     = RT_STREAMING;
  rt.prio_class   = 1;
  rt.prio_count   = 1;
  this->stream_ht->upsert_rsz( this->stream_ht, hdr.stream_id, subj.hash );
  return true;
}

void
EvOmmService::process_msg( RwfMsg &msg ) noexcept
{
  RwfMsgHdr  & hdr = msg.msg;
  OmmSubject   subj;
  OmmSubjRoute sub_rt;

  if ( is_omm_debug )
    debug_print( "item_request", msg );

  if ( hdr.msg_class == REQUEST_MSG_CLASS ) {
    this->request_stream( msg, subj );
  }
  else if ( hdr.msg_class == REFRESH_MSG_CLASS ) {
    /* a publisher pushing an item into the service */
    if ( ! this->msg_key_to_sub( hdr, subj ) )
      return;
    if ( ! this->add_subj_stream( hdr, subj, sub_rt ) ) {
      fprintf( stderr, "Stream %u already assigned\n", hdr.stream_id );
      return;
    }
    this->publish_msg( msg, sub_rt );
  }
  else if ( hdr.msg_class == UPDATE_MSG_CLASS ||
            hdr.msg_class == STATUS_MSG_CLASS ) {
    if ( this->find_stream( hdr.stream_id, sub_rt, false ) )
      this->publish_msg( msg, sub_rt );
  }
  else if ( hdr.msg_class == CLOSE_MSG_CLASS ) {
    this->close_stream( hdr );
  }
}

/* consumer item request */
bool
EvOmmService::request_stream( RwfMsg &msg,  OmmSubject &subj ) noexcept
{
  RwfMsgHdr  & hdr = msg.msg;
  OmmSubjRoute sub_rt;

  if ( hdr.test( X_HAS_BATCH ) ) {
    this->send_status( msg, STATUS_CODE_USAGE_ERROR,
                       "Batch requests not supported" );
    return false;
  }
  if ( ! this->msg_key_to_sub( hdr, subj ) ) {
    this->send_status( msg, STATUS_CODE_SOURCE_UNKNOWN, "No such service" );
    return false;
  }
  if ( ! this->add_subj_stream( hdr, subj, sub_rt ) ) {
    this->send_status( msg, STATUS_CODE_ALREADY_OPEN,
                       "Stream id is open for another item" );
    return false;
  }
  OmmRoute & rt = *sub_rt.rt;
  bool streaming = hdr.test( X_STREAMING ),
       no_refresh = hdr.test( X_NO_REFRESH ),
       pause      = hdr.test( X_PAUSE_FLAG ),
       reissue    = ! sub_rt.loc.is_new;
  /* request options; a reissue may change them */
  if ( streaming ) rt.rt_flags |= RT_STREAMING; else rt.rt_flags &= ~RT_STREAMING;
  if ( hdr.test( X_MSG_KEY_IN_UPDATES ) ) rt.rt_flags |= RT_KEY_IN_UPDATES;
  if ( hdr.test( X_PRIVATE_STREAM ) )     rt.rt_flags |= RT_PRIVATE;
  if ( pause ) rt.rt_flags |= RT_PAUSED; else rt.rt_flags &= ~RT_PAUSED;
  if ( hdr.test( X_HAS_PRIORITY ) ) {
    rt.prio_class = hdr.priority.clas;
    rt.prio_count = hdr.priority.count;
  }
  /* X_HAS_VIEW: accepted, the full record is sent */
  if ( ! streaming )
    rt.stream_type = IS_SNAPSHOT;
  else if ( ! no_refresh )
    rt.stream_type = IS_SOLICITED;
  else if ( ! reissue )
    rt.stream_type = IS_NONE;

  /* the route table sees one subscription per subject per connection:
   * the first stream adds it, more streams / reissues re-notify it (the
   * source sends a solicited image again) */
  NotifySub nsub( subj.sub, subj.sub_len, NULL, 0, subj.hash,
                  sub_rt.hcnt > 0, 'O', *this );
  nsub.notify_type = NOTIFY_IS_INITIAL;
  if ( sub_rt.loc.is_new && sub_rt.hcnt == 0 )
    this->sub_route.add_sub( nsub );
  else if ( ! no_refresh || ! streaming ) {
    nsub.sub_count = 1;
    this->sub_route.notify_sub( nsub );
  }
  return true;
}

/* consumer CLOSE: that stream only; the subject unsubscribes with its last */
void
EvOmmService::close_stream( RwfMsgHdr &hdr ) noexcept
{
  OmmSubjRoute sub_rt;
  if ( this->find_stream( hdr.stream_id, sub_rt, false ) ) {
    OmmRoute & rt   = *sub_rt.rt;
    uint32_t   hash = rt.hash;
    uint16_t   len  = rt.len;
    char       subj[ 1024 ];
    if ( len > sizeof( subj ) ) len = sizeof( subj );
    ::memcpy( subj, rt.value, len );
    if ( this->remove_stream( sub_rt ) ) {
      NotifySub nsub( subj, len, NULL, 0, hash, false, 'O', *this );
      this->sub_route.del_sub( nsub );
    }
  }
}

void
EvOmmConn::publish_msg( RwfMsg &msg,  OmmSubjRoute &sub_rt ) noexcept
{
  void * msgp  = &((char *) msg.msg_buf)[ msg.msg_off ];
  size_t msgsz = msg.msg_end - msg.msg_off;
  EvPublish pub( sub_rt.rt->value, sub_rt.rt->len, NULL, 0, msgp, msgsz,
                 this->sub_route, *this, sub_rt.rt->hash, RWF_MSG_TYPE_ID );
  pub.hdr_len = msg.msg.header_size + 2;
  this->sub_route.forward_msg( pub, NULL );
}

void
EvOmmClient::on_sub( NotifySub &sub ) noexcept
{
  this->send_subscribe( sub.subject, sub.subject_len,
                        sub.is_notify_initial() );
  this->idle_push_write();
}

void
EvOmmClient::on_resub( NotifySub &sub ) noexcept
{
  this->on_sub( sub );
}

void
EvOmmClient::on_unsub( kv::NotifySub &sub ) noexcept
{
  if ( sub.sub_count == 0 ) {
    this->send_unsubscribe( sub.subject, sub.subject_len );
    this->idle_push_write();
  }
}

void
EvOmmClient::on_psub( kv::NotifyPattern & ) noexcept
{
}

void
EvOmmClient::on_punsub( kv::NotifyPattern & ) noexcept
{
}
