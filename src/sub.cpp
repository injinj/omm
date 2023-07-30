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
EvOmmClient::send_subscribe( const char *sub,  size_t sub_len ) noexcept
{
  RouteLoc     loc;
  OmmSource  * src;
  OmmRoute   * rt;
  const char * ric     = sub;
  size_t       ric_len = sub_len;
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain,
                                         this->start_ns )) == NULL )
    return false;
  uint32_t stream_id, h = kv_crc_c( sub, sub_len, 0 );

  rt = this->sub_tab.upsert( h, sub, sub_len, loc );
  rt->is_solicited = true;
  if ( loc.is_new ) {
    stream_id      = this->next_stream_id++;
    rt->service_id = src->service_id;
    rt->domain     = domain;
    rt->stream_id  = stream_id;
    rt->msg_cnt    = 0;

    this->stream_ht->upsert_rsz( this->stream_ht, stream_id, h );
  }
  else {
    stream_id = rt->stream_id;
  }

  TempBuf      temp_buf = this->mktemp( 128 );
  MDMsgMem     mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    REQUEST_MSG_CLASS, (RdmDomainType) domain, stream_id );
  msg.set( X_STREAMING )
     .add_msg_key()
       .service_id( src->service_id )
       .name( ric, ric_len )
       .name_type( NAME_TYPE_RIC )
  .end_msg();

  this->send_msg( "subscribe", msg, temp_buf );
  return true;
}

bool
EvOmmClient::send_unsubscribe( const char *sub,  size_t sub_len ) noexcept
{
  RouteLoc     loc;
  OmmSource  * src;
  OmmRoute   * rt;
  const char * ric     = sub;
  size_t       ric_len = sub_len;
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain,
                                         this->start_ns )) == NULL )
    return false;
  
  uint32_t h = kv_crc_c( sub, sub_len, 0 );
  rt = this->sub_tab.find( h, sub, sub_len, loc );

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
      this->cb->on_msg( sub_rt.rt->value, sub_rt.rt->len, sub_rt.hash, msg );
  }
}

bool
EvOmmConn::on_msg( EvPublish &pub ) noexcept
{
  OmmRoute *rt;
  rt = this->sub_tab.find( pub.subj_hash, pub.subject, pub.subject_len );
  if ( rt == NULL )
    return true;

  if ( pub.msg_enc == RWF_MSG_TYPE_ID && pub.msg_len > 10 ) {
    if ( ((uint8_t *) pub.msg)[ 2 ] == REFRESH_MSG_CLASS ) {
      uint16_t msg_flags = 0;
      get_u15_prefix( &((uint8_t *) pub.msg)[ 8 ],
                      &((uint8_t *) pub.msg)[ pub.msg_len ], msg_flags );
      if ( ( msg_flags & 0x20 ) != 0 ) {
        if ( ! rt->is_solicited )
          return true;
        rt->is_solicited = false; /* only let one solicitied msg through */
      }
    }
    rt->msg_cnt++;
    size_t len = pub.msg_len + 3;
    if ( len > this->max_frag_size )
      this->fragment_msg( (const uint8_t *) pub.msg, pub.msg_len,
                          rt->stream_id );
    else {
      uint8_t * buf = (uint8_t *) this->alloc( len );
      ::memcpy( &buf[ 3 ], pub.msg, pub.msg_len );
      set_u32<MD_BIG>( &buf[ 3 + 4 ], rt->stream_id );
      buf[ 0 ] = (uint8_t) ( ( len >> 8 ) & 0xff );
      buf[ 1 ] = (uint8_t) ( len & 0xff );
      buf[ 2 ] = IPC_DATA;
      this->sz += len;
    }
    this->idle_push( EV_WRITE );
  }
  return true;
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

void
EvOmmConn::close_streams( void ) noexcept
{
  OmmSubjRoute sub_rt;
  uint32_t     stream_id;

  for ( bool b = this->stream_ht->first( sub_rt.pos ); b;
        b = this->stream_ht->next( sub_rt.pos ) ) {
    this->stream_ht->get( sub_rt.pos, stream_id, sub_rt.hash );

    sub_rt.rt   = this->sub_tab.find_by_hash( sub_rt.hash, sub_rt.loc );
    sub_rt.hcnt = 0;
    while ( sub_rt.rt != NULL ) {
      if ( sub_rt.rt->domain != 0 ) {
        sub_rt.hcnt++;
        if ( sub_rt.rt->stream_id == stream_id ) {
          if ( sub_rt.hcnt == 1 ) {
            RouteLoc tmp_loc = sub_rt.loc;
            do {
              OmmRoute *rt =
                this->sub_tab.find_next_by_hash( sub_rt.hash, tmp_loc );
              if ( rt == NULL )
                break;
              if ( rt->domain != 0 )
                sub_rt.hcnt++;
            } while ( sub_rt.hcnt == 1 );
          }
          NotifySub nsub( sub_rt.rt->value, sub_rt.rt->len, NULL, 0, sub_rt.hash,
                          sub_rt.hcnt > 1, 'O', *this );
          this->sub_route.del_sub( nsub );
          sub_rt.rt->domain = 0;
          break;
        }
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

bool
EvOmmConn::add_subj_stream( RwfMsgHdr &hdr,  OmmSubject &subj,
                            OmmSubjRoute &sub_rt ) noexcept
{
  sub_rt.rt = this->sub_tab.upsert2( subj.hash, subj.sub, subj.sub_len,
                                     sub_rt.loc, sub_rt.hcnt );
  if ( sub_rt.rt == NULL )
    return false;
  if ( ! sub_rt.loc.is_new && sub_rt.rt->stream_id != hdr.stream_id )
    return false;
  if ( this->stream_ht->find( hdr.stream_id, sub_rt.pos ) )
    return false;

  if ( sub_rt.loc.is_new ) {
    sub_rt.rt->service_id   = subj.src->service_id;
    sub_rt.rt->domain       = hdr.domain_type;
    sub_rt.rt->stream_id    = hdr.stream_id;
    sub_rt.rt->msg_cnt      = 0;
    sub_rt.rt->is_solicited = false;

    this->stream_ht->set_rsz( this->stream_ht, hdr.stream_id, sub_rt.pos,
                              subj.hash );
  }
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
    if ( ! this->msg_key_to_sub( hdr, subj ) ) {
      this->send_status( msg, STATUS_CODE_INVALID_ARGUMENT,
                         "No service id route" );
      return;
    }
    if ( ! this->add_subj_stream( hdr, subj, sub_rt ) )
      this->send_status( msg, STATUS_CODE_ALREADY_OPEN, "Already subscribed" );

    sub_rt.rt->is_solicited = true;
    NotifySub nsub( subj.sub, subj.sub_len, NULL, 0, subj.hash,
                    sub_rt.hcnt > 0, 'O', *this );
    if ( sub_rt.loc.is_new ) {
      this->sub_route.add_sub( nsub );
    }
    else {
      nsub.sub_count = 1;
      this->sub_route.notify_sub( nsub );
    }
  }
  else if ( hdr.msg_class == REFRESH_MSG_CLASS ) {
    if ( ! this->msg_key_to_sub( hdr, subj ) )
      return;
    if ( ! this->add_subj_stream( hdr, subj, sub_rt ) ) {
      fprintf( stderr, "Already have stream assigned\n" );
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
    if ( this->find_stream( hdr.stream_id, sub_rt, true ) ) {
      NotifySub nsub( sub_rt.rt->value, sub_rt.rt->len, NULL, 0, sub_rt.hash,
                      sub_rt.hcnt > 1, 'O', *this );
      this->sub_route.del_sub( nsub );
      this->stream_ht->remove( sub_rt.pos );
      this->sub_tab.remove( sub_rt.loc );
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
EvOmmClient::on_sub( kv::NotifySub &sub ) noexcept
{
  this->send_subscribe( sub.subject, sub.subject_len );
}

void
EvOmmClient::on_unsub( kv::NotifySub &sub ) noexcept
{
  if ( sub.sub_count == 0 )
    this->send_unsubscribe( sub.subject, sub.subject_len );
}

void
EvOmmClient::on_psub( kv::NotifyPattern & ) noexcept
{
}

void
EvOmmClient::on_punsub( kv::NotifyPattern & ) noexcept
{
}
