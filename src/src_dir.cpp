#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>
#include <raimd/rwf_msg.h>
#include <raimd/md_dict.h>
#include <raimd/app_a.h>
#define DEFINE_RWF_SECTORS
#include <omm/ev_omm_client.h>
#include <omm/ipc.h>
#include <omm/src_dir.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

void
EvOmmClient::send_directory_request( void ) noexcept
{
  TempBuf temp_buf = this->mktemp( 256 );
  MDMsgMem mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    REQUEST_MSG_CLASS, SOURCE_DOMAIN, directory_stream_id );

  msg.set( X_STREAMING )
     .add_priority( 1, 1 )
     .add_msg_key()
       .filter( DIR_SVC_ALL_FILTERS )
  .end_msg();

  this->send_msg( "dir_request", msg, temp_buf );
}

void
EvOmmClient::recv_directory_response( RwfMsg &msg ) noexcept
{
  if ( is_omm_debug )
    debug_print( "directory_response", msg );
  RwfMsg * map = msg.get_container_msg();
  if ( map == NULL || map->base.type_id != RWF_MAP ) {
    if ( map == NULL )
      fprintf( stderr, "no sources available\n" );
    else
      fprintf( stderr, "dir response not a map\n" );
    return;
  }
  UpdSrcCnt cnt = this->source_db.update_source_map( this->start_ns, *map );
  if ( cnt.new_src_cnt > 0 )
    this->src_count += cnt.new_src_cnt;
  if ( cnt.rem_src_cnt > 0 )
    this->src_count -= cnt.rem_src_cnt;
  this->source_db.notify_source_change();
}

void
OmmSrcListener::on_src_change( void ) noexcept
{
}

void
EvOmmConn::init_streams( void ) noexcept
{
  this->stream_ht = UIntHashTab::resize( NULL );
}

void
EvOmmConn::release_streams( void ) noexcept
{
  if ( this->stream_ht != NULL ) {
    delete this->stream_ht;
    this->stream_ht = NULL;
  }
  this->sub_tab.release();
  if ( this->src_count != 0 ) {
    this->source_db.drop_sources( this->start_ns );
    this->src_count = 0;
    this->source_db.notify_source_change();
  }
}

void
EvOmmService::recv_directory_request( RwfMsg &msg ) noexcept
{
  RwfMsgHdr & hdr = msg.msg;

  if ( hdr.msg_class == REQUEST_MSG_CLASS ) {
    if ( is_omm_debug )
      debug_print( "directory_request", msg );

    TempBuf      temp_buf = this->mktemp( 8 * 1024 );
    MDMsgMem     mem;
    RwfMsgWriter resp( mem, NULL, temp_buf.msg, temp_buf.len, /* spc for ipc hdr */
                       REFRESH_MSG_CLASS, SOURCE_DOMAIN, hdr.stream_id );

    RwfMsgKey & msg_key = hdr.msg_key;
    uint32_t    filter  = msg_key.filter;

    resp.set( X_CLEAR_CACHE, X_REFRESH_COMPLETE, X_SOLICITED )
       .add_state( DATA_STATE_OK, STREAM_STATE_OPEN, "Source directory complete" )
       .add_msg_key()
         .filter( filter )
       .end_msg_key();
    resp.add_map( MD_UINT )
        .apply( *this, &EvOmmService::add_source_dirs, filter )
        .end_msg();

    this->send_msg( "directory_response", resp, temp_buf );
  }
  else if ( hdr.msg_class == REFRESH_MSG_CLASS ) {
    if ( is_omm_debug )
      debug_print( "directory_refresh", msg );

    RwfMsg * map = msg.get_container_msg();
    if ( map == NULL || map->base.type_id != RWF_MAP ) {
      fprintf( stderr, "no sources refreshed\n" );
      return;
    }
    UpdSrcCnt cnt = this->source_db.update_source_map( this->start_ns, *map );
    if ( cnt.new_src_cnt > 0 )
      this->src_count += cnt.new_src_cnt;
    if ( cnt.rem_src_cnt > 0 )
      this->src_count -= cnt.rem_src_cnt;
    /*if ( cnt > 0 ) {
      this->src_count += cnt;
      this->source_db.print_sources();
    }*/
    this->source_db.notify_source_change();
  }
  else if ( hdr.msg_class == UPDATE_MSG_CLASS ) {
    if ( is_omm_debug )
      debug_print( "directory_update", msg );

    RwfMsg * map = msg.get_container_msg();
    if ( map == NULL || map->base.type_id != RWF_MAP ) {
      fprintf( stderr, "no sources updated\n" );
      return;
    }
    UpdSrcCnt cnt = this->source_db.update_source_map( this->start_ns, *map );
    if ( cnt.new_src_cnt > 0 )
      this->src_count += cnt.new_src_cnt;
    if ( cnt.rem_src_cnt > 0 )
      this->src_count -= cnt.rem_src_cnt;
    /*if ( cnt > 0 ) {
      this->src_count += cnt;
      this->source_db.print_sources();
    }*/
    this->source_db.notify_source_change();
  }
  else if ( hdr.msg_class == CLOSE_MSG_CLASS ) {
    if ( is_omm_debug )
      debug_print( "directory_close", msg );
    printf( "directory closed\n" );
  }
}

void
OmmSourceDB::notify_source_change( void ) noexcept
{
  OmmSrcListener *next;
  for ( OmmSrcListener *l = this->listener_list.hd; l != NULL; l = next ) {
    next = l->next;
    l->on_src_change();
  }
}

RwfMapWriter &
EvOmmService::add_source_dirs( RwfMapWriter &map,
                               uint32_t filter ) noexcept
{
  const char * dict[ MAX_DICTIONARIES ];
  size_t j;
  for ( size_t i = 0; i < this->source_db.source_list.count; i++ ) {
    if ( this->source_db.source_list.ptr[ i ].is_empty() )
      continue;
    OmmSource & source = *this->source_db.source_list.ptr[ i ].hd;
    RwfFilterListWriter &fil =
      map.add_filter_list( MAP_ADD_ENTRY, source.service_id, MD_UINT );
    uint32_t x = filter & source.filter;

    if ( ( x & DIR_SVC_INFO_FILTER ) != 0 ) {
      ServiceInfo & info = source.info;
      RwfElementListWriter &el = fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_INFO_ID );

      el.append_string( NAME  , info.service_name )
        .append_string( VEND  , info.vendor )
        .append_uint  ( IS_SRC, info.is_source)
        .append_array ( CAPAB , info.capabilities, info.num_capabilities, MD_UINT );

      if ( info.num_dict > 0 ) {
        for ( j = 0; j < info.num_dict; j++ )
          dict[ j ] = info.dictionaries_provided[ j ];
        el.append_array ( DICT_PROV, dict, j );
      }
      if ( info.num_dict_used > 0 ) {
        for ( j = 0; j < info.num_dict_used; j++ )
          dict[ j ] = info.dictionaries_used[ j ];
        el.append_array ( DICT_USED, dict, j );
      }
      if ( info.num_qos > 0 ) {
        el.append_array ( QOS, info.qos, info.num_qos )
          .append_uint  ( SUP_QOS_RNG, info.supports_qos_range );
      }
      if ( info.item_list[ 0 ] != '\0' )
        el.append_string( ITEM_LST, info.item_list );
      el.append_uint  ( SUP_OOB_SNAP, 1 )
        .append_uint  ( ACC_CONS_STA, 0 )
        .end_element_list();
    }

    if ( ( x & DIR_SVC_STATE_FILTER ) != 0 ) {
      ServiceStateInfo & state = source.state;
      RwfElementListWriter &el = fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_STATE_ID );
      el.append_uint ( SVC_STATE, state.service_state )
        .append_uint ( ACC_REQ  , state.accepting_requests )
        .append_state( STAT     , state.status )
        .end_element_list();
    }

    if ( ( x & DIR_SVC_LOAD_FILTER ) != 0 ) {
      ServiceLoadInfo & load = source.load;
      RwfElementListWriter &el = fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_LOAD_ID );
      el.append_uint( OPEN_LIM, load.open_limit )
        .append_uint( OPEN_WIN, load.open_window )
        .append_uint( LOAD_FACT, load.load_factor )
        .end_element_list();
    }

    if ( ( x & DIR_SVC_LINK_FILTER ) != 0 ) {
      RwfMapWriter &lnk = fil.add_map( FILTER_SET_ENTRY, DIR_SVC_LINK_ID );
      lnk.set_key_type( MD_STRING );
      for ( size_t i = 0; i < source.link_cnt; i++ ) {
        ServiceLinkInfo & link = *source.link[ i ];
        lnk.add_element_list( MAP_ADD_ENTRY, link.link_name )
           .append_uint  ( TYPE      , link.type )
           .append_uint  ( LINK_STATE, link.link_state )
           .append_uint  ( LINK_CODE , link.link_code )
           .append_string( TEXT      , link.text )
           .end_element_list();
      }
      lnk.end_map();
    }
  }
  return map;
}

void
OmmSourceDB::drop_sources( uint64_t origin ) noexcept
{
  size_t match_count = 0;
  for ( size_t i = 0; i < this->source_list.count; i++ ) {
    SourceList & list = this->source_list.ptr[ i ];
    OmmSource  * next;
    for ( OmmSource * src = list.hd; src != NULL; src = next ) {
      next = src->next;
      if ( src->origin == origin ) {
        match_count++;
        list.pop( src );
        if ( list.is_empty() ) {
          size_t pos;
          if ( this->service_ht->find( src->service_id, pos ) )
            this->service_ht->remove( pos );
        }
        src->clear_info( DIR_SVC_LINK_ID );
        delete src;
      }
    }
  }
  if ( match_count != 0 )
    this->index_domains();
}

UpdSrcCnt
OmmSourceDB::update_source_map( uint64_t origin,  RwfMsg &map ) noexcept
{
  MDFieldReader rd( map );
  UpdSrcCnt count;
  if ( rd.first() ) {
    MDType key_ftype = map.map.key_ftype;
    do {
      RwfFieldIter * f = (RwfFieldIter *) rd.iter;
      if ( f->u.map.action == MAP_ADD_ENTRY ||
           f->u.map.action == MAP_UPDATE_ENTRY ) {

        MDReference mref( f->u.map.key, f->u.map.keylen, key_ftype, MD_BIG );
        uint32_t    service_id = get_int<uint32_t>( mref );
        MDMsg     * entry;

        if ( rd.type() == MD_MESSAGE && rd.get_sub_msg( entry ) ) {
          if ( this->update_source_entry( origin, service_id,
                                          *(RwfMsg *) entry ) )
            count.new_src_cnt++;
        }
      }
      else if ( f->u.map.action == MAP_DELETE_ENTRY ) {
        MDReference mref( f->u.map.key, f->u.map.keylen, key_ftype, MD_BIG );
        uint32_t    service_id = get_int<uint32_t>( mref );

        if ( this->drop_source_entry( origin, service_id ) )
          count.rem_src_cnt++;
      }
    } while ( rd.next() );
  }
  this->index_domains();
  return count;
}

bool
OmmSourceDB::update_source_entry( uint64_t origin,  uint32_t service_id,
                                  RwfMsg &entry ) noexcept
{
  bool is_new_src = false;
  if ( entry.base.type_id != RWF_FILTER_LIST ) {
    fprintf( stderr, "dir service id %u not filter list\n", service_id );
    return false;
  }

  MDFieldReader rd( entry );
  if ( rd.first() ) {
    do {
      RwfFieldIter * f = (RwfFieldIter *) rd.iter;
      bool is_filter_update = ( f->u.flist.action == FILTER_UPDATE_ENTRY );
      if ( f->u.flist.action == FILTER_SET_ENTRY ||
           f->u.flist.action == FILTER_UPDATE_ENTRY ) {
        MDMsg * fentry;
        if ( rd.type() == MD_MESSAGE && rd.get_sub_msg( fentry ) ) {
          uint32_t info_id = f->u.flist.id;
          is_new_src |=
            this->update_service_info( origin, service_id, info_id,
                                       is_filter_update, *(RwfMsg *) fentry );
        }
      }
      else if ( f->u.flist.action == FILTER_CLEAR_ENTRY ) {
        uint32_t info_id = f->u.flist.id;
        this->clear_service_info( origin, service_id, info_id );
      }
    } while ( rd.next() );
  }
  return is_new_src;
}

bool
OmmSourceDB::drop_source_entry( uint64_t origin,  uint32_t service_id ) noexcept
{
  size_t   pos;
  uint32_t i;
  bool     is_drop_src = false;

  if ( this->service_ht->find( service_id, pos, i ) ) {
    SourceList & list = this->source_list.ptr[ i ];

    for ( OmmSource * src = list.hd; src != NULL; src = src->next ) {
      if ( src->origin == origin ) {
        is_drop_src = true;
        list.pop( src );
        if ( list.is_empty() )
          this->service_ht->remove( pos );
        src->clear_info( DIR_SVC_LINK_ID );
        delete src;
        break;
      }
    }
  }
  return is_drop_src;
}

bool
OmmSourceDB::update_service_info( uint64_t origin,  uint32_t service_id,
                                  uint32_t info_id,  bool is_filter_update,
                                  RwfMsg &info ) noexcept
{
  MDIterMap map[ MAX_OMM_ITER_FIELDS ];
  OmmSource * src = this->find_source( service_id, origin );
  bool is_new_src = false;

  if ( src == NULL ) {
    src = new ( ::malloc( sizeof( OmmSource ) ) )
          OmmSource( origin, service_id );
    this->add_source( src );
    is_new_src = true;
  }

  if ( ! is_filter_update )
    src->clear_info( info_id );
  switch ( (RdmDirSvcInfoId) info_id ) {
    case DIR_SVC_INFO_ID: {
      QosBuf qos_buf[ MAX_QOS ];
      if ( MDIterMap::get_map( info, map,
                               src->info.iter_map( map, qos_buf ) ) > 0 ) {
        for ( size_t i = 0; i < src->info.num_qos; i++ )
          src->info.qos[ i ].decode( qos_buf[ i ], sizeof( qos_buf[ i ] ) );
        src->info.service_name_len = ::strlen( src->info.service_name );
        src->filter |= DIR_SVC_INFO_FILTER;
      }
      break;
    }
    case DIR_SVC_STATE_ID:
      if ( MDIterMap::get_map( info, map, src->state.iter_map( map ) ) > 0 ) {
        src->state.status.decode( src->state.buf, sizeof( src->state.buf ) );
        src->filter |= DIR_SVC_STATE_FILTER;
      }
      break;
    case DIR_SVC_GROUP_ID:
      if ( MDIterMap::get_map( info, map, src->group.iter_map( map ) ) > 0 ) {
        src->group.status.decode( src->group.buf, sizeof( src->group.buf ) );
        src->filter |= DIR_SVC_GROUP_FILTER;
      }
      break;
    case DIR_SVC_LOAD_ID:
      if ( MDIterMap::get_map( info, map, src->load.iter_map( map ) ) > 0 )
        src->filter |= DIR_SVC_LOAD_FILTER;
      break;

    case DIR_SVC_DATA_ID:
      if ( MDIterMap::get_map( info, map, src->data.iter_map( map ) ) > 0 )
        src->filter |= DIR_SVC_DATA_FILTER;
      break;

    case DIR_SVC_LINK_ID: {
      MDFieldReader rd( info );

      MDReference    mref;
      MDMsg        * dentry;

      if ( info.base.type_id  == RWF_MAP &&
           info.map.key_ftype == MD_STRING &&
           rd.first() ) {
        do {
          RwfFieldIter    * f       = (RwfFieldIter *) rd.iter;
          ServiceLinkInfo * link    = NULL;
          const char      * key     = (char *) f->u.map.key;
          size_t            key_len = f->u.map.keylen;

          src->find_link( key, key_len, link );
          if ( f->u.map.action == MAP_ADD_ENTRY ||
               f->u.map.action == MAP_UPDATE_ENTRY ) {
            if ( rd.type() == MD_MESSAGE && rd.get_sub_msg( dentry ) ) {
              if ( link == NULL )
                src->make_link( key, key_len, link );
              if ( MDIterMap::get_map( *dentry, map,
                                       link->iter_map( map ) ) > 0 ) {
                src->filter |= DIR_SVC_LINK_FILTER;
              }
            }
          }
          else if ( link != NULL )
            src->pop_link( link );
        } while ( rd.next() );
      }
      break;
    }
    case DIR_SVC_SEQ_MCAST_ID:
      /* ignored */
      break;
  }
  return is_new_src;
}

void
OmmSourceDB::clear_service_info( uint64_t origin,  uint32_t service_id,
                                 uint32_t info_id ) noexcept
{
  OmmSource * src = this->find_source( service_id, origin );
  if ( src != NULL )
    src->clear_info( info_id );
}

void
OmmSource::clear_info( uint32_t info_id ) noexcept
{
  this->filter &= ~( 1U << info_id );
  switch ( (RdmDirSvcInfoId) info_id ) {
    case DIR_SVC_INFO_ID:  this->info.zero();  break;
    case DIR_SVC_STATE_ID: this->state.zero(); break;
    case DIR_SVC_GROUP_ID: this->group.zero(); break;
    case DIR_SVC_LOAD_ID:  this->load.zero();  break;
    case DIR_SVC_DATA_ID:  this->data.zero();  break;
    case DIR_SVC_LINK_ID:
      for ( size_t i = 0; i < this->link_cnt; i++ ) {
        delete this->link[ i ];
        this->link[ i ] = NULL;
      }
      this->link_cnt = 0;
      break;
    case DIR_SVC_SEQ_MCAST_ID:
      /* ignored */
      break;
  }
}

bool
OmmSource::find_link( const char *key,  size_t keylen,
                      ServiceLinkInfo *&linkp ) noexcept
{
  if ( keylen >= sizeof( this->link[ 0 ]->link_name ) )
    keylen = sizeof( this->link[ 0 ]->link_name ) - 1;
  for ( size_t i = 0; i < this->link_cnt; i++ ) {
    const char *n = this->link[ i ]->link_name;
    if ( n[ keylen ] == '\0' && ::memcmp( n, key, keylen ) == 0 ) {
      linkp = this->link[ i ];
      return true;
    }
  }
  linkp = NULL;
  return false;
}

bool
OmmSource::make_link( const char *key,  size_t keylen,
                      ServiceLinkInfo *&linkp ) noexcept
{
  if ( keylen >= sizeof( this->link[ 0 ]->link_name ) )
    keylen = sizeof( this->link[ 0 ]->link_name ) - 1;
  if ( this->link_cnt < MAX_LINKS ) {
    linkp = new ( ::malloc( sizeof( ServiceLinkInfo ) ) ) ServiceLinkInfo();
    ::memcpy( linkp->link_name, key, keylen );
    this->link[ this->link_cnt++ ] = linkp;
    return true;
  }
  linkp = NULL;
  return false;
}

void
OmmSource::pop_link( ServiceLinkInfo *linkp ) noexcept
{
  size_t i, sz = sizeof( this->link[ 0 ] );
  for ( i = 0; linkp != this->link[ i ]; i++ )
    ;
  if ( i + 1 < this->link_cnt ) {
    sz *= this->link_cnt - ( i + 1 );
    ::memmove( &this->link[ i ], &this->link[ i + 1 ], sz );
  }
  this->link[ --this->link_cnt ] = NULL;
  delete linkp;
  if ( this->link_cnt == 0 )
    this->filter &= ~DIR_SVC_LINK_FILTER;
}

static const uint32_t nil_value = 0xffffffffU;
void
OmmSourceDB::index_domains( void ) noexcept
{
  RouteLoc      loc;
  SourceRoute * rt;
  for ( rt = this->source_sector_tab.first( loc ); rt != NULL;
        rt = this->source_sector_tab.next( loc ) ) {
    if ( rt->next_service != NULL )
      ::free( rt->next_service );
  }
  this->source_sector_tab.release();

  for ( size_t i = 0; i < this->source_list.count; i++ ) {
    for ( OmmSource * src = this->source_list.ptr[ i ].hd; src != NULL;
          src = src->next ) {
      const char * svc     = src->info.service_name;
      size_t       svc_len = src->info.service_name_len,
                   sub_len,
                   sect_len;
      char         sub[ MAX_OMM_STRLEN + 16 ];

      ::memcpy( sub, svc, svc_len );
      sub[ svc_len ] = '.';
      src->info.capabilities_mask = 0;
      for ( uint32_t j = 0; j < src->info.num_capabilities; j++ ) {
        uint8_t cap = src->info.capabilities[ j ];
        if ( cap <= 63 )
          src->info.capabilities_mask |= ( (uint64_t) 1 << cap );
        if ( cap >= MARKET_PRICE_DOMAIN ) {
          if ( cap >= RDM_DOMAIN_COUNT || rdm_sector_str[ cap ] == NULL ) {
            sect_len = uint32_to_string( cap, &sub[ svc_len + 1 ] );
          }
          else {
            const char * sect = rdm_sector_str[ cap ];
            sect_len = rdm_sector_strlen( sect );
            ::memcpy( &sub[ svc_len + 1 ], sect, sect_len );
          }
          sub_len = svc_len + 1 + sect_len;
          sub[ sub_len ] = '\0';
          uint32_t h = kv_crc_c( sub, sub_len, 0 );
          rt = this->source_sector_tab.upsert( h, sub, sub_len, loc );
          if ( loc.is_new ) {
            rt->service_id   = src->service_id;
            rt->service_cnt  = 1;
            rt->domain       = cap;
            rt->next_service = NULL;
          }
          else if ( rt->service_cnt < 0xffffU ) {
            size_t sz = sizeof( rt->next_service[ 0 ] ) * rt->service_cnt;
            rt->next_service = (uint32_t *) ::realloc( rt->next_service, sz );
            rt->next_service[ rt->service_cnt - 1 ] = src->service_id;
            rt->service_cnt++;
          }
          if ( is_omm_debug )
            printf( "%s -> svc_id=%u, dom=%u\n", sub, src->service_id, cap );
        }
      }
    }
  }
}

OmmSource *
OmmSourceDB::match_sub( const char *&sub,  size_t &len,  uint8_t &domain,
                        uint64_t origin ) noexcept
{
  const char * s   = sub;
  size_t       off = 0;

  for ( ; off < len && s[ off ] != '.'; off++ )
    ;
  if ( off < len ) {
    for ( off++; off < len && s[ off ] != '.'; off++ )
      ;
    if ( off < len ) {
      SourceRoute * rt;
      rt = this->source_sector_tab.find( kv_crc_c( sub, off, 0 ), sub, off );
      if ( rt != NULL ) {
        sub += off + 1;
        len -= off + 1;
        domain = rt->domain;
        uint32_t service_id = rt->service_id;
        for ( uint32_t k = 0 ; ;) {
          OmmSource * src = this->find_source( service_id, 0 );
          for ( ; src != NULL; src = src->next ) {
            if ( origin == 0 || origin == src->origin ) {
              if ( src->info.capability_exists( domain ) )
                return src;
            }
          }
          if ( ++k == rt->service_cnt )
            break;
          service_id = rt->next_service[ k - 1 ];
        }
      }
    }
  }
  return NULL;
}

static char *
cap_string( ServiceInfo &info,  char *buf ) noexcept
{
  CatPtr p( buf );
  for ( uint32_t i = 0; i < info.num_capabilities; i++ ) {
    if ( info.capabilities[ i ] >= MARKET_PRICE_DOMAIN &&
         info.capabilities[ i ] < RDM_DOMAIN_COUNT ) {
      const char *s = rdm_sector_str[ info.capabilities[ i ] ];
      if ( p.len() != 0 )
        p.s( "," );
      if ( s != NULL )
        p.s( s );
      else
        p.u( i );
    }
  }
  p.end();
  return buf;
}

static char *
qos_string( ServiceInfo &info,  char *buf ) noexcept
{
  CatPtr p( buf );

  for ( uint32_t i = 0; i < info.num_qos; i++ ) {
    RwfQos & qos = info.qos[ i ];
    const char *t, *r;
    if ( p.len() != 0 )
      p.s( "," );
    switch ( qos.timeliness ) {
      default:
      case 0:                        t = "0"; break;
      case QOS_TIME_REALTIME:        t = "rt"; break;
      case QOS_TIME_DELAYED_UNKNOWN: t = "dl";  break;
      case QOS_TIME_DELAYED:         t = "dl"; break;
    }
    switch ( qos.rate ) {
      default:
      case 0:                       r = "0"; break;
      case QOS_RATE_TICK_BY_TICK:   r = "tic"; break;
      case QOS_RATE_JIT_CONFLATED:  r = "jit"; break;
      case QOS_RATE_TIME_CONFLATED: r = "con"; break;
    }
    p.s( t ).s( "/" ).s( r );
  }
  p.end();

  return buf;
}

void
OmmSource::print_info( bool hdr ) noexcept
{
  char cap[ RDM_DOMAIN_COUNT * 4 ], qos[ MAX_QOS * 8 ];
  char buf[ 120 ];
  CatPtr x( buf );
  if ( hdr ) {
    x.w( 5,  "Svc" )
     .w( 18, "Name" )
     .w( 24, "Capabilities" )
     .w( 15, "QoS" )
     .w( 17, "Link Name" )
     .end();
    printf( "%s\n", buf );
  }
  int32_t n = uint32_digits( this->service_id );
  x.begin();
  x.u( this->service_id, n ).w( 5 - n, "" )
   .w( 18, this->info.service_name )
   .w( 24, cap_string( this->info, cap ) )
   .w( 15, qos_string( this->info, qos ) );
  if ( this->link_cnt > 0 ) {
    x.s( this->link[ 0 ]->link_name );
    if ( this->link[ 0 ]->link_state == LINK_DOWN )
      x.s( "-Down" );
    else if ( this->link[ 0 ]->type == LINK_BROADCAST )
      x.s( "-Bcast" );
  }
  x.end();
  printf( "%s\n", buf );
}

void
OmmSourceDB::print_sources( void ) noexcept
{
  size_t i, j = 0;
  for ( i = 0; i < this->source_list.count; i++ ) {
    if ( ! this->source_list.ptr[ i ].is_empty() ) {
      OmmSource * source = this->source_list.ptr[ i ].hd;
      for (;;) {
        source->print_info( j++ == 0 );
        if ( ( source = source->next) == NULL )
          break;
      }
    }
  }
  printf( "\n" );
  fflush( stdout );
}
