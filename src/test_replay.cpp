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
#include <omm/test_replay.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>
#include <raimd/mf_msg.h>
#include <raimd/sass.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

TestReplay::TestReplay( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept
           : EvSocket( p, p.register_type( "omm_test_replay" ) ),
             poll( p ), sub_route( p.sub_route ),
             dict( d ), source_db( db ), fp( 0 ), fn( 0 ), buf( 0 ), feed( 0 ),
             buflen( 0 ), msg_count( 0 ), feed_len( 0 )
{
  this->sock_opts = OPT_NO_POLL;
}

void
TestReplay::start( void ) noexcept
{
  if ( this->fp != NULL ) {
    int sfd = this->poll.get_null_fd();
    this->PeerData::init_peer( this->poll.get_next_id(), sfd, -1, NULL,
                               "omm_test_replay" );
    this->PeerData::set_name( "omm_test_replay", 15 );
    this->poll.add_sock( this );
    this->poll.timer.add_timer_millis( this->fd, 100, 1, 0 );
  }
}

void
TestReplay::add_replay_file( const char *feed_name,  uint32_t service_id,
                             const char *replay_file ) noexcept
{
  static const char * dict[ 2 ] = { "RWFFld", "RWFEnum" };
  static uint8_t cap[ 4 ] = { LOGIN_DOMAIN, SOURCE_DOMAIN, DICTIONARY_DOMAIN,
                              MARKET_PRICE_DOMAIN };
  static RwfQos  qos      = { QOS_TIME_REALTIME, QOS_RATE_TICK_BY_TICK, 0, 0, 0 };

  char         buf[ 1024 ];
  MDMsgMem     mem;
  RwfMapWriter map( mem, this->dict.rdm_dict, buf, sizeof( buf ) );
  RwfState     state = { STREAM_STATE_OPEN, DATA_STATE_OK, 0, { "OK", 2 } };

  RwfFilterListWriter
    & fil = map.add_filter_list( MAP_ADD_ENTRY, service_id, MD_UINT );

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_INFO_ID )
     .append_string( NAME        , feed_name )
     .append_string( VEND        , "Test" )
     .append_uint  ( IS_SRC      , 1 )
     .append_array ( CAPAB       , cap , 4, MD_UINT )
     .append_array ( DICT_PROV   , dict, 2 )
     .append_array ( DICT_USED   , dict, 2 )
     .append_array ( QOS         , &qos, 1 )
     .append_uint  ( SUP_QOS_RNG , 0 )
     .append_string( ITEM_LST    , "_ITEM_LIST" )
     .append_uint  ( SUP_OOB_SNAP, 1 )
     .append_uint  ( ACC_CONS_STA, 0 )
   .end_element_list();

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_STATE_ID )
     .append_uint  ( SVC_STATE, 1 )
     .append_uint  ( ACC_REQ  , 1 )
     .append_state ( STAT     , state )
   .end_element_list();

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_LOAD_ID )
     .append_uint( OPEN_LIM , 1000 * 1000 )
     .append_uint( OPEN_WIN , 1000 )
     .append_uint( LOAD_FACT, 0 )
   .end_element_list();

  map.end_map();

  RwfMsg *m = RwfMsg::unpack_map( map.buf, 0, map.off, RWF_MAP_TYPE_ID, NULL, mem );
  if ( is_omm_debug ) {
    MDOutput mout;
    if ( m != NULL )
      m->print( &mout );
  }
  this->source_db.update_source_map( this->start_ns, *m );
  size_t len  = ::strlen( replay_file ),
         flen = ::strlen( feed_name );
  this->feed_len = flen;
  this->feed     = (char *) ::malloc( flen + 1 );
  this->fn       = (char *) ::malloc( len + 1 );
  this->fp       = fopen( replay_file, "rb" );
  ::memcpy( this->feed, feed_name, flen + 1 );
  ::memcpy( this->fn, replay_file, len + 1 );
  if ( this->fp == NULL ) {
    perror( replay_file );
  }
}

bool
TestReplay::timer_expire( uint64_t, uint64_t ) noexcept
{
  MDMsgMem mem;
  char     subj_buf[ 1024 ],
         * subj,
           size[ 128 ];
  size_t   sz         = 0;
  uint32_t seqno      = 0;
  int      status     = 0;
  uint16_t flist      = 0,
           msg_type   = MD_UPDATE_TYPE;

  for (;;) {
    if ( fgets( subj_buf, sizeof( subj_buf ), this->fp ) == NULL )
      break;
    if ( subj_buf[ 0 ] <= ' ' || subj_buf[ 0 ] == '#' )
      continue;
    if ( fgets( size, sizeof( size ), this->fp ) == NULL )
      break;
    if ( (sz = atoi( size )) != 0 )
      break;
  }
  if ( sz == 0 ) {
    if ( this->msg_count == 0 ) {
      fprintf( stderr, "\"%s\", no data\n", this->fn );
      return false;
    }
    rewind( this->fp );
    return true;
  }
  if ( sz > this->buflen ) {
    this->buf = (char *) ::realloc( this->buf, sz );
    this->buflen = sz;
  }

  size_t slen = ::strlen( subj_buf );
  while ( slen > 0 && subj_buf[ slen - 1 ] < ' ' )
    subj_buf[ --slen ] = '\0';
  size_t off, dot[ 8 ], dotcnt = 0;
  for ( off = 0; off < slen; off++ )
    if ( subj_buf[ off ] == '.' && dotcnt < 8 )
      dot[ dotcnt++ ] = off;
  if ( dotcnt > 2 ) {
    subj = &subj_buf[ dot[ dotcnt - 2 ] + 1 ];
    slen -= dot[ dotcnt - 2 ] + 1;
  }
  else {
    subj = subj_buf;
  }
  for ( size_t n = 0; n < sz; ) {
    size_t i = fread( &buf[ n ], 1, sz - n, this->fp ); /* message data */
    if ( i == 0 ) {
      if ( feof( this->fp ) ) {
        fprintf( stderr, "\"%s\", eof, truncated msg, subj %s\n", this->fn,
                 subj );
        rewind( this->fp );
        return true;
      }
      perror( this->fn );
      return false;
    }
    n += i;
  }

  MDMsg * m = MDMsg::unpack( this->buf, 0, sz, 0, this->dict.dict, mem );
  if ( m == NULL ) {
    fprintf( stderr, "\"%s\", unpack error, subj %s\n", this->fn, subj );
    return true;
  }

  switch ( m->get_type_id() ) {
    case MARKETFEED_TYPE_ID: {
      MktfdMsg & mf = *(MktfdMsg *) m;
      msg_type = mf_func_to_sass_msg_type( mf.func );
      flist = mf.flist;
      seqno = mf.rtl;
      break;
    }
    case RWF_MSG_TYPE_ID: {
      RwfMsg & rwf = *(RwfMsg *) m;
      msg_type = rwf_to_sass_msg_type( rwf );
      RwfMsg * fl = rwf.get_container_msg();
      if ( fl != NULL )
        flist = fl->fields.flist;
      if ( rwf.msg.test( X_HAS_SEQ_NUM ) )
        seqno = rwf.msg.seq_num;
      break;
    }
    default: {
      MDFieldReader rd( *m );
      if ( rd.find( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN ) )
        rd.get_uint( msg_type );
      if ( rd.find( MD_SASS_SEQ_NO, MD_SASS_SEQ_NO_LEN ) )
        rd.get_uint( seqno );
      break;
    }
  }
  RwfMsgClass  msg_class = ( msg_type == MD_INITIAL_TYPE ? REFRESH_MSG_CLASS :
                                                           UPDATE_MSG_CLASS );
  uint32_t     stream_id = MDDict::dict_hash( subj, slen );
  char         tmp_buf[ 1024 ];
  RwfMsgWriter w( mem, this->dict.rdm_dict, tmp_buf, sizeof( tmp_buf ),
                  msg_class, MARKET_PRICE_DOMAIN, stream_id );
  if ( msg_type == MD_INITIAL_TYPE )
    w.set( X_CLEAR_CACHE, X_REFRESH_COMPLETE );
  else {
    if ( msg_type == MD_CLOSING_TYPE )
      w.add_update( UPD_TYPE_CLOSING_RUN );
    else if ( msg_type == MD_CORRECT_TYPE )
      w.add_update( UPD_TYPE_CORRECTION );
    else if ( msg_type == MD_VERIFY_TYPE )
      w.add_update( UPD_TYPE_VERIFY );
  }
  if ( seqno != 0 )
    w.add_seq_num( seqno );
  w.add_msg_key()
   .name( subj, slen )
   .name_type( NAME_TYPE_RIC )
   .end_msg_key();
  RwfFieldListWriter & fl = w.add_field_list();
  if ( flist != 0 )
    fl.add_flist( flist );
  status = w.err;
  if ( status == 0 )
    status = fl.convert_msg( *m, true );
  if ( status == 0 )
    w.end_msg();
  if ( status == 0 )
    status = w.err;
  if ( status == 0 ) {
    CatPtr p( mem.str_make( this->feed_len + 5 + slen + 1 ) );
    p.b( this->feed, this->feed_len )
     .b( ".REC.", 5 )
     .b( subj, slen ).end();
    uint32_t h = kv_crc_c( p.start, p.len(), 0 );
    EvPublish pub( p.start, p.len(), NULL, 0, w.buf, w.off,
                   this->sub_route, *this, h, RWF_MSG_TYPE_ID );
    this->sub_route.forward_msg( pub, NULL );
    this->msg_count++;
  }
  else {
    fprintf( stderr, "\"%s\", conversion error %d, subj %s\n", this->fn, status,
             subj );
  }

  return true;
}

void TestReplay::write( void ) noexcept {}
void TestReplay::read( void ) noexcept {}
void TestReplay::process( void ) noexcept {}
void TestReplay::release( void ) noexcept {}
void TestReplay::on_write_ready( void ) noexcept {}
bool TestReplay::on_msg( EvPublish & ) noexcept { return true; }
