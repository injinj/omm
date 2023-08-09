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
#include <omm/test_pub.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

void
TestPublish::start( void ) noexcept
{
  this->omm_sv.sub_route.add_route_notify( *this );
  this->poll.timer.add_timer_seconds( *this, 5, 1, 0 );
}

void
TestPublish::add_test_source( const char *feed_name,
                              uint32_t service_id ) noexcept
{
  static const char * dict[ 2 ] = { "RWFFld", "RWFEnum" };
  static uint8_t cap[ 4 ] = { LOGIN_DOMAIN, SOURCE_DOMAIN, DICTIONARY_DOMAIN,
                              MARKET_PRICE_DOMAIN };
  static RwfQos  qos      = { QOS_TIME_REALTIME, QOS_RATE_TICK_BY_TICK, 0, 0, 0 };

  char         buf[ 1024 ];
  MDMsgMem     mem;
  RwfMapWriter map( mem, this->omm_sv.dict.rdm_dict, buf, sizeof( buf ) );
  RwfState     state = { STREAM_STATE_OPEN, DATA_STATE_OK, 0, { "OK", 2 } };

  RwfFilterListWriter
    & fil = map.add_filter_list( MAP_ADD_ENTRY, service_id, MD_UINT );

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_INFO_ID )
     .append_string( NAME        , feed_name )
     .append_string( VEND        , "Test" )
     .append_uint  ( IS_SRC      , 0 )
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

  char link_name[ 64 ], link_name2[ 64 ];
  CatPtr p( link_name ), q( link_name2 );
  p.s( feed_name ).s( " " ).s( "link" ).end();
  q.s( feed_name ).s( " " ).s( "link 2" ).end();

  RwfMapWriter &lnk = fil.add_map( FILTER_SET_ENTRY, DIR_SVC_LINK_ID );
  lnk.set_key_type( MD_STRING )
     .add_element_list( MAP_ADD_ENTRY, link_name )
       .append_uint  ( TYPE      , LINK_INTERACTIVE ) /* 1 = Interactive, 2 = Broadcast */
       .append_uint  ( LINK_STATE, LINK_UP )          /* 1 = Up, 0 = Down */
       .append_uint  ( LINK_CODE , LINK_OK )          /* 1 = 0k, 2 = recovery start, 3 = recovery completed */
       .append_string( TEXT      , "Link state is up" )
     .end_entry();
  lnk.add_element_list( MAP_ADD_ENTRY, link_name2 )
       .append_uint  ( TYPE      , LINK_INTERACTIVE ) /* 1 = Interactive, 2 = Broadcast */
       .append_uint  ( LINK_STATE, LINK_UP )          /* 1 = Up, 0 = Down */
       .append_uint  ( LINK_CODE , LINK_OK )          /* 1 = 0k, 2 = recovery start, 3 = recovery completed */
       .append_string( TEXT      , "Link2 state is up" )
     .end_entry();
  map.end_map();

  RwfMsg *m =
    RwfMsg::unpack_map( map.buf, 0, map.off, RWF_MAP_TYPE_ID, NULL, mem );
  if ( is_omm_debug ) {
    MDOutput mout;
    if ( m != NULL )
      m->print( &mout );
  }
  this->omm_sv.add_source( *m );
}

void
TestRoute::init( uint64_t cur_ns ) noexcept
{
  this->seqno = 0;
  this->ask.set( 1025, MD_DEC_LOGn10_2 );
  this->bid.set( 1050, MD_DEC_LOGn10_2 );
  this->ask_size.set( 100, MD_DEC_INTEGER );
  this->bid_size.set( 50, MD_DEC_INTEGER );
  this->is_active = false;
  this->update_time( cur_ns );
}

void
TestRoute::update_time( uint64_t cur_ns ) noexcept
{
  time_t t = cur_ns / ( (uint64_t) 1000 * 1000 * 1000 );
  struct tm tm;
  localtime_r( &t, &tm );

  this->time.hour       = tm.tm_hour;
  this->time.minute     = tm.tm_min;
  this->time.sec        = tm.tm_sec;
  this->time.fraction   = ( cur_ns / ( (uint64_t) 1000 * 1000 ) ) % 1000;
  this->time.resolution = MD_RES_MILLISECS;

  this->date.year = tm.tm_year + 1900;
  this->date.mon  = tm.tm_mon + 1;
  this->date.day  = tm.tm_mday;
}

void
TestRoute::update( uint64_t cur_ns ) noexcept
{
  this->ask.ival += 25;
  this->bid.ival += 25;
  this->ask_size.ival += 10;
  this->bid_size.ival += 5;
  this->update_time( cur_ns );
  this->seqno++;
}

static const char dsply_name[] = "DSPLY_NAME",
                  bid_size[]   = "BIDSIZE",
                  ask_size[]   = "ASKSIZE",
                  bid[]        = "BID",
                  ask[]        = "ASK",
                  timact[]     = "TIMACT",
                  trade_date[] = "TRADE_DATE";
void
TestPublish::on_sub( NotifySub &sub ) noexcept
{
  if ( this->dict.rdm_dict == NULL ) {
    fprintf( stderr, "No dictionary, sub %.*s\n",
             (int) sub.subject_len, sub.subject );
    return;
  }
  const char * ric     = sub.subject;
  size_t       ric_len = sub.subject_len;
  OmmSource  * src;
  uint8_t      domain  = MARKET_PRICE_DOMAIN;

  if ( (src = this->source_db.match_sub( ric, ric_len, domain, 0 )) == NULL )
    return;

  RouteLoc    loc;
  TestRoute * rt = this->test_tab.upsert( sub.subj_hash, sub.subject,
                                          sub.subject_len, loc );
  if ( loc.is_new )
    rt->init( this->poll.now_ns );
  if ( ! rt->is_active ) {
    printf( "start sub %.*s\n", (int) sub.subject_len, sub.subject );
    rt->update( this->poll.now_ns );
    rt->is_active = true;
  }

  if ( sub.is_notify_initial() )
    this->initial( sub.reply, sub.reply_len, src, ric, ric_len, domain, rt,
                   true );
}

void
TestPublish::on_resub( NotifySub &sub ) noexcept
{
  if ( sub.is_notify_initial() )
    this->on_sub( sub );
}

void
TestPublish::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.sub_count == 0 ) {
    RouteLoc    loc;
    TestRoute * rt = this->test_tab.find( sub.subj_hash, sub.subject,
                                          sub.subject_len, loc );
    if ( rt != NULL )
      rt->is_active = false;
    printf( "stop sub %.*s\n", (int) sub.subject_len, sub.subject );
  }
}

void
TestPublish::initial( const char *reply,  size_t reply_len,  OmmSource *src,
                      const char *ric,  size_t ric_len,  uint8_t domain,
                      TestRoute *rt,  bool is_solicited ) noexcept
{
  char buf[ 1024 ];
  MDMsgMem mem;
  RwfMsgWriter msg( mem, this->dict.rdm_dict, buf, sizeof( buf ),
                    REFRESH_MSG_CLASS, (RdmDomainType) domain, rt->hash );
  if ( is_solicited )
    msg.set( X_CLEAR_CACHE, X_SOLICITED, X_REFRESH_COMPLETE );
  else
    msg.set( X_CLEAR_CACHE, X_REFRESH_COMPLETE );
  msg.add_seq_num( rt->seqno )
     .add_msg_key()
     .service_id( src->service_id )
     .name( ric, ric_len )
     .name_type( NAME_TYPE_RIC )
     .end_msg_key();
  msg.add_field_list()
     .add_flist( 90 )
     .append_string ( dsply_name , ric )
     .append_decimal( ask        , rt->ask )
     .append_decimal( bid        , rt->bid )
     .append_decimal( ask_size   , rt->ask_size )
     .append_decimal( bid_size   , rt->bid_size )
     .append_time   ( timact     , rt->time )
     .append_date   ( trade_date , rt->date )
     .end_msg();
  if ( reply_len == 0 ) {
    reply     = rt->value;
    reply_len = rt->len;
  }
  printf( "pub initial %.*s\n", (int) reply_len, reply );
  EvPublish pub( reply, reply_len, NULL, 0, msg.buf, msg.off,
                 this->poll.sub_route, this->omm_sv, rt->hash,
                 RWF_MSG_TYPE_ID );
  this->poll.sub_route.forward_msg( pub, NULL );
}

void
TestPublish::update( OmmSource *src,  const char *ric,  size_t ric_len,
                     uint8_t domain,  TestRoute *rt ) noexcept
{
  char buf[ 1024 ];
  MDMsgMem mem;
  RwfMsgWriter msg( mem, this->dict.rdm_dict, buf, sizeof( buf ),
                    UPDATE_MSG_CLASS, (RdmDomainType) domain, rt->hash );
  msg.add_seq_num( rt->seqno )
     .add_msg_key()
     .service_id( src->service_id )
     .name( ric, ric_len )
     .name_type( NAME_TYPE_RIC )
     .end_msg_key();
  msg.add_update( UPD_TYPE_QUOTE )
     .add_field_list()
     .add_flist( 90 )
     .append_decimal( ask        , rt->ask )
     .append_decimal( bid        , rt->bid )
     .append_decimal( ask_size   , rt->ask_size )
     .append_decimal( bid_size   , rt->bid_size )
     .append_time   ( timact     , rt->time )
     .append_date   ( trade_date , rt->date )
     .end_msg();
  printf( "pub update %.*s\n", (int) rt->len, rt->value );
  EvPublish pub( rt->value, rt->len, NULL, 0, msg.buf, msg.off,
                 this->poll.sub_route, this->omm_sv, rt->hash,
                 RWF_MSG_TYPE_ID );
  this->poll.sub_route.forward_msg( pub, NULL );
}

bool
TestPublish::timer_cb( uint64_t, uint64_t ) noexcept
{
  RouteLoc loc;
  TestRoute * rt;
  for ( rt = this->test_tab.first( loc ); rt != NULL;
        rt = this->test_tab.next( loc ) ) {
    if ( ! rt->is_active )
      continue;
    rt->update( this->poll.now_ns );

    const char * ric     = rt->value;
    size_t       ric_len = rt->len;
    OmmSource  * src;
    uint8_t      domain  = MARKET_PRICE_DOMAIN;

    if ( (src = this->source_db.match_sub( ric, ric_len, domain, 0 )) == NULL )
      continue;

    if ( rt->seqno % 30 == 0 )
      this->initial( NULL, 0, src, ric, ric_len, domain, rt, false );
    else
      this->update( src, ric, ric_len, domain, rt );
  }
  return true;
}

void
TestPublish::on_psub( NotifyPattern & ) noexcept
{
}

void
TestPublish::on_punsub( NotifyPattern & ) noexcept
{
}

