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
#include <omm/book_pub.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

static const MDFid ORDER_ID_FID = 3426;
static const char * mkt_mkr[ 4 ] = { "MM01", "MM02", "MM03", "MM04" };

/* ---- book model ---- */

void
BookRoute::init( uint64_t cur_ns,  uint8_t dom ) noexcept
{
  this->seqno      = 0;
  this->rand_state = ( (uint64_t) this->hash << 32 ) | 0x9e3779b97f4a7c15ULL;
  this->mid        = 1000 + (int32_t) ( this->hash % 50000 ); /* 10.00 .. */
  this->next_id    = 1000000 + ( this->hash % 1000 ) * 1000;
  this->order_cnt  = 0;
  this->event_cnt  = 0;
  this->before_cnt = 0;
  this->domain     = dom;
  this->is_active  = false;

  uint64_t ms = ( cur_ns / 1000000 ) % 86400000;
  for ( uint32_t i = 1; i <= BOOK_DEPTH; i++ ) {
    uint32_t n = 1 + this->rand( 3 );
    for ( uint32_t j = 0; j < n; j++ )
      this->add_order( BOOK_BID, this->mid - (int32_t) i,
                       ( 1 + this->rand( 20 ) ) * 100, ms );
    n = 1 + this->rand( 3 );
    for ( uint32_t j = 0; j < n; j++ )
      this->add_order( BOOK_ASK, this->mid + (int32_t) i,
                       ( 1 + this->rand( 20 ) ) * 100, ms );
  }
  this->event_cnt  = 0; /* the initial book is the refresh, not events */
  this->before_cnt = 0;
}

uint32_t
BookRoute::rand( void ) noexcept /* xorshift64* */
{
  uint64_t x = this->rand_state;
  x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
  this->rand_state = x;
  return (uint32_t) ( ( x * 0x2545f4914f6cdd1dULL ) >> 32 );
}

int32_t
BookRoute::best( uint8_t side ) noexcept
{
  int32_t b = 0;
  bool    first = true;
  for ( uint32_t i = 0; i < this->order_cnt; i++ ) {
    BookOrder & o = this->orders[ i ];
    if ( o.side != side )
      continue;
    if ( first || ( side == BOOK_BID ? o.prc > b : o.prc < b ) ) {
      b     = o.prc;
      first = false;
    }
  }
  return first ? ( side == BOOK_BID ? this->mid - 1 : this->mid + 1 ) : b;
}

/* aggregate a price level; false if no orders there */
bool
BookRoute::level( int32_t prc,  uint8_t side,  BookLevel &lv ) noexcept
{
  lv.prc      = prc;
  lv.side     = side;
  lv.acc_size = 0;
  lv.no_ord   = 0;
  lv.time_ms  = 0;
  for ( uint32_t i = 0; i < this->order_cnt; i++ ) {
    BookOrder & o = this->orders[ i ];
    if ( o.prc == prc && o.side == side ) {
      lv.acc_size += o.size;
      lv.no_ord++;
      if ( o.time_ms > lv.time_ms )
        lv.time_ms = o.time_ms;
    }
  }
  return lv.no_ord != 0;
}

/* remember a level's state before this tick touches it (MBP diff) */
void
BookRoute::note_level( int32_t prc,  uint8_t side ) noexcept
{
  for ( uint32_t i = 0; i < this->before_cnt; i++ )
    if ( this->before[ i ].prc == prc && this->before[ i ].side == side )
      return;
  if ( this->before_cnt < BOOK_MAX_EVENTS )
    this->level( prc, side, this->before[ this->before_cnt++ ] );
}

void
BookRoute::add_order( uint8_t side,  int32_t prc,  uint32_t size,
                      uint64_t ms ) noexcept
{
  if ( this->order_cnt >= BOOK_MAX_ORDERS ||
       this->event_cnt >= BOOK_MAX_EVENTS )
    return;
  this->note_level( prc, side );
  BookOrder & o = this->orders[ this->order_cnt++ ];
  o.id      = this->next_id++;
  o.prc     = prc;
  o.size    = size;
  o.side    = side;
  o.mmid    = (uint8_t) this->rand( 4 );
  o.time_ms = ms;
  BookEvent & ev = this->events[ this->event_cnt++ ];
  ev.action = MAP_ADD_ENTRY;
  ev.ord    = o;
}

void
BookRoute::del_order( uint32_t i,  uint64_t ms ) noexcept
{
  if ( i >= this->order_cnt || this->event_cnt >= BOOK_MAX_EVENTS )
    return;
  BookOrder & o = this->orders[ i ];
  this->note_level( o.prc, o.side );
  BookEvent & ev = this->events[ this->event_cnt++ ];
  ev.action      = MAP_DELETE_ENTRY;
  ev.ord         = o;
  ev.ord.time_ms = ms;
  this->orders[ i ] = this->orders[ --this->order_cnt ];
}

void
BookRoute::upd_order( uint32_t i,  uint32_t size,  uint64_t ms ) noexcept
{
  if ( i >= this->order_cnt || this->event_cnt >= BOOK_MAX_EVENTS )
    return;
  BookOrder & o = this->orders[ i ];
  this->note_level( o.prc, o.side );
  o.size    = size;
  o.time_ms = ms;
  BookEvent & ev = this->events[ this->event_cnt++ ];
  ev.action = MAP_UPDATE_ENTRY;
  ev.ord    = o;
}

/* one tick: size change 45%, cancel 20%, new order 20%, mid shift 15%
 * (the shift drops the levels that fell out of depth and seeds the new
 * best level, several entries in one update) */
void
BookRoute::tick( uint64_t cur_ns ) noexcept
{
  uint64_t ms = ( cur_ns / 1000000 ) % 86400000;
  uint32_t r  = this->rand( 100 );

  this->event_cnt  = 0;
  this->before_cnt = 0;
  this->seqno++;

  if ( r < 45 && this->order_cnt > 0 ) {
    this->upd_order( this->rand( this->order_cnt ),
                     ( 1 + this->rand( 20 ) ) * 100, ms );
  }
  else if ( r < 65 && this->order_cnt > 4 ) {
    this->del_order( this->rand( this->order_cnt ), ms );
  }
  else if ( r < 85 && this->order_cnt + 2 < BOOK_MAX_ORDERS ) {
    uint8_t side = (uint8_t) ( 1 + this->rand( 2 ) );
    int32_t prc  = ( side == BOOK_BID ) ?
                   this->mid - 1 - (int32_t) this->rand( BOOK_DEPTH ) :
                   this->mid + 1 + (int32_t) this->rand( BOOK_DEPTH );
    this->add_order( side, prc, ( 1 + this->rand( 20 ) ) * 100, ms );
  }
  else {
    int32_t dir = ( this->rand( 2 ) == 0 ) ? -1 : 1;
    if ( this->mid + dir <= (int32_t) BOOK_DEPTH + 1 )
      dir = 1;
    this->mid += dir;
    /* the far side loses its deepest level */
    for ( uint32_t i = 0; i < this->order_cnt; ) {
      BookOrder & o = this->orders[ i ];
      bool out = ( o.side == BOOK_BID ) ?
                 o.prc < this->mid - (int32_t) BOOK_DEPTH :
                 o.prc > this->mid + (int32_t) BOOK_DEPTH;
      if ( out && this->event_cnt + 3 < BOOK_MAX_EVENTS )
        this->del_order( i, ms ); /* compacts, i stays */
      else
        i++;
    }
    /* the near side gets a new best */
    uint8_t side = ( dir > 0 ) ? BOOK_BID : BOOK_ASK;
    int32_t prc  = ( dir > 0 ) ? this->mid - 1 : this->mid + 1;
    uint32_t n   = 1 + this->rand( 2 );
    for ( uint32_t j = 0; j < n; j++ )
      this->add_order( side, prc, ( 1 + this->rand( 20 ) ) * 100, ms );
  }
}

/* ---- publisher ---- */

BookPublish::BookPublish( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept
           : EvSocket( p, p.register_type( "omm_book_pub" ) ),
             RouteNotify( p.sub_route ), poll( p ), sub_route( p.sub_route ),
             dict( d ), source_db( db ), tick_ms( 250 ), part_entries( 0 )
{
  this->sock_opts = OPT_NO_POLL;
}

void
BookPublish::start( void ) noexcept
{
  int sfd = this->poll.get_null_fd();
  this->PeerData::init_peer( this->poll.get_next_id(), sfd, -1, NULL,
                             "omm_book_pub" );
  this->PeerData::set_name( "omm_book_pub", 12 );
  this->poll.add_sock( this );
  this->sub_route.add_route_notify( *this );
  this->poll.timer.add_timer_millis( this->fd, this->tick_ms, 1, 0 );
}

void
BookPublish::on_sub( NotifySub &sub ) noexcept
{
  if ( this->dict.rdm_dict == NULL ) {
    fprintf( stderr, "No dictionary, sub %.*s\n",
             (int) sub.subject_len, sub.subject );
    return;
  }
  const char * ric     = sub.subject;
  size_t       ric_len = sub.subject_len;
  OmmSource  * src;
  uint8_t      domain  = 0;

  /* match_sub strips <svc>.<sector>. and returns the sector's domain */
  if ( (src = this->source_db.match_sub( ric, ric_len, domain, 0 )) == NULL )
    return;
  if ( domain != MARKET_BY_ORDER_DOMAIN && domain != MARKET_BY_PRICE_DOMAIN )
    return;

  RouteLoc    loc;
  BookRoute * rt = this->book_tab.upsert( sub.subj_hash, sub.subject,
                                          sub.subject_len, loc );
  if ( loc.is_new )
    rt->init( this->poll.now_ns, domain );
  if ( ! rt->is_active ) {
    printf( "start book %.*s\n", (int) sub.subject_len, sub.subject );
    rt->is_active = true;
  }
  if ( sub.is_notify_initial() )
    this->initial( sub.reply, sub.reply_len, src, ric, ric_len, rt, true );
}

void
BookPublish::on_resub( NotifySub &sub ) noexcept
{
  if ( sub.is_notify_initial() )
    this->on_sub( sub );
}

void
BookPublish::on_unsub( NotifySub &sub ) noexcept
{
  if ( sub.sub_count == 0 ) {
    RouteLoc    loc;
    BookRoute * rt = this->book_tab.find( sub.subj_hash, sub.subject,
                                          sub.subject_len, loc );
    if ( rt != NULL && rt->is_active ) {
      rt->is_active = false;
      printf( "stop book %.*s\n", (int) sub.subject_len, sub.subject );
    }
  }
}

/* ---- encoders ---- */

void
BookPublish::add_summary( RwfMapWriter &map,  BookRoute *,
                          const char *ric,  size_t ric_len ) noexcept
{
  time_t t = this->poll.now_ns / ( (uint64_t) 1000 * 1000 * 1000 );
  struct tm tm;
  MDDate date;
  localtime_r( &t, &tm );
  date.year = tm.tm_year + 1900;
  date.mon  = tm.tm_mon + 1;
  date.day  = tm.tm_mday;

  map.add_summary_field_list()
     .append_uint  ( "PROD_PERM" , 1 )
     .append_string( "DSPLY_NAME", 10, ric, ric_len )
     .append_uint  ( "CURRENCY"  , 840 ) /* USD */
     .append_uint  ( "MKT_ST_IND", 20 )  /* BBO, as the ETA example */
     .append_date  ( "ACTIV_DATE", date )
     .append_uint  ( "TRD_UNITS" , 2 )   /* 2 decimal places */
     .append_uint  ( "BOOK_STATE", 1 )   /* Normal */
     .end_summary();
}

void
BookPublish::add_mbo_order( RwfMapWriter &map,  RwfMapAction action,
                            BookOrder &o ) noexcept
{
  char   id[ 16 ];
  size_t id_len = ::snprintf( id, sizeof( id ), "%u", o.id );
  MDReference key( id, id_len, MD_OPAQUE, md_endian );

  if ( action == MAP_DELETE_ENTRY ) {
    map.add_delete_entry( key );
    return;
  }
  MDDecimal prc ( o.prc, MD_DEC_LOGn10_2 ),
            size( o.size, MD_DEC_INTEGER );
  RwfFieldListWriter & fl = map.add_field_list( action, key );
  fl.append_decimal( "ORDER_PRC" , prc )
    .append_decimal( "ORDER_SIZE", size )
    .append_uint   ( "QUOTIM_MS" , o.time_ms );
  if ( action == MAP_ADD_ENTRY ) /* side and maker are fixed for an order */
    fl.append_uint  ( "ORDER_SIDE", o.side )
      .append_string( "MKT_MKR_ID", mkt_mkr[ o.mmid & 3 ] );
  fl.end_entry();
}

void
BookPublish::add_mbp_level( RwfMapWriter &map,  RwfMapAction action,
                            BookLevel &lv ) noexcept
{
  char   pp[ 32 ];
  size_t pp_len = ::snprintf( pp, sizeof( pp ), "%d.%02d%c",
                              lv.prc / 100, lv.prc % 100,
                              lv.side == BOOK_BID ? 'B' : 'A' );
  MDReference key( pp, pp_len, MD_OPAQUE, md_endian );

  if ( action == MAP_DELETE_ENTRY ) {
    map.add_delete_entry( key );
    return;
  }
  MDDecimal prc( lv.prc, MD_DEC_LOGn10_2 ),
            acc( lv.acc_size, MD_DEC_INTEGER );
  RwfFieldListWriter & fl = map.add_field_list( action, key );
  fl.append_decimal( "ORDER_PRC", prc )
    .append_decimal( "ACC_SIZE" , acc )
    .append_uint   ( "NO_ORD"   , lv.no_ord )
    .append_uint   ( "LV_TIM_MS", lv.time_ms )
    .append_uint   ( "QUOTIM_MS", lv.time_ms );
  if ( action == MAP_ADD_ENTRY )
    fl.append_uint( "ORDER_SIDE", lv.side );
  fl.end_entry();
}

void
BookPublish::publish( const char *reply,  size_t reply_len,  BookRoute *rt,
                      RwfMsgWriter &msg ) noexcept
{
  if ( msg.err != 0 ) {
    fprintf( stderr, "book encode %.*s err %d\n", (int) rt->len, rt->value,
             msg.err );
    return;
  }
  if ( reply_len == 0 ) {
    reply     = rt->value;
    reply_len = rt->len;
  }
  EvPublish pub( reply, reply_len, NULL, 0, msg.buf, msg.off,
                 this->sub_route, *this, rt->hash, RWF_MSG_TYPE_ID );
  this->sub_route.forward_msg( pub, NULL );
}

/* refresh: the whole book, MBO one entry per order, MBP one per level;
 * part_entries != 0 splits it (CLEAR_CACHE + summary on part 0,
 * REFRESH_COMPLETE on the last, part numbers throughout) */
void
BookPublish::initial( const char *reply,  size_t reply_len,  OmmSource *src,
                      const char *ric,  size_t ric_len,  BookRoute *rt,
                      bool is_solicited ) noexcept
{
  char      buf[ 16 * 1024 ];
  MDMsgMem  mem;
  BookLevel lvls[ BOOK_MAX_ORDERS ];
  uint32_t  total = 0;
  bool      is_mbo = ( rt->domain == MARKET_BY_ORDER_DOMAIN );

  if ( is_mbo )
    total = rt->order_cnt;
  else { /* distinct price levels */
    for ( uint32_t i = 0; i < rt->order_cnt; i++ ) {
      BookOrder & o = rt->orders[ i ];
      uint32_t j = 0;
      for ( ; j < total; j++ )
        if ( lvls[ j ].prc == o.prc && lvls[ j ].side == o.side )
          break;
      if ( j == total )
        rt->level( o.prc, o.side, lvls[ total++ ] );
    }
  }
  uint32_t per    = ( this->part_entries == 0 || this->part_entries >= total )
                    ? ( total == 0 ? 1 : total ) : this->part_entries,
           nparts = ( total + per - 1 ) / per;
  if ( nparts == 0 )
    nparts = 1;

  for ( uint32_t part = 0, k = 0; part < nparts; part++ ) {
    bool last = ( part + 1 == nparts );
    RwfMsgWriter msg( mem, this->dict.rdm_dict, buf, sizeof( buf ),
                      REFRESH_MSG_CLASS, (RdmDomainType) rt->domain,
                      rt->hash );
    if ( part == 0 )
      msg.set( X_CLEAR_CACHE );
    if ( is_solicited )
      msg.set( X_SOLICITED );
    if ( last )
      msg.set( X_REFRESH_COMPLETE );
    if ( nparts > 1 )
      msg.add_part_num( (uint16_t) part );
    msg.add_seq_num( (uint32_t) rt->seqno )
       .add_state( DATA_STATE_OK, STREAM_STATE_OPEN,
                   last ? "Item Refresh Completed" : "Item Refresh In Progress" )
       .add_qos( QOS_TIME_REALTIME, QOS_RATE_TICK_BY_TICK, false )
       .add_msg_key()
         .service_id( src->service_id )
         .name( ric, ric_len )
         .name_type( NAME_TYPE_RIC )
       .end_msg_key();
    RwfMapWriter & map = msg.add_map( MD_OPAQUE );
    map.set_key_fid( ORDER_ID_FID )
       .set_hint_cnt( total )
       .set_container_type( W_FIELD_LIST );
    if ( part == 0 )
      this->add_summary( map, rt, ric, ric_len );
    for ( uint32_t n = 0; n < per && k < total; n++, k++ ) {
      if ( is_mbo )
        this->add_mbo_order( map, MAP_ADD_ENTRY, rt->orders[ k ] );
      else
        this->add_mbp_level( map, MAP_ADD_ENTRY, lvls[ k ] );
    }
    msg.end_msg();
    if ( is_omm_debug )
      printf( "pub %s refresh %.*s part %u/%u %u entries\n",
              is_mbo ? "mbo" : "mbp", (int) rt->len, rt->value, part + 1,
              nparts, total );
    this->publish( reply, reply_len, rt, msg );
    mem.reuse();
  }
}

/* update: MBO replays the tick's events; MBP diffs the touched levels
 * against their state before the tick (absent -> ADD, present both ->
 * UPDATE, gone -> DELETE) */
void
BookPublish::update( OmmSource *src,  const char *ric,  size_t ric_len,
                     BookRoute *rt ) noexcept
{
  char     buf[ 8 * 1024 ];
  MDMsgMem mem;
  bool     is_mbo = ( rt->domain == MARKET_BY_ORDER_DOMAIN );
  uint32_t cnt    = 0;

  RwfMsgWriter msg( mem, this->dict.rdm_dict, buf, sizeof( buf ),
                    UPDATE_MSG_CLASS, (RdmDomainType) rt->domain, rt->hash );
  msg.add_seq_num( (uint32_t) rt->seqno )
     .add_msg_key()
       .service_id( src->service_id )
       .name( ric, ric_len )
       .name_type( NAME_TYPE_RIC )
     .end_msg_key();
  msg.add_update( UPD_TYPE_QUOTE );
  RwfMapWriter & map = msg.add_map( MD_OPAQUE );
  map.set_key_fid( ORDER_ID_FID )
     .set_container_type( W_FIELD_LIST );

  if ( is_mbo ) {
    for ( uint32_t i = 0; i < rt->event_cnt; i++ ) {
      BookEvent & ev = rt->events[ i ];
      this->add_mbo_order( map, (RwfMapAction) ev.action, ev.ord );
      cnt++;
    }
  }
  else {
    for ( uint32_t i = 0; i < rt->before_cnt; i++ ) {
      BookLevel & was = rt->before[ i ];
      BookLevel   now;
      bool        exists = rt->level( was.prc, was.side, now );
      if ( was.no_ord == 0 && exists )
        this->add_mbp_level( map, MAP_ADD_ENTRY, now );
      else if ( was.no_ord != 0 && exists )
        this->add_mbp_level( map, MAP_UPDATE_ENTRY, now );
      else if ( was.no_ord != 0 && ! exists )
        this->add_mbp_level( map, MAP_DELETE_ENTRY, was );
      else
        continue;
      cnt++;
    }
  }
  if ( cnt == 0 )
    return;
  msg.end_msg();
  if ( is_omm_debug )
    printf( "pub %s update %.*s %u entries\n", is_mbo ? "mbo" : "mbp",
            (int) rt->len, rt->value, cnt );
  this->publish( NULL, 0, rt, msg );
}

bool
BookPublish::timer_expire( uint64_t, uint64_t ) noexcept
{
  RouteLoc    loc;
  BookRoute * rt;
  for ( rt = this->book_tab.first( loc ); rt != NULL;
        rt = this->book_tab.next( loc ) ) {
    if ( ! rt->is_active )
      continue;
    const char * ric     = rt->value;
    size_t       ric_len = rt->len;
    OmmSource  * src;
    uint8_t      domain  = 0;

    if ( (src = this->source_db.match_sub( ric, ric_len, domain, 0 )) == NULL )
      continue;
    rt->tick( this->poll.now_ns );
    /* an unsolicited refresh now and then, as a real feed resyncs */
    if ( rt->seqno % 400 == 0 )
      this->initial( NULL, 0, src, ric, ric_len, rt, false );
    else
      this->update( src, ric, ric_len, rt );
  }
  return true;
}

void BookPublish::on_psub( NotifyPattern & ) noexcept {}
void BookPublish::on_punsub( NotifyPattern & ) noexcept {}
void BookPublish::write( void ) noexcept {}
void BookPublish::read( void ) noexcept {}
void BookPublish::process( void ) noexcept {}
void BookPublish::release( void ) noexcept {}
void BookPublish::on_write_ready( void ) noexcept {}
bool BookPublish::on_msg( EvPublish & ) noexcept { return true; }
