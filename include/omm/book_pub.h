#ifndef __rai_omm__book_pub_h__
#define __rai_omm__book_pub_h__

#include <omm/ev_omm.h>

namespace rai {
using namespace md;
using namespace kv;
namespace omm {

/* Synthetic MARKET_BY_ORDER (7) / MARKET_BY_PRICE (8) feed.  One order
 * book per subject; the sector picks the encoding:
 *   <svc>.MBO.<ric>  -> Map keyed by ORDER_ID, one entry per order
 *   <svc>.MBP.<ric>  -> Map keyed by "<price><A|B>", one entry per level
 *                       aggregated from the same order model
 * Wire shape follows the ETA examples
 * (Real-Time-SDK/Cpp-C/Eta/Applications/Examples/Common/
 *  rsslMarketByOrderItems.c, rsslMarketByPriceItems.c): Map key type
 * BUFFER, keyFieldId ORDER_ID, summary FieldList on the first refresh part
 * only, ADD/UPDATE/DELETE entries, delete entries carry no data. */

enum BookSide {
  BOOK_BID = 1, /* ORDER_SIDE enum */
  BOOK_ASK = 2
};

struct BookOrder {
  uint32_t id;      /* ORDER_ID key, unique within a book */
  int32_t  prc;     /* price in ticks, ORDER_PRC = prc * 10^-2 */
  uint32_t size;    /* ORDER_SIZE */
  uint8_t  side;    /* BOOK_BID / BOOK_ASK */
  uint8_t  mmid;    /* MKT_MKR_ID index */
  uint64_t time_ms; /* QUOTIM_MS */
};

struct BookEvent {
  uint8_t   action;   /* RwfMapAction of the order (MBO) */
  BookOrder ord;      /* the order after the change (before, for delete) */
};

struct BookLevel {     /* MBP aggregate of one price / side */
  int32_t  prc;
  uint8_t  side;
  uint32_t acc_size,   /* ACC_SIZE */
           no_ord;     /* NO_ORD */
  uint64_t time_ms;    /* LV_TIM_MS */
};

static const uint32_t BOOK_MAX_ORDERS = 64,
                      BOOK_MAX_EVENTS = 16,
                      BOOK_DEPTH      = 5;   /* levels per side kept */

struct BookRoute {
  uint64_t  seqno,
            rand_state;
  int32_t   mid;                        /* mid price ticks */
  uint32_t  next_id,
            order_cnt,
            event_cnt;
  BookOrder orders[ BOOK_MAX_ORDERS ];
  BookEvent events[ BOOK_MAX_EVENTS ];  /* last tick's changes */
  BookLevel before[ BOOK_MAX_EVENTS ];  /* MBP: levels before the tick */
  uint32_t  before_cnt;
  uint8_t   domain;                     /* MARKET_BY_ORDER / MARKET_BY_PRICE */
  bool      is_active;
  uint32_t  hash;
  uint16_t  len;
  char      value[ 2 ];

  void     init( uint64_t cur_ns,  uint8_t domain ) noexcept;
  uint32_t rand( void ) noexcept;
  uint32_t rand( uint32_t n ) { return this->rand() % n; }
  int32_t  best( uint8_t side ) noexcept;
  bool     level( int32_t prc,  uint8_t side,  BookLevel &lv ) noexcept;
  void     note_level( int32_t prc,  uint8_t side ) noexcept;
  void     add_order( uint8_t side,  int32_t prc,  uint32_t size,
                      uint64_t ms ) noexcept;
  void     del_order( uint32_t i,  uint64_t ms ) noexcept;
  void     upd_order( uint32_t i,  uint32_t size,  uint64_t ms ) noexcept;
  void     tick( uint64_t cur_ns ) noexcept;  /* one random book change */
};

struct BookPublish : public EvSocket, public RouteNotify {
  EvPoll        & poll;
  RoutePublish  & sub_route;
  OmmDict       & dict;
  OmmSourceDB   & source_db;
  RouteVec<BookRoute> book_tab;
  uint32_t        tick_ms,       /* timer period */
                  part_entries;  /* refresh entries per part, 0 = one part */

  void * operator new( size_t, void *ptr ) { return ptr; }
  BookPublish( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept;

  void start( void ) noexcept;
  /* RouteNotify */
  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  virtual void on_resub( kv::NotifySub &sub ) noexcept;
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;

  void initial( const char *reply,  size_t reply_len,  OmmSource *src,
                const char *ric,  size_t ric_len,  BookRoute *rt,
                bool is_solicited ) noexcept;
  void update( OmmSource *src,  const char *ric,  size_t ric_len,
               BookRoute *rt ) noexcept;
  /* encoders, shared by refresh (all orders) and update (events) */
  void add_summary( RwfMapWriter &map,  BookRoute *rt,  const char *ric,
                    size_t ric_len ) noexcept;
  void add_mbo_order( RwfMapWriter &map,  RwfMapAction action,
                      BookOrder &o ) noexcept;
  void add_mbp_level( RwfMapWriter &map,  RwfMapAction action,
                      BookLevel &lv ) noexcept;
  void publish( const char *reply,  size_t reply_len,  BookRoute *rt,
                RwfMsgWriter &msg ) noexcept;
  /* EvSocket */
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual bool timer_expire( uint64_t tid,  uint64_t eid ) noexcept;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void on_write_ready( void ) noexcept;
};

}
}

#endif
