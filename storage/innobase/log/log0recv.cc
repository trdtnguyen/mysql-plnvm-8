/*****************************************************************************

Copyright (c) 1997, 2018, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2012, Facebook Inc.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file log/log0recv.cc
 Recovery

 Created 9/20/1997 Heikki Tuuri
 *******************************************************/

#include "ha_prototypes.h"

#include <my_aes.h>
#include <sys/types.h>

#include <array>
#include <iomanip>
#include <map>
#include <new>
#include <string>
#include <vector>

#include "arch0arch.h"
#include "log0recv.h"

#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "dict0dd.h"
#include "fil0fil.h"
#include "ha_prototypes.h"
#include "ibuf0ibuf.h"
#include "log0log.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "os0thread-create.h"
#include "page0cur.h"
#include "page0zip.h"
#include "trx0rec.h"
#include "trx0undo.h"
#include "ut0new.h"

#ifndef UNIV_HOTBACKUP
#include "buf0rea.h"
#include "row0merge.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "trx0purge.h"
#else /* !UNIV_HOTBACKUP */
/** This is set to false if the backup was originally taken with the
mysqlbackup --include regexp option: then we do not want to create tables in
directories which were not included */
bool meb_replay_file_ops = true;
#include "../meb/mutex.h"
#endif /* !UNIV_HOTBACKUP */

#if defined(UNIV_PMEMOBJ_LOG) || defined (UNIV_PMEMOBJ_WAL) || defined (UNIV_PMEMOBJ_PART_PL)
#include "mtr0types.h" 
#include "my_pmemobj.h"
extern PMEM_WRAPPER* gb_pmw;

//static bool IS_GLOBAL_HASHTABLE = true;
static bool IS_GLOBAL_HASHTABLE = false;
#endif //UNIV_PMEMOBJ_PART_PL and other log-based methods

std::list<space_id_t> recv_encr_ts_list;

/** Log records are stored in the hash table in chunks at most of this size;
this must be less than UNIV_PAGE_SIZE as it is stored in the buffer pool */
#define RECV_DATA_BLOCK_SIZE (MEM_MAX_ALLOC_IN_BUF - sizeof(recv_data_t))

/** Read-ahead area in applying log records to file pages */
#if defined (UNIV_PMEMOBJ_PART_PL)
//static const size_t RECV_READ_AHEAD_AREA = 16;
static const size_t RECV_READ_AHEAD_AREA = 64;
#else //original
static const size_t RECV_READ_AHEAD_AREA = 32;
#endif //UNIV_PMEMOBJ_PART_PL

/** The recovery system */
recv_sys_t *recv_sys = nullptr;

/** true when applying redo log records during crash recovery; false
otherwise.  Note that this is false while a background thread is
rolling back incomplete transactions. */
volatile bool recv_recovery_on;

#ifdef UNIV_HOTBACKUP
volatile bool is_online_redo_copy = true;
volatile lsn_t backup_redo_log_flushed_lsn;

extern bool meb_is_space_loaded(const space_id_t space_id);

/* Re-define mutex macros to use the Mutex class defined by the MEB
source. MEB calls the routines in "fil0fil.cc" in parallel and,
therefore, the mutex protecting the critical sections of the tablespace
memory cache must be included also in the MEB compilation of this
module. (For other modules the mutex macros are defined as no ops in the
MEB compilation in "meb/src/include/bh_univ.i".) */

#undef mutex_enter
#undef mutex_exit
#undef mutex_own
#undef mutex_validate

#define mutex_enter(M) recv_mutex.lock()
#define mutex_exit(M) recv_mutex.unlock()
#define mutex_own(M) 1
#define mutex_validate(M) 1

/* Re-define the mutex macros for the mutex protecting the critical
sections of the log subsystem using an object of the meb::Mutex class. */

meb::Mutex recv_mutex;
extern meb::Mutex log_mutex;
meb::Mutex apply_log_mutex;

#undef log_mutex_enter
#undef log_mutex_exit
#define log_mutex_enter() log_mutex.lock()
#define log_mutex_exit() log_mutex.unlock()

/** Print important values from a page header.
@param[in]	page	page */
void meb_print_page_header(const page_t *page) {
  ib::trace_1() << "space " << mach_read_from_4(page + FIL_PAGE_SPACE_ID)
                << " nr " << mach_read_from_4(page + FIL_PAGE_OFFSET) << " lsn "
                << mach_read_from_8(page + FIL_PAGE_LSN) << " type "
                << mach_read_from_2(page + FIL_PAGE_TYPE);
}
#endif /* UNIV_HOTBACKUP */

#ifndef UNIV_HOTBACKUP
PSI_memory_key mem_log_recv_page_hash_key;
PSI_memory_key mem_log_recv_space_hash_key;
#endif /* !UNIV_HOTBACKUP */

/** true when recv_init_crash_recovery() has been called. */
bool recv_needed_recovery;

/** true if buf_page_is_corrupted() should check if the log sequence
number (FIL_PAGE_LSN) is in the future.  Initially false, and set by
recv_recovery_from_checkpoint_start(). */
bool recv_lsn_checks_on;

/** If the following is true, the buffer pool file pages must be invalidated
after recovery and no ibuf operations are allowed; this becomes true if
the log record hash table becomes too full, and log records must be merged
to file pages already before the recovery is finished: in this case no
ibuf operations are allowed, as they could modify the pages read in the
buffer pool before the pages have been recovered to the up-to-date state.

true means that recovery is running and no operations on the log files
are allowed yet: the variable name is misleading. */
bool recv_no_ibuf_operations;

/** true When the redo log is being backed up */
bool recv_is_making_a_backup = false;

/** true when recovering from a backed up redo log file */
bool recv_is_from_backup = false;

#define buf_pool_get_curr_size() (5 * 1024 * 1024)

/** The following counter is used to decide when to print info on
log scan */
static ulint recv_scan_print_counter;

/** The type of the previous parsed redo log record */
static mlog_id_t recv_previous_parsed_rec_type;

/** The offset of the previous parsed redo log record */
static ulint recv_previous_parsed_rec_offset;

/** The 'multi' flag of the previous parsed redo log record */
static ulint recv_previous_parsed_rec_is_multi;

/** This many frames must be left free in the buffer pool when we scan
the log and store the scanned log records in the buffer pool: we will
use these free frames to read in pages when we start applying the
log records to the database.
This is the default value. If the actual size of the buffer pool is
larger than 10 MB we'll set this value to 512. */
ulint recv_n_pool_free_frames;

/** The maximum lsn we see for a page during the recovery process. If this
is bigger than the lsn we are able to scan up to, that is an indication that
the recovery failed and the database may be corrupt. */
static lsn_t recv_max_page_lsn;

#ifndef UNIV_HOTBACKUP
#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t recv_writer_thread_key;
#endif /* UNIV_PFS_THREAD */

/** Flag indicating if recv_writer thread is active. */
bool recv_writer_thread_active = false;
#endif /* !UNIV_HOTBACKUP */

/* prototypes */

#ifndef UNIV_HOTBACKUP

/** Reads a specified log segment to a buffer.
@param[in,out]	log		redo log
@param[in,out]	buf		buffer where to read
@param[in]	start_lsn	read area start
@param[in]	end_lsn		read area end */
static void recv_read_log_seg(log_t &log, byte *buf, lsn_t start_lsn,
                              lsn_t end_lsn);

/** Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static void recv_init_crash_recovery();
#endif /* !UNIV_HOTBACKUP */

/** Calculates the new value for lsn when more data is added to the log.
@param[in]	lsn		Old LSN
@param[in]	len		This many bytes of data is added, log block
                                headers not included
@return LSN after data addition */
lsn_t recv_calc_lsn_on_data_add(lsn_t lsn, uint64_t len) {
  ulint frag_len;
  uint64_t lsn_len;

  frag_len = (lsn % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;

  ut_ad(frag_len <
        OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);

  lsn_len = len;

  lsn_len +=
      (lsn_len + frag_len) /
      (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) *
      (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

  return (lsn + lsn_len);
}

/** Destructor */
MetadataRecover::~MetadataRecover() {
  for (auto &table : m_tables) {
    UT_DELETE(table.second);
  }
}

/** Get the dynamic metadata of a specified table, create a new one
if not exist
@param[in]	id	table id
@return the metadata of the specified table */
PersistentTableMetadata *MetadataRecover::getMetadata(table_id_t id) {
  PersistentTableMetadata *metadata = nullptr;
  PersistentTables::iterator iter = m_tables.find(id);

  if (iter == m_tables.end()) {
    metadata = UT_NEW_NOKEY(PersistentTableMetadata(id, 0));

    m_tables.insert(std::make_pair(id, metadata));
  } else {
    metadata = iter->second;
    ut_ad(metadata->get_table_id() == id);
  }

  ut_ad(metadata != nullptr);
  return (metadata);
}

/** Parse a dynamic metadata redo log of a table and store
the metadata locally
@param[in]	id	table id
@param[in]	version	table dynamic metadata version
@param[in]	ptr	redo log start
@param[in]	end	end of redo log
@retval ptr to next redo log record, nullptr if this log record
was truncated */
byte *MetadataRecover::parseMetadataLog(table_id_t id, uint64_t version,
                                        byte *ptr, byte *end) {
  if (ptr + 2 > end) {
    /* At least we should get type byte and another one byte
    for data, if not, it's an incomplete log */
    return (nullptr);
  }

  persistent_type_t type = static_cast<persistent_type_t>(ptr[0]);

  ut_ad(dict_persist->persisters != nullptr);

  Persister *persister = dict_persist->persisters->get(type);
  PersistentTableMetadata *metadata = getMetadata(id);

  bool corrupt;
  ulint consumed = persister->read(*metadata, ptr, end - ptr, &corrupt);

  if (corrupt) {
    recv_sys->found_corrupt_log = true;
  } else if (consumed != 0) {
    metadata->set_version(version);
  }

  if (consumed == 0) {
    return (nullptr);
  } else {
    return (ptr + consumed);
  }
}

/** Apply the collected persistent dynamic metadata to in-memory
table objects */
void MetadataRecover::apply() {
  PersistentTables::iterator iter;

  for (iter = m_tables.begin(); iter != m_tables.end(); ++iter) {
    table_id_t table_id = iter->first;
    PersistentTableMetadata *metadata = iter->second;
    dict_table_t *table;

    table = dd_table_open_on_id(table_id, nullptr, nullptr, false, true);

    /* If the table is nullptr, it might be already dropped */
    if (table == nullptr) {
      continue;
    }

    mutex_enter(&dict_sys->mutex);

    /* At this time, the metadata in DDTableBuffer has
    already been applied to table object, we can apply
    the latest status of metadata read from redo logs to
    the table now. We can read the dirty_status directly
    since it's in recovery phase */

    /* The table should be either CLEAN or applied BUFFERED
    metadata from DDTableBuffer just now */
    ut_ad(table->dirty_status.load() == METADATA_CLEAN ||
          table->dirty_status.load() == METADATA_BUFFERED);

    bool buffered = (table->dirty_status.load() == METADATA_BUFFERED);

    mutex_enter(&dict_persist->mutex);

    bool is_dirty = dict_table_apply_dynamic_metadata(table, metadata);

    if (is_dirty) {
      /* This table was not marked as METADATA_BUFFERED
      before the redo logs are applied, so it's not in
      the list */
      if (!buffered) {
        ut_ad(!table->in_dirty_dict_tables_list);

        UT_LIST_ADD_LAST(dict_persist->dirty_dict_tables, table);
      }

      table->dirty_status.store(METADATA_DIRTY);
      ut_d(table->in_dirty_dict_tables_list = true);
      ++dict_persist->num_dirty_tables;
    }

    mutex_exit(&dict_persist->mutex);
    mutex_exit(&dict_sys->mutex);

    dd_table_close(table, NULL, NULL, false);
  }
}

/** Creates the recovery system. */
void recv_sys_create() {
  if (recv_sys != nullptr) {
    return;
  }

  recv_sys = static_cast<recv_sys_t *>(ut_zalloc_nokey(sizeof(*recv_sys)));

  mutex_create(LATCH_ID_RECV_SYS, &recv_sys->mutex);
  mutex_create(LATCH_ID_RECV_WRITER, &recv_sys->writer_mutex);

  recv_sys->spaces = nullptr;
}

/** Resize the recovery parsing buffer upto log_buffer_size */
static bool recv_sys_resize_buf() {
  ut_ad(recv_sys->buf_len <= srv_log_buffer_size);

  /* If the buffer cannot be extended further, return false. */
  if (recv_sys->buf_len == srv_log_buffer_size) {
    ib::error(ER_IB_MSG_723, srv_log_buffer_size);
    return false;
  }

  /* Extend the buffer by double the current size with the resulting
  size not more than srv_log_buffer_size. */
  recv_sys->buf_len = ((recv_sys->buf_len * 2) >= srv_log_buffer_size)
                          ? srv_log_buffer_size
                          : recv_sys->buf_len * 2;

  /* Resize the buffer to the new size. */
  recv_sys->buf =
      static_cast<byte *>(ut_realloc(recv_sys->buf, recv_sys->buf_len));

  ut_ad(recv_sys->buf != nullptr);

  /* Return error and fail the recovery if not enough memory available */
  if (recv_sys->buf == nullptr) {
    ib::error(ER_IB_MSG_740);
    return false;
  }

  ib::info(ER_IB_MSG_739, recv_sys->buf_len);
  return true;
}

/** Free up recovery data structures. */
static void recv_sys_finish() {
  if (recv_sys->spaces != nullptr) {
    for (auto &space : *recv_sys->spaces) {
      if (space.second.m_heap != nullptr) {
        mem_heap_free(space.second.m_heap);
        space.second.m_heap = nullptr;
      }
    }

    UT_DELETE(recv_sys->spaces);
  }

#ifndef UNIV_HOTBACKUP
  ut_a(recv_sys->dblwr.pages.empty());

  if (!recv_sys->dblwr.deferred.empty()) {
    /* Free the pages that were not required for recovery. */
    for (auto &page : recv_sys->dblwr.deferred) {
      page.close();
    }
  }

  recv_sys->dblwr.deferred.clear();
#endif /* !UNIV_HOTBACKUP */

  ut_free(recv_sys->buf);
  ut_free(recv_sys->last_block_buf_start);
  UT_DELETE(recv_sys->metadata_recover);

  recv_sys->buf = nullptr;
  recv_sys->spaces = nullptr;
  recv_sys->metadata_recover = nullptr;
  recv_sys->last_block_buf_start = nullptr;
}

/** Release recovery system mutexes. */
void recv_sys_close() {
  if (recv_sys == nullptr) {
    return;
  }

  recv_sys_finish();

#ifndef UNIV_HOTBACKUP
  if (recv_sys->flush_start != nullptr) {
    os_event_destroy(recv_sys->flush_start);
  }

  if (recv_sys->flush_end != nullptr) {
    os_event_destroy(recv_sys->flush_end);
  }

  ut_ad(!recv_writer_thread_active);
  mutex_free(&recv_sys->writer_mutex);
#endif /* !UNIV_HOTBACKUP */

  call_destructor(&recv_sys->dblwr);
  call_destructor(&recv_sys->deleted);
  call_destructor(&recv_sys->missing_ids);

  mutex_free(&recv_sys->mutex);

  ut_free(recv_sys);
  recv_sys = nullptr;
}

#ifndef UNIV_HOTBACKUP
/** Reset the state of the recovery system variables. */
void recv_sys_var_init() {
  recv_recovery_on = false;
  recv_needed_recovery = false;
  recv_lsn_checks_on = false;
  recv_no_ibuf_operations = false;
  recv_scan_print_counter = 0;
  recv_previous_parsed_rec_type = MLOG_SINGLE_REC_FLAG;
  recv_previous_parsed_rec_offset = 0;
  recv_previous_parsed_rec_is_multi = 0;
#if defined (UNIV_PMEMOBJ_PART_PL)
  recv_n_pool_free_frames = PMEM_N_POOL_FREE_FRAMES;
#else
  recv_n_pool_free_frames = 256;
#endif
  recv_max_page_lsn = 0;
}
#endif /* !UNIV_HOTBACKUP */

/** Get the number of bytes used by all the heaps
@return number of bytes used */
#ifndef UNIV_HOTBACKUP
static size_t recv_heap_used()
#else  /* !UNIV_HOTBACKUP */
size_t meb_heap_used()
#endif /* !UNIV_HOTBACKUP */
{
  size_t size = 0;

  for (auto &space : *recv_sys->spaces) {
    if (space.second.m_heap != nullptr) {
      size += mem_heap_get_size(space.second.m_heap);
    }
  }

  return (size);
}

/** Prints diagnostic info of corrupt log.
@param[in]	ptr	pointer to corrupt log record
@param[in]	type	type of the log record (could be garbage)
@param[in]	space	tablespace ID (could be garbage)
@param[in]	page_no	page number (could be garbage)
@return whether processing should continue */
static bool recv_report_corrupt_log(const byte *ptr, int type, space_id_t space,
                                    page_no_t page_no) {
  ib::error(ER_IB_MSG_694);

  ib::info(
      ER_IB_MSG_695, type, ulong{space}, ulong{page_no},
      ulonglong{recv_sys->recovered_lsn}, int{recv_previous_parsed_rec_type},
      ulonglong{recv_previous_parsed_rec_is_multi},
      ssize_t{ptr - recv_sys->buf}, ulonglong{recv_previous_parsed_rec_offset});

  ut_ad(ptr <= recv_sys->buf + recv_sys->len);

#if defined (UNIV_PMEMOBJ_PART_PL)
  /*we strictly assert any log error in PL-NVM for debugging*/
  assert(0);
  return false;
#endif

  const ulint limit = 100;
  const ulint before = std::min(recv_previous_parsed_rec_offset, limit);
  const ulint after = std::min(recv_sys->len - (ptr - recv_sys->buf), limit);

  ib::info(ER_IB_MSG_696, ulonglong{before}, ulonglong{after});

  ut_print_buf(
      stderr, recv_sys->buf + recv_previous_parsed_rec_offset - before,
      ptr - recv_sys->buf + before + after - recv_previous_parsed_rec_offset);
  putc('\n', stderr);

#ifndef UNIV_HOTBACKUP
  if (srv_force_recovery == 0) {
    ib::info(ER_IB_MSG_697);

    return (false);
  }
#endif /* !UNIV_HOTBACKUP */

  ib::warn(ER_IB_MSG_698, FORCE_RECOVERY_MSG);

  return (true);
}

/** Inits the recovery system for a recovery operation.
@param[in]	max_mem		Available memory in bytes */
void recv_sys_init(ulint max_mem) {
  if (recv_sys->spaces != nullptr) {
    return;
  }

  mutex_enter(&recv_sys->mutex);

#ifndef UNIV_HOTBACKUP
  if (!srv_read_only_mode) {
    recv_sys->flush_start = os_event_create(0);
    recv_sys->flush_end = os_event_create(0);
  }
#else  /* !UNIV_HOTBACKUP */
  recv_is_from_backup = true;
#endif /* !UNIV_HOTBACKUP */

  /* Set appropriate value of recv_n_pool_free_frames. If capacity
  is at least 10M and 25% above 512 pages then bump free frames to
  512. */
  if (buf_pool_get_curr_size() >= (10 * 1024 * 1024) &&
      (buf_pool_get_curr_size() >= ((512 + 128) * UNIV_PAGE_SIZE))) {
    /* Buffer pool of size greater than 10 MB. */
#if defined (UNIV_PMEMOBJ_PART_PL)
	  recv_n_pool_free_frames = PMEM_N_POOL_FREE_FRAMES;
#else //original
	  recv_n_pool_free_frames = 512;
#endif //UNIV_PMEMOBJ_PART_PL
  }

  recv_sys->buf = static_cast<byte *>(ut_malloc_nokey(RECV_PARSING_BUF_SIZE));
  recv_sys->buf_len = RECV_PARSING_BUF_SIZE;

  recv_sys->len = 0;
  recv_sys->recovered_offset = 0;

  using Spaces = recv_sys_t::Spaces;

  recv_sys->spaces = UT_NEW(Spaces(), mem_log_recv_space_hash_key);

  recv_sys->n_addrs = 0;

  recv_sys->apply_log_recs = false;
  recv_sys->apply_batch_on = false;
  recv_sys->is_cloned_db = false;

  recv_sys->last_block_buf_start =
      static_cast<byte *>(ut_malloc_nokey(2 * OS_FILE_LOG_BLOCK_SIZE));

  recv_sys->last_block = static_cast<byte *>(
      ut_align(recv_sys->last_block_buf_start, OS_FILE_LOG_BLOCK_SIZE));

  recv_sys->found_corrupt_log = false;
  recv_sys->found_corrupt_fs = false;

#if defined (UNIV_TRACE_RECOVERY_TIME)
	/*this variable only used for the original*/
	recv_sys->recv_extra_apply_time = 0;
#endif
  recv_max_page_lsn = 0;

  /* Call the constructor for both placement new objects. */
  new (&recv_sys->dblwr) recv_dblwr_t();

  new (&recv_sys->deleted) recv_sys_t::Missing_Ids();

  new (&recv_sys->missing_ids) recv_sys_t::Missing_Ids();

  recv_sys->metadata_recover = UT_NEW_NOKEY(MetadataRecover());

  mutex_exit(&recv_sys->mutex);
}

/** Empties the hash table when it has been fully processed. */
static void recv_sys_empty_hash() {
  ut_ad(mutex_own(&recv_sys->mutex));

  if (recv_sys->n_addrs != 0) {
    ib::fatal(ER_IB_MSG_699, ulonglong{recv_sys->n_addrs});
  }

  for (auto &space : *recv_sys->spaces) {
    if (space.second.m_heap != nullptr) {
      mem_heap_free(space.second.m_heap);
      space.second.m_heap = nullptr;
    }
  }

  UT_DELETE(recv_sys->spaces);

  using Spaces = recv_sys_t::Spaces;

  recv_sys->spaces = UT_NEW(Spaces(), mem_log_recv_space_hash_key);
}

/** Check the consistency of a log header block.
@param[in]	buf	header block
@return true if ok */
#ifndef UNIV_HOTBACKUP
static
#endif /* !UNIV_HOTBACKUP */
    bool
    recv_check_log_header_checksum(const byte *buf) {
  auto c1 = log_block_get_checksum(buf);
  auto c2 = log_block_calc_checksum_crc32(buf);

  return (c1 == c2);
}

/** Check the 4-byte checksum to the trailer checksum field of a log
block.
@param[in]	block	pointer to a log block
@return whether the checksum matches */
static bool log_block_checksum_is_ok(const byte *block) {
  return (!srv_log_checksums ||
          log_block_get_checksum(block) == log_block_calc_checksum(block));
}

/** Get the page map for a tablespace. It will create one if one isn't found.
@param[in]	space_id	Tablespace ID for which page map required.
@param[in]	create		false if lookup only
@return the space data or null if not found */
static recv_sys_t::Space *recv_get_page_map(space_id_t space_id, bool create) {
  auto it = recv_sys->spaces->find(space_id);

  if (it != recv_sys->spaces->end()) {
    return (&it->second);

  } else if (create) {
    mem_heap_t *heap;

    heap = mem_heap_create_typed(256, MEM_HEAP_FOR_RECV_SYS);

    using Space = recv_sys_t::Space;
    using value_type = recv_sys_t::Spaces::value_type;

    auto where =
        recv_sys->spaces->insert(it, value_type(space_id, Space(heap)));

    return (&where->second);
  }

  return (nullptr);
}

/** Gets the list of log records for a <space, page>.
@param[in]	space_id	Tablespace ID
@param[in]	page_no		Page number
@return the redo log entries or nullptr if not found */
static recv_addr_t *recv_get_rec(space_id_t space_id, page_no_t page_no) {
  recv_sys_t::Space *space;

  space = recv_get_page_map(space_id, false);

  if (space != nullptr) {
    auto it = space->m_pages.find(page_no);

    if (it != space->m_pages.end()) {
      return (it->second);
    }
  }

  return (nullptr);
}

#ifndef UNIV_HOTBACKUP
/** Store the collected persistent dynamic metadata to
mysql.innodb_dynamic_metadata */
void MetadataRecover::store() {
  ut_ad(dict_sys->dynamic_metadata != nullptr);
  ut_ad(dict_persist->table_buffer != nullptr);

  DDTableBuffer *table_buffer = dict_persist->table_buffer;

  if (empty()) {
    return;
  }

  mutex_enter(&dict_persist->mutex);

  for (auto meta : m_tables) {
    table_id_t table_id = meta.first;
    PersistentTableMetadata *metadata = meta.second;
    byte buffer[REC_MAX_DATA_SIZE];
    ulint size;

    size = dict_persist->persisters->write(*metadata, buffer);

    dberr_t error =
        table_buffer->replace(table_id, metadata->get_version(), buffer, size);
    if (error != DB_SUCCESS) {
      ut_ad(0);
    }
  }

  mutex_exit(&dict_persist->mutex);
}

/** recv_writer thread tasked with flushing dirty pages from the buffer
pools. */
static void recv_writer_thread() {
  ut_ad(!srv_read_only_mode);

  /* The code flow is as follows:
  Step 1: In recv_recovery_from_checkpoint_start().
  Step 2: This recv_writer thread is started.
  Step 3: In recv_recovery_from_checkpoint_finish().
  Step 4: Wait for recv_writer thread to complete. This is based
          on the flag recv_writer_thread_active.
  Step 5: Assert that recv_writer thread is not active anymore.

  It is possible that the thread that is started in step 2,
  becomes active only after step 4 and hence the assert in
  step 5 fails.  So mark this thread active only if necessary. */
  mutex_enter(&recv_sys->writer_mutex);

  if (recv_recovery_on) {
    recv_writer_thread_active = true;
  } else {
    mutex_exit(&recv_sys->writer_mutex);
    return;
  }
  mutex_exit(&recv_sys->writer_mutex);

  while (srv_shutdown_state == SRV_SHUTDOWN_NONE) {
    os_thread_sleep(100000);

    mutex_enter(&recv_sys->writer_mutex);

    if (!recv_recovery_on) {
      mutex_exit(&recv_sys->writer_mutex);
      break;
    }

    if (log_test != nullptr) {
      mutex_exit(&recv_sys->writer_mutex);
      continue;
    }

    /* Flush pages from end of LRU if required */
    os_event_reset(recv_sys->flush_end);
    recv_sys->flush_type = BUF_FLUSH_LRU;
    os_event_set(recv_sys->flush_start);
    os_event_wait(recv_sys->flush_end);

    mutex_exit(&recv_sys->writer_mutex);
  }

  recv_writer_thread_active = false;

  my_thread_end();
}

#if 0
/** recv_writer thread tasked with flushing dirty pages from the buffer
pools. */
static
void
recv_writer_thread()
{
	ut_ad(!srv_read_only_mode);

	/* The code flow is as follows:
	Step 1: In recv_recovery_from_checkpoint_start().
	Step 2: This recv_writer thread is started.
	Step 3: In recv_recovery_from_checkpoint_finish().
	Step 4: Wait for recv_writer thread to complete. This is based
	        on the flag recv_writer_thread_active.
	Step 5: Assert that recv_writer thread is not active anymore.

	It is possible that the thread that is started in step 2,
	becomes active only after step 4 and hence the assert in
	step 5 fails.  So mark this thread active only if necessary. */
	mutex_enter(&recv_sys->writer_mutex);

	if (recv_recovery_on) {
		recv_writer_thread_active = true;
	} else {
		mutex_exit(&recv_sys->writer_mutex);
		return;
	}
	mutex_exit(&recv_sys->writer_mutex);

	while (srv_shutdown_state == SRV_SHUTDOWN_NONE) {

		os_thread_sleep(100000);

		mutex_enter(&recv_sys->writer_mutex);

		if (!recv_recovery_on) {
			mutex_exit(&recv_sys->writer_mutex);
			break;
		}

		/* Flush pages from end of LRU if required */
		os_event_reset(recv_sys->flush_end);
		recv_sys->flush_type = BUF_FLUSH_LRU;
		os_event_set(recv_sys->flush_start);
		os_event_wait(recv_sys->flush_end);

		mutex_exit(&recv_sys->writer_mutex);
	}

	recv_writer_thread_active = false;

	my_thread_end();
}
#endif

/** Frees the recovery system. */
void recv_sys_free() {
  mutex_enter(&recv_sys->mutex);

  recv_sys_finish();

  /* wake page cleaner up to progress */
  if (!srv_read_only_mode) {
    ut_ad(!recv_recovery_on);
    ut_ad(!recv_writer_thread_active);
    if (buf_flush_event != nullptr) {
      os_event_reset(buf_flush_event);
    }
    os_event_set(recv_sys->flush_start);
  }

  /* Free encryption data structures. */
  if (recv_sys->keys != nullptr) {
    for (auto &key : *recv_sys->keys) {
      if (key.ptr != nullptr) {
        ut_free(key.ptr);
        key.ptr = nullptr;
      }

      if (key.iv != nullptr) {
        ut_free(key.iv);
        key.iv = nullptr;
      }
    }

    recv_sys->keys->swap(*recv_sys->keys);

    UT_DELETE(recv_sys->keys);
    recv_sys->keys = nullptr;
  }

  mutex_exit(&recv_sys->mutex);
}

/** Copy of the LOG_HEADER_CREATOR field. */
static char log_header_creator[LOG_HEADER_CREATOR_END - LOG_HEADER_CREATOR + 1];

/** Determine if a redo log from a version before MySQL 8.0.3 is clean.
@param[in,out]	log		redo log
@param[in]	checkpoint_no	checkpoint number
@param[in]	checkpoint_lsn	checkpoint LSN
@return error code
@retval DB_SUCCESS	if the redo log is clean
@retval DB_ERROR	if the redo log is corrupted or dirty */
static dberr_t recv_log_recover_pre_8_0_4(log_t &log,
                                          checkpoint_no_t checkpoint_no,
                                          lsn_t checkpoint_lsn) {
  lsn_t source_offset;
  lsn_t block_lsn;
  page_no_t page_no;
  byte *buf;

  source_offset = log_files_real_offset_for_lsn(log, checkpoint_lsn);

  block_lsn = ut_uint64_align_down(checkpoint_lsn, OS_FILE_LOG_BLOCK_SIZE);

  page_no = (page_no_t)(source_offset / univ_page_size.physical());

  buf = log.buf + block_lsn % log.buf_size;

  static const char *NO_UPGRADE_RECOVERY_MSG =
      "Upgrade after a crash is not supported."
      " This redo log was created with ";

  static const char *NO_UPGRADE_RTFM_MSG =
      ". Please follow the instructions at " REFMAN "upgrading.html";

  dberr_t err;

  err = fil_redo_io(IORequestLogRead, page_id_t(log.files_space_id, page_no),
                    univ_page_size,
                    (ulint)((source_offset & ~(OS_FILE_LOG_BLOCK_SIZE - 1)) %
                            univ_page_size.physical()),
                    OS_FILE_LOG_BLOCK_SIZE, buf);

  ut_a(err == DB_SUCCESS);

  if (log_block_calc_checksum(buf) != log_block_get_checksum(buf)) {
    ib::error(ER_IB_MSG_700)
        << NO_UPGRADE_RECOVERY_MSG << log_header_creator
        << ", and it appears corrupted" << NO_UPGRADE_RTFM_MSG;

    return (DB_CORRUPTION);
  }

  /* On a clean shutdown, the redo log will be logically empty
  after the checkpoint LSN. */

  if (log_block_get_data_len(buf) !=
      (source_offset & (OS_FILE_LOG_BLOCK_SIZE - 1))) {
    ib::error(ER_IB_MSG_701)
        << NO_UPGRADE_RECOVERY_MSG << log_header_creator << NO_UPGRADE_RTFM_MSG;

    return (DB_ERROR);
  }

  /* Mark the redo log for upgrading. */
  srv_log_file_size = 0;

  recv_sys->parse_start_lsn = checkpoint_lsn;
  recv_sys->bytes_to_ignore_before_checkpoint = 0;
  recv_sys->recovered_lsn = checkpoint_lsn;
  recv_sys->checkpoint_lsn = checkpoint_lsn;
  recv_sys->scanned_lsn = checkpoint_lsn;

  log_start(log, checkpoint_no + 1, checkpoint_lsn, checkpoint_lsn);

  return (DB_SUCCESS);
}

/** Find the latest checkpoint in the log header.
@param[in,out]	log		redo log
@param[out]	max_field	LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2
@return error code or DB_SUCCESS */
static MY_ATTRIBUTE((warn_unused_result)) dberr_t
    recv_find_max_checkpoint(log_t &log, ulint *max_field) {
  bool found_checkpoint = false;

  *max_field = 0;

  byte *buf = log.checkpoint_buf;

  log.state = log_state_t::CORRUPTED;

  log_files_header_read(log, 0);

  /* Check the header page checksum. There was no
  checksum in the first redo log format (version 0). */
  log.format = mach_read_from_4(buf + LOG_HEADER_FORMAT);

  if (log.format != 0 && !recv_check_log_header_checksum(buf)) {
    ib::error(ER_IB_MSG_1264) << "Invalid redo log header checksum.";

    return (DB_CORRUPTION);
  }

  memcpy(log_header_creator, buf + LOG_HEADER_CREATOR,
         sizeof log_header_creator);

  log_header_creator[(sizeof log_header_creator) - 1] = 0;

  switch (log.format) {
    case 0:
      ib::error(ER_IB_MSG_1265) << "Unsupported redo log format (" << log.format
                                << "). The redo log was created"
                                << " before MySQL 5.7.9";

      return (DB_ERROR);

    case LOG_HEADER_FORMAT_5_7_9:
    case LOG_HEADER_FORMAT_8_0_1:

      ib::info(ER_IB_MSG_704, ulong{log.format});

    case LOG_HEADER_FORMAT_CURRENT:
      /* The checkpoint page format is identical upto v3. */
      break;

    default:
      ib::error(ER_IB_MSG_705, ulong{log.format}, REFMAN);

      return (DB_ERROR);
  }

  uint64_t max_no = 0;
  constexpr ulint CKP1 = LOG_CHECKPOINT_1;
  constexpr ulint CKP2 = LOG_CHECKPOINT_2;

  for (auto i = CKP1; i <= CKP2; i += CKP2 - CKP1) {
    log_files_header_read(log, static_cast<uint32_t>(i));

    if (!recv_check_log_header_checksum(buf)) {
      DBUG_PRINT("ib_log", ("invalid checkpoint, at %lu, checksum %x", i,
                            (unsigned)log_block_get_checksum(buf)));
      continue;
    }

    log.state = log_state_t::OK;

    log.current_file_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);

    log.current_file_real_offset =
        mach_read_from_8(buf + LOG_CHECKPOINT_OFFSET);

    if (log.current_file_real_offset % log.file_size < LOG_FILE_HDR_SIZE) {
      log.current_file_real_offset -=
          log.current_file_real_offset % log.file_size;

      log.current_file_real_offset += LOG_FILE_HDR_SIZE;
    }

    log_files_update_offsets(log, log.current_file_lsn);

    uint64_t checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

    DBUG_PRINT("ib_log", ("checkpoint " UINT64PF " at " LSN_PF, checkpoint_no,
                          log.current_file_lsn));

    if (checkpoint_no >= max_no) {
      *max_field = i;
      max_no = checkpoint_no;
      found_checkpoint = true;
    }
  }

  if (!found_checkpoint) {
    /* Before 5.7.9, we could get here during database
    initialization if we created an ib_logfile0 file that
    was filled with zeroes, and were killed. After
    5.7.9, we would reject such a file already earlier,
    when checking the file header. */

    ib::error(ER_IB_MSG_706);
    return (DB_ERROR);
  }

  return (DB_SUCCESS);
}

/** Reads in pages which have hashed log records, from an area around a given
page number.
@param[in]	page_id		Read the pages around this page number
@return number of pages found */
static ulint recv_read_in_area(const page_id_t &page_id) {
  page_no_t low_limit;

  low_limit = page_id.page_no() - (page_id.page_no() % RECV_READ_AHEAD_AREA);

  ulint n = 0;

  std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;

  for (page_no_t page_no = low_limit;
       page_no < low_limit + RECV_READ_AHEAD_AREA; ++page_no) {
    recv_addr_t *recv_addr;

    recv_addr = recv_get_rec(page_id.space(), page_no);

    const page_id_t cur_page_id(page_id.space(), page_no);

    if (recv_addr != nullptr && !buf_page_peek(cur_page_id)) {
      mutex_enter(&recv_sys->mutex);

      if (recv_addr->state == RECV_NOT_PROCESSED) {
        recv_addr->state = RECV_BEING_READ;

        page_nos[n] = page_no;

        ++n;
      }

      mutex_exit(&recv_sys->mutex);
    }
  }

  buf_read_recv_pages(false, page_id.space(), &page_nos[0], n);

  return (n);
}

/** Apply the log records to a page
@param[in,out]	recv_addr	Redo log records to apply */
static void recv_apply_log_rec(recv_addr_t *recv_addr) {
  if (recv_addr->state == RECV_DISCARDED) {
    ut_a(recv_sys->n_addrs > 0);
    --recv_sys->n_addrs;
    return;
  }

  bool found;
  const page_id_t page_id(recv_addr->space, recv_addr->page_no);

  const page_size_t page_size =
      fil_space_get_page_size(recv_addr->space, &found);

  if (!found || recv_sys->missing_ids.find(recv_addr->space) !=
                    recv_sys->missing_ids.end()) {
    /* Tablespace was discarded or dropped after changes were
    made to it. Or, we have ignored redo log for this tablespace
    earlier and somehow it has been found now. We can't apply
    this redo log out of order. */

    recv_addr->state = RECV_PROCESSED;

    ut_a(recv_sys->n_addrs > 0);
    --recv_sys->n_addrs;

    /* If the tablespace has been explicitly deleted, we
    can safely ignore it. */

    if (recv_sys->deleted.find(recv_addr->space) == recv_sys->deleted.end()) {
      recv_sys->missing_ids.insert(recv_addr->space);
    }

  } else if (recv_addr->state == RECV_NOT_PROCESSED) {
    mutex_exit(&recv_sys->mutex);

    if (buf_page_peek(page_id)) {
      mtr_t mtr;

      mtr_start(&mtr);

      buf_block_t *block;

      block = buf_page_get(page_id, page_size, RW_X_LATCH, &mtr);

      buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

      recv_recover_page(false, block);

      mtr_commit(&mtr);

    } else {
      recv_read_in_area(page_id);
    }

    mutex_enter(&recv_sys->mutex);
  }
}

/** Empties the hash table of stored log records, applying them to appropriate
pages.
@param[in,out]	log		Redo log
@param[in]	allow_ibuf	if true, ibuf operations are allowed during
                                the application; if false, no ibuf operations
                                are allowed, and after the application all
                                file pages are flushed to disk and invalidated
                                in buffer pool: this alternative means that
                                no new log records can be generated during
                                the application; the caller must in this case
                                own the log mutex */
void recv_apply_hashed_log_recs(log_t &log, bool allow_ibuf) {
#if defined (UNIV_PMEMOBJ_PART_PL)
	/*redirect to our function*/
	if (!gb_pmw->ppl->is_new){
		return (pm_ppl_recv_apply_hashed_log_recs(gb_pmw->pop, gb_pmw->ppl, allow_ibuf) );
	}
#endif
  for (;;) {
    mutex_enter(&recv_sys->mutex);

    if (!recv_sys->apply_batch_on) {
      break;
    }

    mutex_exit(&recv_sys->mutex);

    os_thread_sleep(500000);
  }

  if (!allow_ibuf) {
    recv_no_ibuf_operations = true;
  }

  recv_sys->apply_log_recs = true;
  recv_sys->apply_batch_on = true;

  auto batch_size = recv_sys->n_addrs;

  ib::info(ER_IB_MSG_707, ulonglong{batch_size});

  static const size_t PCT = 10;

  size_t pct = PCT;
  size_t applied = 0;
  auto unit = batch_size / PCT;

  if (unit <= PCT) {
    pct = 100;
    unit = batch_size;
  }

  auto start_time = ut_time();

  for (const auto &space : *recv_sys->spaces) {
    bool dropped;

    if (space.first != TRX_SYS_SPACE &&
        !fil_tablespace_open_for_recovery(space.first)) {
      /* Tablespace was dropped. It should not have been scanned unless it
      is an undo space that was under construction. */
      ut_ad(!fil_tablespace_lookup_for_recovery(space.first) ||
            fsp_is_undo_tablespace(space.first));

      dropped = true;
    } else {
      dropped = false;
    }

    for (auto pages : space.second.m_pages) {
      ut_ad(pages.second->space == space.first);

      if (dropped) {
        pages.second->state = RECV_DISCARDED;
      }

      recv_apply_log_rec(pages.second);

      ++applied;

      if (unit == 0 || (applied % unit) == 0) {
        ib::info(ER_IB_MSG_708) << pct << "%";

        pct += PCT;

        start_time = ut_time();

      } else if (ut_time() - start_time >= PRINT_INTERVAL_SECS) {
        start_time = ut_time();

        ib::info(ER_IB_MSG_709)
            << std::setprecision(2)
            << ((double)applied * 100) / (double)batch_size << "%";
      }
    }
  }

  /* Wait until all the pages have been processed */

  while (recv_sys->n_addrs != 0) {
    mutex_exit(&recv_sys->mutex);

    os_thread_sleep(500000);

    mutex_enter(&recv_sys->mutex);
  }

  if (!allow_ibuf) {
    /* Flush all the file pages to disk and invalidate them in
    the buffer pool */

    ut_d(log.disable_redo_writes = true);

    mutex_exit(&recv_sys->mutex);

    /* Stop the recv_writer thread from issuing any LRU
    flush batches. */
    mutex_enter(&recv_sys->writer_mutex);

    /* Wait for any currently run batch to end. */
    buf_flush_wait_LRU_batch_end();

    os_event_reset(recv_sys->flush_end);

    recv_sys->flush_type = BUF_FLUSH_LIST;

    os_event_set(recv_sys->flush_start);

    os_event_wait(recv_sys->flush_end);

    buf_pool_invalidate();

    /* Allow batches from recv_writer thread. */
    mutex_exit(&recv_sys->writer_mutex);

    ut_d(log.disable_redo_writes = false);

    mutex_enter(&recv_sys->mutex);

    recv_no_ibuf_operations = false;
  }

  recv_sys->apply_log_recs = false;
  recv_sys->apply_batch_on = false;

  recv_sys_empty_hash();

  mutex_exit(&recv_sys->mutex);

  ib::info(ER_IB_MSG_710);
}

#else /* !UNIV_HOTBACKUP */
/** Scans the log segment and n_bytes_scanned is set to the length of valid
log scanned.
@param[in]	buf			buffer containing log data
@param[in]	buf_len			data length in that buffer
@param[in,out]	scanned_lsn		LSN of buffer start, we return scanned
lsn
@param[in,out]	scanned_checkpoint_no	4 lowest bytes of the highest scanned
checkpoint number so far
@param[out]	n_bytes_scanned		how much we were able to scan, smaller
than buf_len if log data ended here */
void meb_scan_log_seg(byte *buf, ulint buf_len, lsn_t *scanned_lsn,
                      ulint *scanned_checkpoint_no, ulint *n_bytes_scanned) {
  *n_bytes_scanned = 0;

  for (auto log_block = buf; log_block < buf + buf_len;
       log_block += OS_FILE_LOG_BLOCK_SIZE) {
    ulint no = log_block_get_hdr_no(log_block);

    if (no != log_block_convert_lsn_to_no(*scanned_lsn) ||
        !log_block_checksum_is_ok(log_block)) {
      ib::trace_2() << "Scanned lsn: " << *scanned_lsn << " header no: " << no
                    << " converted no: "
                    << log_block_convert_lsn_to_no(*scanned_lsn)
                    << " checksum: " << log_block_checksum_is_ok(log_block)
                    << " block cp no: "
                    << log_block_get_checkpoint_no(log_block);

      /* Garbage or an incompletely written log block */

      log_block += OS_FILE_LOG_BLOCK_SIZE;
      break;
    }

    if (*scanned_checkpoint_no > 0 &&
        log_block_get_checkpoint_no(log_block) < *scanned_checkpoint_no &&
        *scanned_checkpoint_no - log_block_get_checkpoint_no(log_block) >
            0x80000000UL) {
      /* Garbage from a log buffer flush which was made
      before the most recent database recovery */

      ib::trace_2() << "Scanned cp no: " << *scanned_checkpoint_no
                    << " block cp no "
                    << log_block_get_checkpoint_no(log_block);

      break;
    }

    ulint data_len = log_block_get_data_len(log_block);

    *scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
    *scanned_lsn += data_len;

    *n_bytes_scanned += data_len;

    if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
      /* Log data ends here */

      break;
    }
  }
}

/** Apply a single log record stored in the hash table.
@param[in,out]	recv_addr	a parsed log record
@param[in,out]	block           a buffer pool frame for applying the record */
void meb_apply_log_record(recv_addr_t *recv_addr, buf_block_t *block) {
  bool found;
  const page_id_t page_id(recv_addr->space, recv_addr->page_no);

  const page_size_t &page_size =
      fil_space_get_page_size(recv_addr->space, &found);

  ib::trace_3() << "recv_addr {State: " << recv_addr->state
                << ", Space id: " << recv_addr->space
                << ", Page no: " << recv_addr->page_no
                << ", Page size: " << page_size << ", found: " << found << "\n";

  if (!found) {
    recv_addr->state = RECV_DISCARDED;

    mutex_enter(&recv_sys->mutex);

    ut_a(recv_sys->n_addrs);
    --recv_sys->n_addrs;

    mutex_exit(&recv_sys->mutex);

    return;
  }

  mutex_enter(&recv_sys->mutex);

  /* We simulate a page read made by the buffer pool, to
  make sure the recovery apparatus works ok. We must init
  the block. */

  meb_page_init(page_id, page_size, block);

  /* Extend the tablespace's last file if the page_no
  does not fall inside its bounds; we assume the last
  file is auto-extending, and mysqlbackup copied the file
  when it still was smaller */

  fil_space_t *space = fil_space_get(recv_addr->space);

  bool success;

  success = fil_space_extend(space, recv_addr->page_no + 1);

  if (!success) {
    ib::fatal(ER_IB_MSG_711) << "Cannot extend tablespace " << recv_addr->space
                             << " to hold " << recv_addr->page_no << " pages";
  }

  mutex_exit(&recv_sys->mutex);

  /* Read the page from the tablespace file. */

  dberr_t err;

  if (page_size.is_compressed()) {
    err = fil_io(IORequestRead, true, page_id, page_size, 0,
                 page_size.physical(), block->page.zip.data, nullptr);

    if (err == DB_SUCCESS && !buf_zip_decompress(block, TRUE)) {
      ut_error;
    }
  } else {
    err = fil_io(IORequestRead, true, page_id, page_size, 0,
                 page_size.logical(), block->frame, nullptr);
  }

  if (err != DB_SUCCESS) {
    ib::fatal(ER_IB_MSG_712)
        << "Cannot read from tablespace " << recv_addr->space << " page number "
        << recv_addr->page_no;
  }

  apply_log_mutex.lock();

  /* Apply the log records to this page */
  recv_recover_page(false, block);

  apply_log_mutex.unlock();

  mutex_enter(&recv_sys->mutex);

  /* Write the page back to the tablespace file using the
  fil0fil.cc routines */

  buf_flush_init_for_writing(block, block->frame, buf_block_get_page_zip(block),
                             mach_read_from_8(block->frame + FIL_PAGE_LSN),
                             fsp_is_checksum_disabled(block->page.id.space()),
                             true /* skip_lsn_check */);

  mutex_exit(&recv_sys->mutex);

  if (page_size.is_compressed()) {
    err = fil_io(IORequestWrite, true, page_id, page_size, 0,
                 page_size.physical(), block->page.zip.data, nullptr);
  } else {
    err = fil_io(IORequestWrite, true, page_id, page_size, 0,
                 page_size.logical(), block->frame, nullptr);
  }

  if (err != DB_SUCCESS) {
    ib::fatal(ER_IB_MSG_713)
        << "Cannot write to tablespace " << recv_addr->space << " page number "
        << recv_addr->page_no;
  }
}

/** Apply a single log record stored in the hash table using default block.
@param[in,out]	recv_addr	a parsed log record */
void meb_apply_log_rec_func(recv_addr_t *recv_addr) {
  meb_apply_log_record(recv_addr, back_block1);
}

/** Dummy wait function for meb_apply_log_recs_via_callback(). */
void meb_nowait_func() { return; }

/** Applies log records in the hash table to a backup. */
void meb_apply_log_recs() {
  meb_apply_log_recs_via_callback(meb_apply_log_rec_func, meb_nowait_func);
}

/** Apply all log records in the hash table to a backup using callback
functions. This function employes two callback functions that allow redo
log records to be applied in parallel. The apply_log_record_function
assigns a parsed redo log record for application. The
apply_log_record_function is called repeatedly until all log records in
the hash table are assigned for application. After that the
wait_till_done_function is called once. The wait_till_done_function
function blocks until the application of all the redo log records
previously assigned with apply_log_record_function calls is complete.
Even though this function assigns the log records in the hash table
sequentially, the application of the log records may be done in parallel
if the apply_log_record_function delegates the actual application work
to multiple worker threads running in parallel.
@param[in]  apply_log_record_function	a function that assigns one redo log
record for application
@param[in]  wait_till_done_function     a function that blocks until all
assigned redo log records have been applied */
void meb_apply_log_recs_via_callback(
    void (*apply_log_record_function)(recv_addr_t *),
    void (*wait_till_done_function)()) {
  ulint n_hash_cells = recv_sys->n_addrs;
  ulint i = 0;

  recv_sys->apply_log_recs = true;
  recv_sys->apply_batch_on = true;

  ib::info(ER_IB_MSG_714) << "Starting to apply a batch of log records to the"
                          << " database...";

  fputs("InnoDB: Progress in percent: ", stderr);

  for (const auto &space : *recv_sys->spaces) {
    for (auto pages : space.second.m_pages) {
      ut_ad(pages.second->space == space.first);

      (*apply_log_record_function)(pages.second);
    }

    ++i;
    if ((100 * i) / n_hash_cells != (100 * (i + 1)) / n_hash_cells) {
      fprintf(stderr, "%lu ", (ulong)((100 * i) / n_hash_cells));
      fflush(stderr);
    }
  }

  /* wait till all the redo log records have been applied */
  (*wait_till_done_function)();

  /* write logs in next line */
  fprintf(stderr, "\n");
  recv_sys->apply_log_recs = false;
  recv_sys->apply_batch_on = false;
  recv_sys_empty_hash();
}

#endif /* !UNIV_HOTBACKUP */

/** Try to parse a single log record body and also applies it if
specified.
@param[in]	type		redo log entry type
@param[in]	ptr		redo log record body
@param[in]	end_ptr		end of buffer
@param[in]	space_id	tablespace identifier
@param[in]	page_no		page number
@param[in,out]	block		buffer block, or nullptr if
                                a page log record should not be applied
                                or if it is a MLOG_FILE_ operation
@param[in,out]	mtr		mini-transaction, or nullptr if
                                a page log record should not be applied
@param[in]	parsed_bytes	Number of bytes parsed so far
@return log record end, nullptr if not a complete record */
static byte *recv_parse_or_apply_log_rec_body(
    mlog_id_t type, byte *ptr, byte *end_ptr, space_id_t space_id,
    page_no_t page_no, buf_block_t *block, mtr_t *mtr, ulint parsed_bytes) {
  ut_ad(!block == !mtr);

  switch (type) {
    case MLOG_FILE_DELETE:

      return (fil_tablespace_redo_delete(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          recv_sys->bytes_to_ignore_before_checkpoint != 0));

    case MLOG_FILE_CREATE:

      return (fil_tablespace_redo_create(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          recv_sys->bytes_to_ignore_before_checkpoint != 0));

    case MLOG_FILE_RENAME:

      return (fil_tablespace_redo_rename(
          ptr, end_ptr, page_id_t(space_id, page_no), parsed_bytes,
          recv_sys->bytes_to_ignore_before_checkpoint != 0));

    case MLOG_INDEX_LOAD:
#ifdef UNIV_HOTBACKUP
      /* While scaning redo logs during  backup phase a
      MLOG_INDEX_LOAD type redo log record indicates a DDL
      (create index, alter table...)is performed with
      'algorithm=inplace'. This redo log indicates that

      1. The DDL was started after MEB started backing up, in which
      case MEB will not be able to take a consistent backup and should
      fail. or
      2. There is a possibility of this record existing in the REDO
      even after the completion of the index create operation. This is
      because of InnoDB does  not checkpointing after the flushing the
      index pages.

      If MEB gets the last_redo_flush_lsn and that is less than the
      lsn of the current record MEB fails the backup process.
      Error out in case of online backup and emit a warning in case
      of offline backup and continue. */
      if (!recv_recovery_on) {
        if (is_online_redo_copy) {
          if (backup_redo_log_flushed_lsn < recv_sys->recovered_lsn) {
            ib::trace_1() << "Last flushed lsn: " << backup_redo_log_flushed_lsn
                          << " load_index lsn " << recv_sys->recovered_lsn;

            if (backup_redo_log_flushed_lsn == 0) {
              ib::error(ER_IB_MSG_715) << "MEB was not able"
                                       << " to determine the"
                                       << " InnoDB Engine"
                                       << " Status";
            }

            ib::fatal(ER_IB_MSG_716) << "An optimized(without"
                                     << " redo logging) DDL"
                                     << " operation has been"
                                     << " performed. All modified"
                                     << " pages may not have been"
                                     << " flushed to the disk yet.\n"
                                     << "    MEB will not be able to"
                                     << " take a consistent backup."
                                     << " Retry the backup"
                                     << " operation";
          }
          /** else the index is flushed to disk before
          backup started hence no error */
        } else {
          /* offline backup */
          ib::trace_1() << "Last flushed lsn: " << backup_redo_log_flushed_lsn
                        << " load_index lsn " << recv_sys->recovered_lsn;

          ib::warn(ER_IB_MSG_717);
        }
      }
#endif /* UNIV_HOTBACKUP */
      if (end_ptr < ptr + 8) {
#if defined (UNIV_PMEMOBJ_PART_PL)
			assert(0);
#endif
        return (nullptr);
      }

      return (ptr + 8);

    case MLOG_WRITE_STRING:

#ifdef UNIV_HOTBACKUP
      if (recv_recovery_on && meb_is_space_loaded(space_id)) {
#endif /* UNIV_HOTBACKUP */
        /* For encrypted tablespace, we need to get the
        encryption key information before the page 0 is
        recovered. Otherwise, redo will not find the key
        to decrypt the data pages. */

        if (page_no == 0 && !fsp_is_system_or_temp_tablespace(space_id)) {
          return (fil_tablespace_redo_encryption(ptr, end_ptr, space_id));
        }
#ifdef UNIV_HOTBACKUP
      }
#endif /* UNIV_HOTBACKUP */

      break;

    default:
      break;
  }
#if defined (UNIV_PMEMOBJ_PART_PL)
/*This optimization let the REDO phase 1 finish earlier by just parsing
 * the header of the REDO log.
 * It reduce the the REDO1 duration. However, it increase the REDO2
 * time. So I skip this optimization now*/
//  if(!gb_pmw->ppl->is_new){
//	  /*In PM-PPL REDO1 just return here*/
//	  if (block == nullptr){
//		  return nullptr;
//	  }
//  }
#endif //UNIV_PMEMOBJ_PART_PL

  page_t *page;
  page_zip_des_t *page_zip;
  dict_index_t *index = nullptr;

#ifdef UNIV_DEBUG
  ulint page_type;
#endif /* UNIV_DEBUG */

#if defined(UNIV_HOTBACKUP) && defined(UNIV_DEBUG)
  ib::trace_3() << "recv_parse_or_apply_log_rec_body { type: "
                << get_mlog_string(type) << ", space_id: " << space_id
                << ", page_no: " << page_no
                << ", ptr : " << static_cast<const void *>(ptr)
                << ", end_ptr: " << static_cast<const void *>(end_ptr)
                << ", block: " << static_cast<const void *>(block)
                << ", mtr: " << static_cast<const void *>(mtr) << " }";
#endif /* UNIV_HOTBACKUP && UNIV_DEBUG */

  if (block != nullptr) {
    /* Applying a page log record. */

    page = block->frame;
    page_zip = buf_block_get_page_zip(block);

    ut_d(page_type = fil_page_get_type(page));
#if defined(UNIV_HOTBACKUP) && defined(UNIV_DEBUG)
    if (page_type == 0) {
      meb_print_page_header(page);
    }
#endif /* UNIV_HOTBACKUP && UNIV_DEBUG */

  } else {
    /* Parsing a page log record. */
    page = nullptr;
    page_zip = nullptr;

    ut_d(page_type = FIL_PAGE_TYPE_ALLOCATED);
  }

  const byte *old_ptr = ptr;

  switch (type) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      /* The LSN is checked in recv_parse_log_rec(). */
      break;
#endif /* UNIV_LOG_LSN_DEBUG */
    case MLOG_4BYTES:

      ut_ad(page == nullptr || end_ptr > ptr + 2);

      /* Most FSP flags can only be changed by CREATE or ALTER with
      ALGORITHM=COPY, so they do not change once the file
      is created. The SDI flag is the only one that can be
      changed by a recoverable transaction. So if there is
      change in FSP flags, update the in-memory space structure
      (fil_space_t) */

      if (page != nullptr && page_no == 0 &&
          mach_read_from_2(ptr) == FSP_HEADER_OFFSET + FSP_SPACE_FLAGS) {
        ptr = mlog_parse_nbytes(MLOG_4BYTES, ptr, end_ptr, page, page_zip);

        /* When applying log, we have complete records.
        They can be incomplete (ptr=nullptr) only during
        scanning (page==nullptr) */

        ut_ad(ptr != nullptr);

        fil_space_t *space = fil_space_acquire(space_id);

        ut_ad(space != nullptr);

        fil_space_set_flags(space, mach_read_from_4(FSP_HEADER_OFFSET +
                                                    FSP_SPACE_FLAGS + page));
        fil_space_release(space);

        break;
      }

      // fall through

    case MLOG_1BYTE:
      /* If 'ALTER TABLESPACE ... ENCRYPTION' was in progress and page 0 has
      REDO entry for this, set encryption_op_in_progress flag now so that any
      other page of this tablespace in redo log is written accordingly. */
      if (page_no == 0 && page != nullptr && end_ptr >= ptr + 2) {
        ulint offs = mach_read_from_2(ptr);

        fil_space_t *space = fil_space_acquire(space_id);
        ut_ad(space != nullptr);
        ulint offset = fsp_header_get_encryption_progress_offset(
            page_size_t(space->flags));

        if (offs == offset) {
          ptr = mlog_parse_nbytes(MLOG_1BYTE, ptr, end_ptr, page, page_zip);
          byte op = mach_read_from_1(page + offset);
          switch (op) {
            case ENCRYPTION_IN_PROGRESS:
              space->encryption_op_in_progress = ENCRYPTION;
              break;
            case UNENCRYPTION_IN_PROGRESS:
              space->encryption_op_in_progress = UNENCRYPTION;
              break;
            default:
              /* Don't reset operation in progress yet. It'll be done in
              fsp_resume_encryption_unencryption(). */
              break;
          }
        }
        fil_space_release(space);
      }

      // fall through

    case MLOG_2BYTES:
    case MLOG_8BYTES:
#if defined (UNIV_PMEMOBJ_PART_PL)
	goto skip_check; //skip debug check
#endif

#ifdef UNIV_DEBUG
      if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        /* It is OK to set FIL_PAGE_TYPE and certain
        list node fields on an empty page.  Any other
        write is not OK. */

        /* NOTE: There may be bogus assertion failures for
        dict_hdr_create(), trx_rseg_header_create(),
        trx_sys_create_doublewrite_buf(), and
        trx_sysf_create().
        These are only called during database creation. */

        ulint offs = mach_read_from_2(ptr);

        switch (type) {
          default:
            ut_error;
          case MLOG_2BYTES:
            /* Note that this can fail when the
            redo log been written with something
            older than InnoDB Plugin 1.0.4. */
            ut_ad(
                offs == FIL_PAGE_TYPE ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_BYTE +
                            FIL_ADDR_SIZE ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET ||
                offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                            FIL_ADDR_BYTE + 0 /*FLST_PREV*/
                || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                               FIL_ADDR_BYTE + FIL_ADDR_SIZE /*FLST_NEXT*/);
            break;
          case MLOG_4BYTES:
            /* Note that this can fail when the
            redo log been written with something
            older than InnoDB Plugin 1.0.4. */
            ut_ad(
                0 ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_SPACE ||
                offs == IBUF_TREE_SEG_HEADER + IBUF_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER /* flst_init */
                ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE ||
                offs == PAGE_BTR_IBUF_FREE_LIST + PAGE_HEADER + FIL_ADDR_PAGE +
                            FIL_ADDR_SIZE ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
                offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE ||
                offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                            FIL_ADDR_PAGE + 0 /*FLST_PREV*/
                || offs == PAGE_BTR_IBUF_FREE_LIST_NODE + PAGE_HEADER +
                               FIL_ADDR_PAGE + FIL_ADDR_SIZE /*FLST_NEXT*/);
            break;
        }
      }
#endif /* UNIV_DEBUG */

#if defined (UNIV_PMEMOBJ_PART_PL)
skip_check: //skip debug check
#endif
      ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);

      if (ptr != nullptr && page != nullptr && page_no == 0 &&
          type == MLOG_4BYTES) {
        ulint offs = mach_read_from_2(old_ptr);

        switch (offs) {
          fil_space_t *space;
          uint32_t val;
          default:
            break;

          case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
          case FSP_HEADER_OFFSET + FSP_SIZE:
          case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
          case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:

            space = fil_space_get(space_id);

            ut_a(space != nullptr);

            val = mach_read_from_4(page + offs);

            switch (offs) {
              case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                space->flags = val;
                break;

              case FSP_HEADER_OFFSET + FSP_SIZE:

                space->size_in_header = val;

                if (space->size >= val) {
                  break;
                }

                ib::info(ER_IB_MSG_718, ulong{space->id}, space->name,
                         ulong{val});

                if (fil_space_extend(space, val)) {
                  break;
                }

                ib::error(ER_IB_MSG_719, ulong{space->id}, space->name,
                          ulong{val});
                break;

              case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                space->free_limit = val;
                break;

              case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                space->free_len = val;
                ut_ad(val == flst_get_len(page + offs));
                break;
            }
        }
      }
      break;

    case MLOG_REC_INSERT:
    case MLOG_COMP_REC_INSERT:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_INSERT,
                                  &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_cur_parse_insert_rec(FALSE, ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_REC_CLUST_DELETE_MARK:
    case MLOG_COMP_REC_CLUST_DELETE_MARK:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(
                          ptr, end_ptr, type == MLOG_COMP_REC_CLUST_DELETE_MARK,
                          &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr, page, page_zip,
                                                   index);
      }

      break;

    case MLOG_COMP_REC_SEC_DELETE_MARK:

      ut_ad(!page || fil_page_type_is_index(page_type));

      /* This log record type is obsolete, but we process it for
      backward compatibility with MySQL 5.0.3 and 5.0.4. */

      ut_a(!page || page_is_comp(page));
      ut_a(!page_zip);

      ptr = mlog_parse_index(ptr, end_ptr, true, &index);

      if (ptr == nullptr) {
        break;
      }

      /* Fall through */

    case MLOG_REC_SEC_DELETE_MARK:

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr, page, page_zip);
      break;

    case MLOG_REC_UPDATE_IN_PLACE:
    case MLOG_COMP_REC_UPDATE_IN_PLACE:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index(
               ptr, end_ptr, type == MLOG_COMP_REC_UPDATE_IN_PLACE, &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr =
            btr_cur_parse_update_in_place(ptr, end_ptr, page, page_zip, index);
      }

      break;

    case MLOG_LIST_END_DELETE:
    case MLOG_COMP_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE:
    case MLOG_COMP_LIST_START_DELETE:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index(ptr, end_ptr,
                                  type == MLOG_COMP_LIST_END_DELETE ||
                                      type == MLOG_COMP_LIST_START_DELETE,
                                  &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_LIST_END_COPY_CREATED:
    case MLOG_COMP_LIST_END_COPY_CREATED:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr != (ptr = mlog_parse_index(
                          ptr, end_ptr, type == MLOG_COMP_LIST_END_COPY_CREATED,
                          &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, block,
                                                       index, mtr);
      }

      break;

    case MLOG_PAGE_REORGANIZE:
    case MLOG_COMP_PAGE_REORGANIZE:
    case MLOG_ZIP_PAGE_REORGANIZE:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index(ptr, end_ptr, type != MLOG_PAGE_REORGANIZE,
                                  &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = btr_parse_page_reorganize(
            ptr, end_ptr, index, type == MLOG_ZIP_PAGE_REORGANIZE, block, mtr);
      }

      break;

    case MLOG_PAGE_CREATE:
    case MLOG_COMP_PAGE_CREATE:

      /* Allow anything in page_type when creating a page. */
      ut_a(!page_zip);

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE, FIL_PAGE_INDEX);

      break;

    case MLOG_PAGE_CREATE_RTREE:
    case MLOG_COMP_PAGE_CREATE_RTREE:

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_RTREE,
                        FIL_PAGE_RTREE);

      break;

    case MLOG_PAGE_CREATE_SDI:
    case MLOG_COMP_PAGE_CREATE_SDI:

      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_SDI, FIL_PAGE_SDI);

      break;

    case MLOG_UNDO_INSERT:

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);

      break;

    case MLOG_UNDO_ERASE_END:

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);

      break;

    case MLOG_UNDO_INIT:

      /* Allow anything in page_type when creating a page. */

      ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);

      break;
    case MLOG_UNDO_HDR_CREATE:
    case MLOG_UNDO_HDR_REUSE:

      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);

      ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);

      break;

    case MLOG_REC_MIN_MARK:
    case MLOG_COMP_REC_MIN_MARK:

      ut_ad(!page || fil_page_type_is_index(page_type));

      /* On a compressed page, MLOG_COMP_REC_MIN_MARK
      will be followed by MLOG_COMP_REC_DELETE
      or MLOG_ZIP_WRITE_HEADER(FIL_PAGE_PREV, FIL_nullptr)
      in the same mini-transaction. */

      ut_a(type == MLOG_COMP_REC_MIN_MARK || !page_zip);

      ptr = btr_parse_set_min_rec_mark(
          ptr, end_ptr, type == MLOG_COMP_REC_MIN_MARK, page, mtr);

      break;

    case MLOG_REC_DELETE:
    case MLOG_COMP_REC_DELETE:

      ut_ad(!page || fil_page_type_is_index(page_type));

      if (nullptr !=
          (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_DELETE,
                                  &index))) {
        ut_a(!page ||
             (ibool) !!page_is_comp(page) == dict_table_is_comp(index->table));

        ptr = page_cur_parse_delete_rec(ptr, end_ptr, block, index, mtr);
      }

      break;

    case MLOG_IBUF_BITMAP_INIT:

      /* Allow anything in page_type when creating a page. */

      ptr = ibuf_parse_bitmap_init(ptr, end_ptr, block, mtr);

      break;

    case MLOG_INIT_FILE_PAGE:
    case MLOG_INIT_FILE_PAGE2:

      /* Allow anything in page_type when creating a page. */

      ptr = fsp_parse_init_file_page(ptr, end_ptr, block);

      break;

    case MLOG_WRITE_STRING:

      ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED || page_no == 0);

      ptr = mlog_parse_string(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_WRITE_NODE_PTR:

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_node_ptr(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_WRITE_BLOB_PTR:

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_blob_ptr(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_WRITE_HEADER:

      ut_ad(!page || fil_page_type_is_index(page_type));

      ptr = page_zip_parse_write_header(ptr, end_ptr, page, page_zip);

      break;

    case MLOG_ZIP_PAGE_COMPRESS:

      /* Allow anything in page_type when creating a page. */
      ptr = page_zip_parse_compress(ptr, end_ptr, page, page_zip);
      break;

    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, true, &index))) {
        ut_a(!page || ((ibool) !!page_is_comp(page) ==
                       dict_table_is_comp(index->table)));

        ptr = page_zip_parse_compress_no_data(ptr, end_ptr, page, page_zip,
                                              index);
      }

      break;

    case MLOG_TEST:
#ifndef UNIV_HOTBACKUP
      if (log_test != nullptr) {
        ptr = log_test->parse_mlog_rec(ptr, end_ptr);
        break;
      }
#endif /* !UNIV_HOTBACKUP */
       /* Fall through. */

    default:
      ptr = nullptr;
      recv_sys->found_corrupt_log = true;
  }

  if (index != nullptr) {
    dict_table_t *table = index->table;

    dict_mem_index_free(index);
    dict_mem_table_free(table);
  }

  return (ptr);
}

/** Adds a new log record to the hash table of log records.
@param[in]	type		log record type
@param[in]	space_id	Tablespace id
@param[in]	page_no		page number
@param[in]	body		log record body
@param[in]	rec_end		log record end
@param[in]	start_lsn	start lsn of the mtr
@param[in]	end_lsn		end lsn of the mtr */
static void recv_add_to_hash_table(mlog_id_t type, space_id_t space_id,
                                   page_no_t page_no, byte *body, byte *rec_end,
                                   lsn_t start_lsn, lsn_t end_lsn) {
  ut_ad(type != MLOG_FILE_DELETE);
  ut_ad(type != MLOG_FILE_CREATE);
  ut_ad(type != MLOG_FILE_RENAME);
  ut_ad(type != MLOG_DUMMY_RECORD);
  ut_ad(type != MLOG_INDEX_LOAD);

  recv_sys_t::Space *space;

  space = recv_get_page_map(space_id, true);

  recv_t *recv;

  recv = static_cast<recv_t *>(mem_heap_alloc(space->m_heap, sizeof(*recv)));

  recv->type = type;
  recv->end_lsn = end_lsn;
  recv->len = rec_end - body;
  recv->start_lsn = start_lsn;

  auto it = space->m_pages.find(page_no);

  recv_addr_t *recv_addr;

  if (it != space->m_pages.end()) {
    recv_addr = it->second;

  } else {
    recv_addr = static_cast<recv_addr_t *>(
        mem_heap_alloc(space->m_heap, sizeof(*recv_addr)));

    recv_addr->space = space_id;
    recv_addr->page_no = page_no;
    recv_addr->state = RECV_NOT_PROCESSED;

    UT_LIST_INIT(recv_addr->rec_list, &recv_t::rec_list);

    using value_type = recv_sys_t::Pages::value_type;

    space->m_pages.insert(it, value_type(page_no, recv_addr));

    ++recv_sys->n_addrs;
  }

  UT_LIST_ADD_LAST(recv_addr->rec_list, recv);

  recv_data_t **prev_field;

  prev_field = &recv->data;

  /* Store the log record body in chunks of less than UNIV_PAGE_SIZE:
  the heap grows into the buffer pool, and bigger chunks could not
  be allocated */

  while (rec_end > body) {
    ulint len = rec_end - body;

    if (len > RECV_DATA_BLOCK_SIZE) {
      len = RECV_DATA_BLOCK_SIZE;
    }

    recv_data_t *recv_data;

    recv_data = static_cast<recv_data_t *>(
        mem_heap_alloc(space->m_heap, sizeof(*recv_data) + len));

    *prev_field = recv_data;

    memcpy(recv_data + 1, body, len);

    prev_field = &recv_data->next;

    body += len;
  }

  *prev_field = nullptr;
}

/** Copies the log record body from recv to buf.
@param[in]	buf		Buffer of length at least recv->len
@param[in]	recv		Log record */
static void recv_data_copy_to_buf(byte *buf, recv_t *recv) {
  ulint len = recv->len;
  recv_data_t *recv_data = recv->data;

  while (len > 0) {
    ulint part_len;

    if (len > RECV_DATA_BLOCK_SIZE) {
      part_len = RECV_DATA_BLOCK_SIZE;
    } else {
      part_len = len;
    }

    memcpy(buf, ((byte *)recv_data) + sizeof(*recv_data), part_len);

    buf += part_len;
    len -= part_len;

    recv_data = recv_data->next;
  }
}

/** Applies the hashed log records to the page, if the page lsn is less than the
lsn of a log record. This can be called when a buffer page has just been
read in, or also for a page already in the buffer pool.
@param[in]	just_read_in	true if the IO handler calls this for a freshly
                                read page
@param[in,out]	block		Buffer block */
void recv_recover_page_func(
#ifndef UNIV_HOTBACKUP
    bool just_read_in,
#endif /* !UNIV_HOTBACKUP */
    buf_block_t *block) {
#if defined (UNIV_PMEMOBJ_PART_PL)
	/*redirect the begin of this function to use our funcitons*/
	if (!gb_pmw->ppl->is_new){
		buf_page_t* bpage = (buf_page_t*) block;

		PMEM_PAGE_LOG_HASHED_LINE* pline;

		pline = pm_ppl_get_line_from_key(
				gb_pmw->pop, gb_pmw->ppl,
				bpage->id.fold());

		assert(pline != NULL);
		
		if (IS_GLOBAL_HASHTABLE){
			/*the global hashtable approach*/
			return pm_ppl_recv_recover_page_func(gb_pmw->pop, gb_pmw->ppl, NULL, just_read_in, block);
		} else {
			return pm_ppl_recv_recover_page_func(gb_pmw->pop, gb_pmw->ppl, pline, just_read_in, block);
		}
	}
#endif //UNIV_PMEMOBJ_PART_PL

  mutex_enter(&recv_sys->mutex);

  if (recv_sys->apply_log_recs == false) {
    /* Log records should not be applied now */

    mutex_exit(&recv_sys->mutex);

    return;
  }

  recv_addr_t *recv_addr;

  recv_addr = recv_get_rec(block->page.id.space(), block->page.id.page_no());

  if (recv_addr == nullptr || recv_addr->state == RECV_BEING_PROCESSED ||
      recv_addr->state == RECV_PROCESSED) {
#ifndef UNIV_HOTBACKUP
    ut_ad(recv_addr == nullptr || recv_needed_recovery ||
          recv_sys->scanned_lsn < recv_sys->checkpoint_lsn);
#endif /* !UNIV_HOTBACKUP */

    mutex_exit(&recv_sys->mutex);

    return;
  }

#ifndef UNIV_HOTBACKUP
  buf_page_t bpage = block->page;

  if (!fsp_is_system_temporary(bpage.id.space()) &&
      (arch_page_sys != nullptr && arch_page_sys->is_active())) {
    page_t *frame;
    lsn_t frame_lsn;

    frame = bpage.zip.data;

    if (!frame) {
      frame = block->frame;
    }
    frame_lsn = mach_read_from_8(frame + FIL_PAGE_LSN);

    arch_page_sys->track_page(&bpage, LSN_MAX, frame_lsn, true);
  }
#endif /* !UNIV_HOTBACKUP */

#ifndef UNIV_HOTBACKUP
  /* this is explicitly false in case of meb, skip the assert */
  ut_ad(recv_needed_recovery ||
        recv_sys->scanned_lsn < recv_sys->checkpoint_lsn);

  DBUG_PRINT("ib_log", ("Applying log to page %u:%u", recv_addr->space,
                        recv_addr->page_no));

#ifdef UNIV_DEBUG
  lsn_t max_lsn;

  ut_d(max_lsn = log_sys->scanned_lsn);
#endif /* UNIV_DEBUG */
#else  /* !UNIV_HOTBACKUP */
  ib::trace_2() << "Applying log to space " << recv_addr->space << " page "
                << recv_addr->page_no;
#endif /* !UNIV_HOTBACKUP */

  recv_addr->state = RECV_BEING_PROCESSED;

  mutex_exit(&recv_sys->mutex);

  mtr_t mtr;

  mtr_start(&mtr);

  mtr_set_log_mode(&mtr, MTR_LOG_NONE);

  page_t *page = block->frame;

  page_zip_des_t *page_zip = buf_block_get_page_zip(block);

#ifndef UNIV_HOTBACKUP
  if (just_read_in) {
    /* Move the ownership of the x-latch on the page to
    this OS thread, so that we can acquire a second
    x-latch on it.  This is needed for the operations to
    the page to pass the debug checks. */

    rw_lock_x_lock_move_ownership(&block->lock);
  }

  bool success = buf_page_get_known_nowait(
      RW_X_LATCH, block, Cache_hint::KEEP_OLD, __FILE__, __LINE__, &mtr);
  ut_a(success);

  buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);
#endif /* !UNIV_HOTBACKUP */

  /* Read the newest modification lsn from the page */
  lsn_t page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

#ifndef UNIV_HOTBACKUP

  /* It may be that the page has been modified in the buffer
  pool: read the newest modification LSN there */

  lsn_t page_newest_lsn;

  page_newest_lsn = buf_page_get_newest_modification(&block->page);

  if (page_newest_lsn) {
    page_lsn = page_newest_lsn;
  }
#else  /* !UNIV_HOTBACKUP */
  /* In recovery from a backup we do not really use the buffer pool */
  lsn_t page_newest_lsn = 0;
  /* Count applied and skipped log records */
  size_t applied_recs = 0;
  size_t skipped_recs = 0;
#endif /* !UNIV_HOTBACKUP */

#ifndef UNIV_HOTBACKUP
  lsn_t end_lsn = 0;
#endif /* !UNIV_HOTBACKUP */
  lsn_t start_lsn = 0;
  bool modification_to_page = false;

  for (auto recv = UT_LIST_GET_FIRST(recv_addr->rec_list); recv != nullptr;
       recv = UT_LIST_GET_NEXT(rec_list, recv)) {
#ifndef UNIV_HOTBACKUP
    end_lsn = recv->end_lsn;

    ut_ad(end_lsn <= max_lsn);
#endif /* !UNIV_HOTBACKUP */

    byte *buf;

    if (recv->len > RECV_DATA_BLOCK_SIZE) {
      /* We have to copy the record body to a separate
      buffer */

      buf = static_cast<byte *>(ut_malloc_nokey(recv->len));

      recv_data_copy_to_buf(buf, recv);
    } else {
      buf = ((byte *)(recv->data)) + sizeof(recv_data_t);
    }

    if (recv->type == MLOG_INIT_FILE_PAGE) {
      page_lsn = page_newest_lsn;

      memset(FIL_PAGE_LSN + page, 0, 8);
      memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page, 0, 8);

      if (page_zip) {
        memset(FIL_PAGE_LSN + page_zip->data, 0, 8);
      }
    }

    /* Ignore applying the redo logs for tablespace that is
    truncated. Truncated tablespaces are handled explicitly
    post-recovery, where we will restore the tablespace back
    to a normal state.

    Applying redo at this stage will cause problems because the
    redo will have action recorded on page before tablespace
    was re-inited and that would lead to a problem later. */

    if (recv->start_lsn >= page_lsn
#ifndef UNIV_HOTBACKUP
        && undo::is_active(recv_addr->space)
#endif /* !UNIV_HOTBACKUP */
    ) {

      lsn_t end_lsn;

      if (!modification_to_page) {
#ifndef UNIV_HOTBACKUP
        ut_a(recv_needed_recovery);
#endif /* !UNIV_HOTBACKUP */
        modification_to_page = true;
        start_lsn = recv->start_lsn;
      }

      DBUG_PRINT("ib_log", ("apply " LSN_PF ":"
                            " %s len " ULINTPF " page %u:%u",
                            recv->start_lsn, get_mlog_string(recv->type),
                            recv->len, recv_addr->space, recv_addr->page_no));

      recv_parse_or_apply_log_rec_body(recv->type, buf, buf + recv->len,
                                       recv_addr->space, recv_addr->page_no,
                                       block, &mtr, ULINT_UNDEFINED);

      end_lsn = recv->start_lsn + recv->len;

      mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);

      mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page,
                      end_lsn);

      if (page_zip) {
        mach_write_to_8(FIL_PAGE_LSN + page_zip->data, end_lsn);
      }
#ifdef UNIV_HOTBACKUP
      ++applied_recs;
    } else {
      ++skipped_recs;
#endif /* UNIV_HOTBACKUP */
    }

    if (recv->len > RECV_DATA_BLOCK_SIZE) {
      ut_free(buf);
    }
  }

#ifdef UNIV_ZIP_DEBUG
  if (fil_page_index_page_check(page)) {
    page_zip_des_t *page_zip = buf_block_get_page_zip(block);

    ut_a(!page_zip || page_zip_validate_low(page_zip, page, nullptr, FALSE));
  }
#endif /* UNIV_ZIP_DEBUG */

#ifndef UNIV_HOTBACKUP
  if (modification_to_page) {
    buf_flush_recv_note_modification(block, start_lsn, end_lsn);
  }
#else  /* !UNIV_HOTBACKUP */
  UT_NOT_USED(start_lsn);
#endif /* !UNIV_HOTBACKUP */

  /* Make sure that committing mtr does not change the modification
  LSN values of page */

  mtr.discard_modifications();

  mtr_commit(&mtr);

  mutex_enter(&recv_sys->mutex);

  if (recv_max_page_lsn < page_lsn) {
    recv_max_page_lsn = page_lsn;
  }

  recv_addr->state = RECV_PROCESSED;

  ut_a(recv_sys->n_addrs > 0);
  --recv_sys->n_addrs;

  mutex_exit(&recv_sys->mutex);

#ifdef UNIV_HOTBACKUP
  ib::trace_2() << "Applied " << applied_recs << " Skipped " << skipped_recs;
#endif /* UNIV_HOTBACKUP */
}

/** Tries to parse a single log record.
@param[out]	type		log record type
@param[in]	ptr		pointer to a buffer
@param[in]	end_ptr		end of the buffer
@param[out]	space_id	tablespace identifier
@param[out]	page_no		page number
@param[out]	body		start of log record body
@return length of the record, or 0 if the record was not complete */
static ulint recv_parse_log_rec(mlog_id_t *type, byte *ptr, byte *end_ptr,
                                space_id_t *space_id, page_no_t *page_no,
                                byte **body) {
  byte *new_ptr;

  *body = nullptr;

  UNIV_MEM_INVALID(type, sizeof *type);
  UNIV_MEM_INVALID(space_id, sizeof *space_id);
  UNIV_MEM_INVALID(page_no, sizeof *page_no);
  UNIV_MEM_INVALID(body, sizeof *body);

  if (ptr == end_ptr) {
    return (0);
  }

  switch (*ptr) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN | MLOG_SINGLE_REC_FLAG:
    case MLOG_LSN:

      new_ptr =
          mlog_parse_initial_log_record(ptr, end_ptr, type, space_id, page_no);

      if (new_ptr != nullptr) {
        const lsn_t lsn = static_cast<lsn_t>(*space_id) << 32 | *page_no;

        ut_a(lsn == recv_sys->recovered_lsn);
      }

      *type = MLOG_LSN;
      return (new_ptr == nullptr ? 0 : new_ptr - ptr);
#endif /* UNIV_LOG_LSN_DEBUG */

    case MLOG_MULTI_REC_END:
    case MLOG_DUMMY_RECORD:
      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;
      *type = static_cast<mlog_id_t>(*ptr);
      return (1);

    case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
    case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
      recv_sys->found_corrupt_log = true;
      return (0);

    case MLOG_TABLE_DYNAMIC_META:
    case MLOG_TABLE_DYNAMIC_META | MLOG_SINGLE_REC_FLAG:

      table_id_t id;
      uint64 version;

      *page_no = FIL_NULL;
      *space_id = SPACE_UNKNOWN;

      new_ptr =
          mlog_parse_initial_dict_log_record(ptr, end_ptr, type, &id, &version);

      if (new_ptr != nullptr) {
        new_ptr = recv_sys->metadata_recover->parseMetadataLog(
            id, version, new_ptr, end_ptr);
      }

      return (new_ptr == nullptr ? 0 : new_ptr - ptr);
  }

  new_ptr =
      mlog_parse_initial_log_record(ptr, end_ptr, type, space_id, page_no);

  *body = new_ptr;

  if (new_ptr == nullptr) {
    return (0);
  }

  new_ptr = recv_parse_or_apply_log_rec_body(*type, new_ptr, end_ptr, *space_id,
                                             *page_no, nullptr, nullptr,
                                             new_ptr - ptr);

  if (new_ptr == nullptr) {
    return (0);
  }

  return (new_ptr - ptr);
}

/** Subtracts next number of bytes to ignore before we reach the checkpoint
or returns information that there was nothing more to skip.
@param[in]	next_parsed_bytes	number of next bytes that were parsed,
which are supposed to be subtracted from bytes to ignore before checkpoint
@retval	true	there were still bytes to ignore
@retval false	there was already 0 bytes to ignore, nothing changed. */
static bool recv_update_bytes_to_ignore_before_checkpoint(
    size_t next_parsed_bytes) {
  auto &to_ignore = recv_sys->bytes_to_ignore_before_checkpoint;

  if (to_ignore != 0) {
    if (to_ignore >= next_parsed_bytes) {
      to_ignore -= next_parsed_bytes;
    } else {
      to_ignore = 0;
    }
    return (true);
  }

  return (false);
}

/** Parse and store a single log record entry.
@param[in]	ptr		start of buffer
@param[in]	end_ptr		end of buffer
@return true if end of processing */
static bool recv_single_rec(byte *ptr, byte *end_ptr) {
  /* The mtr did not modify multiple pages */

  lsn_t old_lsn = recv_sys->recovered_lsn;

  /* Try to parse a log record, fetching its type, space id,
  page no, and a pointer to the body of the log record */

  byte *body;
  mlog_id_t type;
  page_no_t page_no;
  space_id_t space_id;

  ulint len =
      recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);

  if (recv_sys->found_corrupt_log) {
    recv_report_corrupt_log(ptr, type, space_id, page_no);

#ifdef UNIV_HOTBACKUP
    return (true);
#endif /* UNIV_HOTBACKUP */

  } else if (len == 0 || recv_sys->found_corrupt_fs) {
    return (true);
  }

  lsn_t new_recovered_lsn;

  new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

  if (new_recovered_lsn > recv_sys->scanned_lsn) {
    /* The log record filled a log block, and we
    require that also the next log block should
    have been scanned in */

    return (true);
  }

  recv_previous_parsed_rec_type = type;
  recv_previous_parsed_rec_is_multi = 0;
  recv_previous_parsed_rec_offset = recv_sys->recovered_offset;

  recv_sys->recovered_offset += len;
  recv_sys->recovered_lsn = new_recovered_lsn;

  if (recv_update_bytes_to_ignore_before_checkpoint(len)) {
    return (false);
  }

  switch (type) {
    case MLOG_DUMMY_RECORD:
      /* Do nothing */
      break;

#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      /* Do not add these records to the hash table.
      The page number and space id fields are misused
      for something else. */
      break;
#endif /* UNIV_LOG_LSN_DEBUG */

    default:

      if (recv_recovery_on) {
#ifndef UNIV_HOTBACKUP
        if (space_id == TRX_SYS_SPACE ||
            fil_tablespace_lookup_for_recovery(space_id)) {
#endif /* !UNIV_HOTBACKUP */

          recv_add_to_hash_table(type, space_id, page_no, body, ptr + len,
                                 old_lsn, recv_sys->recovered_lsn);

#ifndef UNIV_HOTBACKUP
        } else {
          recv_sys->missing_ids.insert(space_id);
        }
#endif /* !UNIV_HOTBACKUP */
      }

      /* fall through */

    case MLOG_INDEX_LOAD:
    case MLOG_FILE_DELETE:
    case MLOG_FILE_RENAME:
    case MLOG_FILE_CREATE:
    case MLOG_TABLE_DYNAMIC_META:

      /* These were already handled by
      recv_parse_log_rec() and
      recv_parse_or_apply_log_rec_body(). */

      DBUG_PRINT("ib_log",
                 ("scan " LSN_PF ": log rec %s"
                  " len " ULINTPF " " PAGE_ID_PF,
                  old_lsn, get_mlog_string(type), len, space_id, page_no));
      break;
  }

  return (false);
}

/** Parse and store a multiple record log entry.
@param[in]	ptr		start of buffer
@param[in]	end_ptr		end of buffer
@return true if end of processing */
static bool recv_multi_rec(byte *ptr, byte *end_ptr) {
  /* Check that all the records associated with the single mtr
  are included within the buffer */

  ulint n_recs = 0;
  ulint total_len = 0;

  for (;;) {
    mlog_id_t type;
    byte *body;
    page_no_t page_no;
    space_id_t space_id;

    ulint len =
        recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);

    if (recv_sys->found_corrupt_log) {
      recv_report_corrupt_log(ptr, type, space_id, page_no);

      return (true);

    } else if (len == 0) {
      return (true);

    } else if ((*ptr & MLOG_SINGLE_REC_FLAG)) {
      recv_sys->found_corrupt_log = true;

      recv_report_corrupt_log(ptr, type, space_id, page_no);

      return (true);

    } else if (recv_sys->found_corrupt_fs) {
      return (true);
    }

    recv_previous_parsed_rec_type = type;

    recv_previous_parsed_rec_offset = recv_sys->recovered_offset + total_len;

    recv_previous_parsed_rec_is_multi = 1;

    total_len += len;
    ++n_recs;

    ptr += len;

    if (type == MLOG_MULTI_REC_END) {
      DBUG_PRINT("ib_log", ("scan " LSN_PF ": multi-log end total_len " ULINTPF
                            " n=" ULINTPF,
                            recv_sys->recovered_lsn, total_len, n_recs));

      break;
    }

    DBUG_PRINT("ib_log",
               ("scan " LSN_PF ": multi-log rec %s len " ULINTPF " " PAGE_ID_PF,
                recv_sys->recovered_lsn, get_mlog_string(type), len, space_id,
                page_no));
  }

  lsn_t new_recovered_lsn =
      recv_calc_lsn_on_data_add(recv_sys->recovered_lsn, total_len);

  if (new_recovered_lsn > recv_sys->scanned_lsn) {
    /* The log record filled a log block, and we require
    that also the next log block should have been scanned in */

    return (true);
  }

  /* Add all the records to the hash table */

  ptr = recv_sys->buf + recv_sys->recovered_offset;

  for (;;) {
    lsn_t old_lsn = recv_sys->recovered_lsn;

    /* This will apply MLOG_FILE_ records. */

    mlog_id_t type;
    byte *body;
    page_no_t page_no;
    space_id_t space_id;

    ulint len =
        recv_parse_log_rec(&type, ptr, end_ptr, &space_id, &page_no, &body);

    if (recv_sys->found_corrupt_log &&
        !recv_report_corrupt_log(ptr, type, space_id, page_no)) {
      return (true);

    } else if (recv_sys->found_corrupt_fs) {
      return (true);
    }

    ut_a(len != 0);
    ut_a(!(*ptr & MLOG_SINGLE_REC_FLAG));

    recv_sys->recovered_offset += len;

    recv_sys->recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

    const bool apply = !recv_update_bytes_to_ignore_before_checkpoint(len);

    switch (type) {
      case MLOG_MULTI_REC_END:

        /* Found the end mark for the records */
        return (false);

#ifdef UNIV_LOG_LSN_DEBUG
      case MLOG_LSN:
        /* Do not add these records to the hash table.
        The page number and space id fields are misused
        for something else. */
        break;
#endif /* UNIV_LOG_LSN_DEBUG */

      case MLOG_FILE_DELETE:
      case MLOG_FILE_CREATE:
      case MLOG_FILE_RENAME:
      case MLOG_TABLE_DYNAMIC_META:
        /* case MLOG_TRUNCATE: Disabled for WL6378 */
        /* These were already handled by
        recv_parse_or_apply_log_rec_body(). */
        break;

      default:

        if (!apply) {
          break;
        }

        if (recv_recovery_on) {
#ifndef UNIV_HOTBACKUP
          if (space_id == TRX_SYS_SPACE ||
              fil_tablespace_lookup_for_recovery(space_id)) {
#endif /* !UNIV_HOTBACKUP */

            recv_add_to_hash_table(type, space_id, page_no, body, ptr + len,
                                   old_lsn, new_recovered_lsn);

#ifndef UNIV_HOTBACKUP
          } else {
            recv_sys->missing_ids.insert(space_id);
          }
#endif /* !UNIV_HOTBACKUP */
        }
    }

    ptr += len;
  }

  return (false);
}

/** Parse log records from a buffer and optionally store them to a
hash table to wait merging to file pages.
@param[in]	checkpoint_lsn	the LSN of the latest checkpoint */
static void recv_parse_log_recs(lsn_t checkpoint_lsn) {
  ut_ad(recv_sys->parse_start_lsn != 0);

  for (;;) {
    byte *ptr = recv_sys->buf + recv_sys->recovered_offset;

    byte *end_ptr = recv_sys->buf + recv_sys->len;

    if (ptr == end_ptr) {
      return;
    }

    bool single_rec;

    switch (*ptr) {
#ifdef UNIV_LOG_LSN_DEBUG
      case MLOG_LSN:
#endif /* UNIV_LOG_LSN_DEBUG */
      case MLOG_DUMMY_RECORD:
        single_rec = true;
        break;
      default:
        single_rec = !!(*ptr & MLOG_SINGLE_REC_FLAG);
    }

    if (single_rec) {
      if (recv_single_rec(ptr, end_ptr)) {
        return;
      }

    } else if (recv_multi_rec(ptr, end_ptr)) {
      return;
    }
  }
}

/** Adds data from a new log block to the parsing buffer of recv_sys if
recv_sys->parse_start_lsn is non-zero.
@param[in]	log_block		log block
@param[in]	scanned_lsn		lsn of how far we were able
                                        to find data in this log block
@return true if more data added */
static bool recv_sys_add_to_parsing_buf(const byte *log_block,
                                        lsn_t scanned_lsn) {
  ut_ad(scanned_lsn >= recv_sys->scanned_lsn);

  if (!recv_sys->parse_start_lsn) {
    /* Cannot start parsing yet because no start point for
    it found */

    return (false);
  }

  ulint more_len;
  ulint data_len = log_block_get_data_len(log_block);

  if (recv_sys->parse_start_lsn >= scanned_lsn) {
    return (false);

  } else if (recv_sys->scanned_lsn >= scanned_lsn) {
    return (false);

  } else if (recv_sys->parse_start_lsn > recv_sys->scanned_lsn) {
    more_len = (ulint)(scanned_lsn - recv_sys->parse_start_lsn);

  } else {
    more_len = (ulint)(scanned_lsn - recv_sys->scanned_lsn);
  }

  if (more_len == 0) {
    return (false);
  }

  ut_ad(data_len >= more_len);

  ulint start_offset = data_len - more_len;

  if (start_offset < LOG_BLOCK_HDR_SIZE) {
    start_offset = LOG_BLOCK_HDR_SIZE;
  }

  ulint end_offset = data_len;

  if (end_offset > OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    end_offset = OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
  }

  ut_ad(start_offset <= end_offset);

  if (start_offset < end_offset) {
    memcpy(recv_sys->buf + recv_sys->len, log_block + start_offset,
           end_offset - start_offset);

    recv_sys->len += end_offset - start_offset;

    ut_a(recv_sys->len <= recv_sys->buf_len);
  }

  return (true);
}

/** Moves the parsing buffer data left to the buffer start. */
static void recv_reset_buffer() {
  ut_memmove(recv_sys->buf, recv_sys->buf + recv_sys->recovered_offset,
             recv_sys->len - recv_sys->recovered_offset);

  recv_sys->len -= recv_sys->recovered_offset;

  recv_sys->recovered_offset = 0;
}

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.  Unless
UNIV_HOTBACKUP is defined, this function will apply log records
automatically when the hash table becomes full.
@param[in,out]	log		redo log
@param[in]	max_memory	we let the hash table of recs to grow to
                                this size, at the maximum
@param[in]	buf		buffer containing a log segment or garbage
@param[in]	len		buffer length
@param[in]	checkpoint_lsn	latest checkpoint LSN
@param[in]	start_lsn	buffer start lsn
@param[in,out]	contiguous_lsn	it is known that log contain
                                contiguous log data up to this lsn
@param[out]	read_upto_lsn	scanning succeeded up to this lsn
@return true if not able to scan any more in this log */
#ifndef UNIV_HOTBACKUP
static bool recv_scan_log_recs(log_t &log,
#else  /* !UNIV_HOTBACKUP */
bool meb_scan_log_recs(
#endif /* !UNIV_HOTBACKUP */
                               ulint max_memory, const byte *buf, ulint len,
                               lsn_t checkpoint_lsn, lsn_t start_lsn,
                               lsn_t *contiguous_lsn, lsn_t *read_upto_lsn) {
  const byte *log_block = buf;
  lsn_t scanned_lsn = start_lsn;
  bool finished = false;
  bool more_data = false;

  ut_ad(start_lsn % OS_FILE_LOG_BLOCK_SIZE == 0);
  ut_ad(len % OS_FILE_LOG_BLOCK_SIZE == 0);
  ut_ad(len >= OS_FILE_LOG_BLOCK_SIZE);

  do {
    ut_ad(!finished);

    ulint no = log_block_get_hdr_no(log_block);

    ulint expected_no = log_block_convert_lsn_to_no(scanned_lsn);

    if (no != expected_no) {
      /* Garbage or an incompletely written log block.

      We will not report any error, because this can
      happen when InnoDB was killed while it was
      writing redo log. We simply treat this as an
      abrupt end of the redo log. */

      finished = true;

      break;
    }

    if (!log_block_checksum_is_ok(log_block)) {
      uint32_t checksum1 = log_block_get_checksum(log_block);
      uint32_t checksum2 = log_block_calc_checksum(log_block);
      ib::error(ER_IB_MSG_720, ulonglong{no}, ulonglong{scanned_lsn},
                ulong{checksum1}, ulong{checksum2});

      /* Garbage or an incompletely written log block.

      This could be the result of killing the server
      while it was writing this log block. We treat
      this as an abrupt end of the redo log. */

      finished = true;

      break;
    }

    if (log_block_get_flush_bit(log_block)) {
      /* This block was a start of a log flush operation:
      we know that the previous flush operation must have
      been completed before this block can have been flushed.
      Therefore, we know that log data is contiguous up to
      scanned_lsn. */

      if (scanned_lsn > *contiguous_lsn) {
        *contiguous_lsn = scanned_lsn;
      }
    }

    ulint data_len = log_block_get_data_len(log_block);

    if (scanned_lsn + data_len > recv_sys->scanned_lsn &&
        log_block_get_checkpoint_no(log_block) <
            recv_sys->scanned_checkpoint_no &&
        (recv_sys->scanned_checkpoint_no -
             log_block_get_checkpoint_no(log_block) >
         0x80000000UL)) {
      /* Garbage from a log buffer flush which was made
      before the most recent database recovery */

      finished = true;

      break;
    }

    if (!recv_sys->parse_start_lsn &&
        log_block_get_first_rec_group(log_block) > 0) {
      /* We found a point from which to start the parsing
      of log records */

      recv_sys->parse_start_lsn =
          scanned_lsn + log_block_get_first_rec_group(log_block);

      ib::info(ER_IB_MSG_1261)
          << "Starting to parse redo log at lsn = " << recv_sys->parse_start_lsn
          << ", whereas checkpoint_lsn = " << recv_sys->checkpoint_lsn;

      if (recv_sys->parse_start_lsn < recv_sys->checkpoint_lsn) {
        /* We start to parse log records even before
        checkpoint_lsn, from the beginning of the log
        block which contains the checkpoint_lsn.

        That's because the first group of log records
        in the log block, starts before checkpoint_lsn,
        and checkpoint_lsn could potentially point to
        the middle of some log record. We need to find
        the first group of log records that starts at
        or after checkpoint_lsn. This could be only
        achieved by traversing all groups of log records
        that start within the log block since the first
        one (to discover their beginnings we need to
        parse them). However, we don't want to report
        missing tablespaces for space_id in log records
        before checkpoint_lsn. Hence we need to ignore
        those records and that's why we need a counter
        of bytes to ignore. */

        recv_sys->bytes_to_ignore_before_checkpoint =
            recv_sys->checkpoint_lsn - recv_sys->parse_start_lsn;

        ut_a(recv_sys->bytes_to_ignore_before_checkpoint <=
             OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE);

        ut_a(recv_sys->checkpoint_lsn % OS_FILE_LOG_BLOCK_SIZE +
                 LOG_BLOCK_TRL_SIZE <
             OS_FILE_LOG_BLOCK_SIZE);

        ut_a(recv_sys->parse_start_lsn % OS_FILE_LOG_BLOCK_SIZE >=
             LOG_BLOCK_HDR_SIZE);
      }

      recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
      recv_sys->recovered_lsn = recv_sys->parse_start_lsn;
    }

    scanned_lsn += data_len;

    if (scanned_lsn > recv_sys->scanned_lsn) {
#ifndef UNIV_HOTBACKUP
      if (srv_read_only_mode) {
        if (scanned_lsn > recv_sys->checkpoint_lsn) {
          ib::warn(ER_IB_MSG_721);

          return (true);
        }

      } else if (!recv_needed_recovery &&
                 scanned_lsn > recv_sys->checkpoint_lsn) {
        ib::info(ER_IB_MSG_722, ulonglong{recv_sys->scanned_lsn});

        recv_init_crash_recovery();
      }
#endif /* !UNIV_HOTBACKUP */

      /* We were able to find more log data: add it to the
      parsing buffer if parse_start_lsn is already
      non-zero */

      if (recv_sys->len + 4 * OS_FILE_LOG_BLOCK_SIZE >= recv_sys->buf_len) {
        if (!recv_sys_resize_buf()) {
          recv_sys->found_corrupt_log = true;

#ifndef UNIV_HOTBACKUP
          if (srv_force_recovery == 0) {
            ib::error(ER_IB_MSG_724);
            return (true);
          }
#endif /* !UNIV_HOTBACKUP */
        }
      }

      if (!recv_sys->found_corrupt_log) {
        more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn);
      }

      recv_sys->scanned_lsn = scanned_lsn;

      recv_sys->scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
    }

    if (data_len < OS_FILE_LOG_BLOCK_SIZE) {
      /* Log data for this group ends here */
      finished = true;

      break;

    } else {
      log_block += OS_FILE_LOG_BLOCK_SIZE;
    }

  } while (log_block < buf + len);

  *read_upto_lsn = scanned_lsn;

  if (recv_needed_recovery ||
      (recv_is_from_backup && !recv_is_making_a_backup)) {
    ++recv_scan_print_counter;

    if (finished || (recv_scan_print_counter % 80) == 0) {
      ib::info(ER_IB_MSG_725, ulonglong{scanned_lsn});
    }
  }

  if (more_data && !recv_sys->found_corrupt_log) {
    /* Try to parse more log records */

    recv_parse_log_recs(checkpoint_lsn);

#ifndef UNIV_HOTBACKUP
    if (recv_heap_used() > max_memory) {
#if defined (UNIV_TRACE_RECOVERY_TIME)
	 //trace the apply time between parse phases
	  ulint t1 = ut_time_us(NULL);
#endif

      recv_apply_hashed_log_recs(log, false);

#if defined (UNIV_TRACE_RECOVERY_TIME)
	ulint t2 = ut_time_us(NULL);
	recv_sys->recv_extra_apply_time += (t2 - t1);
#endif
    }
#endif /* !UNIV_HOTBACKUP */

    if (recv_sys->recovered_offset > recv_sys->buf_len / 4) {
      /* Move parsing buffer data to the buffer start */

      recv_reset_buffer();
    }
  }

  return (finished);
}

#ifndef UNIV_HOTBACKUP
/** Reads a specified log segment to a buffer.
@param[in,out]	log		redo log
@param[in,out]	buf		buffer where to read
@param[in]	start_lsn	read area start
@param[in]	end_lsn		read area end */
static void recv_read_log_seg(log_t &log, byte *buf, lsn_t start_lsn,
                              lsn_t end_lsn) {
  log_background_threads_inactive_validate(log);

  do {
    lsn_t source_offset;

    source_offset = log_files_real_offset_for_lsn(log, start_lsn);

    ut_a(end_lsn - start_lsn <= ULINT_MAX);

    ulint len;

    len = (ulint)(end_lsn - start_lsn);

    ut_ad(len != 0);

    if ((source_offset % log.file_size) + len > log.file_size) {
      /* If the above condition is true then len
      (which is ulint) is > the expression below,
      so the typecast is ok */
      len = (ulint)(log.file_size - (source_offset % log.file_size));
    }

    ++log.n_log_ios;

    ut_a(source_offset / UNIV_PAGE_SIZE <= PAGE_NO_MAX);

    const page_no_t page_no =
        static_cast<page_no_t>(source_offset / univ_page_size.physical());

    dberr_t

        err = fil_redo_io(
            IORequestLogRead, page_id_t(log.files_space_id, page_no),
            univ_page_size, (ulint)(source_offset % univ_page_size.physical()),
            len, buf);

    ut_a(err == DB_SUCCESS);

    start_lsn += len;
    buf += len;

  } while (start_lsn != end_lsn);
}

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.
@param[in,out]	log			redo log
@param[in,out]	contiguous_lsn		log sequence number
                                        until which all redo log has been
                                        scanned */
static void recv_recovery_begin(log_t &log, lsn_t *contiguous_lsn) {
  mutex_enter(&recv_sys->mutex);

  recv_sys->len = 0;
  recv_sys->recovered_offset = 0;
  recv_sys->n_addrs = 0;
  recv_sys_empty_hash();

  /* Since 8.0, we can start recovery at checkpoint_lsn which points
  to the middle of log record. In such case we first to need to find
  the beginning of the first group of log records, which is at lsn
  greater than the checkpoint_lsn. */
  recv_sys->parse_start_lsn = 0;

  /* This is updated when we find value for parse_start_lsn. */
  recv_sys->bytes_to_ignore_before_checkpoint = 0;

  recv_sys->checkpoint_lsn = *contiguous_lsn;
  recv_sys->scanned_lsn = *contiguous_lsn;
  recv_sys->recovered_lsn = *contiguous_lsn;

  recv_sys->scanned_checkpoint_no = 0;
  recv_previous_parsed_rec_type = MLOG_SINGLE_REC_FLAG;
  recv_previous_parsed_rec_offset = 0;
  recv_previous_parsed_rec_is_multi = 0;
  ut_ad(recv_max_page_lsn == 0);

  mutex_exit(&recv_sys->mutex);

  ulint max_mem =
      UNIV_PAGE_SIZE * (buf_pool_get_n_pages() -
                        (recv_n_pool_free_frames * srv_buf_pool_instances));

  *contiguous_lsn =
      ut_uint64_align_down(*contiguous_lsn, OS_FILE_LOG_BLOCK_SIZE);

  lsn_t start_lsn = *contiguous_lsn;

  lsn_t checkpoint_lsn = start_lsn;

  bool finished = false;

  while (!finished) {
    lsn_t end_lsn = start_lsn + RECV_SCAN_SIZE;

    recv_read_log_seg(log, log.buf, start_lsn, end_lsn);

    finished = recv_scan_log_recs(log, max_mem, log.buf, RECV_SCAN_SIZE,
                                  checkpoint_lsn, start_lsn, contiguous_lsn,
                                  &log.scanned_lsn);

    start_lsn = end_lsn;
  }

  DBUG_PRINT("ib_log", ("scan " LSN_PF " completed", log.scanned_lsn));
}

/** Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static void recv_init_crash_recovery() {
  ut_ad(!srv_read_only_mode);
  ut_a(!recv_needed_recovery);

  recv_needed_recovery = true;

  ib::info(ER_IB_MSG_726);
  ib::info(ER_IB_MSG_727);

  buf_dblwr_process();

  if (srv_force_recovery < SRV_FORCE_NO_LOG_REDO) {
    /* Spawn the background thread to flush dirty pages
    from the buffer pools. */

    os_thread_create(recv_writer_thread_key, recv_writer_thread);
  }
}
#endif /* !UNIV_HOTBACKUP */

#if defined (UNIV_PMEMOBJ_PART_PL)
/* ==============================================
 * PL-NVM functions for recovery
 *===============================================
 * */
/*
 * Alternative for log_sys->lsn
 * */
ulint
pm_ppl_get_max_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl)
{
	uint32_t n, k, i, j;
	uint64_t max_lsn;

	max_lsn = 0;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	k = ppl->n_blocks_per_bucket;

    //find low_watermark for each line
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

        //for each block in the line
        for (j = 0; j < k; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);

            if (!plog_block->is_free){
				if (max_lsn < plog_block->lastLSN){
					max_lsn = plog_block->lastLSN;
				}
            }
        } //end for each block in the line

    } //end for each line

	return max_lsn;
}
#endif //UNIV_PMEMOBJ_PART_PL

#ifndef UNIV_HOTBACKUP
/** Start recovering from a redo log checkpoint.
@see recv_recovery_from_checkpoint_finish
@param[in,out]	log		redo log
@param[in]	flush_lsn	FIL_PAGE_FILE_FLUSH_LSN
                                of first system tablespace page
@return error code or DB_SUCCESS */
dberr_t recv_recovery_from_checkpoint_start(log_t &log, lsn_t flush_lsn) {
  /* Initialize red-black tree for fast insertions into the
  flush_list during recovery process. */
  buf_flush_init_flush_rbt();

  if (srv_force_recovery >= SRV_FORCE_NO_LOG_REDO) {
    ib::info(ER_IB_MSG_728);

    /* We leave redo log not started and this is read-only mode. */
    ut_a(log.sn == 0);
    ut_a(srv_read_only_mode);

    return (DB_SUCCESS);
  }

  recv_recovery_on = true;

  /* Look for the latest checkpoint */

  dberr_t err;
  ulint max_cp_field;

  err = recv_find_max_checkpoint(log, &max_cp_field);

  if (err != DB_SUCCESS) {
    return (err);
  }

  log_files_header_read(log, static_cast<uint32_t>(max_cp_field));

  lsn_t checkpoint_lsn;
  checkpoint_no_t checkpoint_no;

  checkpoint_lsn = mach_read_from_8(log.checkpoint_buf + LOG_CHECKPOINT_LSN);

  checkpoint_no = mach_read_from_8(log.checkpoint_buf + LOG_CHECKPOINT_NO);

  /* Read the first log file header to print a note if this is
  a recovery from a restored InnoDB Hot Backup */
  byte log_hdr_buf[LOG_FILE_HDR_SIZE];
  const page_id_t page_id{log.files_space_id, 0};

  err = fil_redo_io(IORequestLogRead, page_id, univ_page_size, 0,
                    LOG_FILE_HDR_SIZE, log_hdr_buf);

  ut_a(err == DB_SUCCESS);

  /* Make sure creator is properly '\0'-terminated for output */
  log_hdr_buf[LOG_HEADER_CREATOR_END - 1] = '\0';

  if (0 == ut_memcmp(log_hdr_buf + LOG_HEADER_CREATOR, (byte *)"MEB",
                     (sizeof "MEB") - 1)) {
    if (srv_read_only_mode) {
      ib::error(ER_IB_MSG_729);

      return (DB_ERROR);
    }

    /* This log file was created by mysqlbackup --restore: print
    a note to the user about it */

    ib::info(ER_IB_MSG_730,
             reinterpret_cast<const char *>(log_hdr_buf) + LOG_HEADER_CREATOR);

    /* Replace the label. */
    ut_ad(LOG_HEADER_CREATOR_END - LOG_HEADER_CREATOR >=
          sizeof LOG_HEADER_CREATOR_CURRENT);

    memset(log_hdr_buf + LOG_HEADER_CREATOR, 0,
           LOG_HEADER_CREATOR_END - LOG_HEADER_CREATOR);

    strcpy(reinterpret_cast<char *>(log_hdr_buf) + LOG_HEADER_CREATOR,
           LOG_HEADER_CREATOR_CURRENT);

    /* Re-calculate the header block checksum. */
    log_block_set_checksum(log_hdr_buf,
                           log_block_calc_checksum_crc32(log_hdr_buf));

    /* Write to the log file to wipe over the label */
    err = fil_redo_io(IORequestLogWrite, page_id, univ_page_size, 0,
                      OS_FILE_LOG_BLOCK_SIZE, log_hdr_buf);

    ut_a(err == DB_SUCCESS);

  } else if (0 == ut_memcmp(log_hdr_buf + LOG_HEADER_CREATOR,
                            (byte *)LOG_HEADER_CREATOR_CLONE,
                            (sizeof LOG_HEADER_CREATOR_CLONE) - 1)) {
    recv_sys->is_cloned_db = true;
    ib::info(ER_IB_MSG_731);
  }

  /* Start reading the log from the checkpoint LSN up.
  The variable contiguous_lsn contains an LSN up to which
  the log is known to be contiguously written to log. */

  ut_ad(RECV_SCAN_SIZE <= log.buf_size);

  ut_ad(recv_sys->n_addrs == 0);

  lsn_t contiguous_lsn;

  contiguous_lsn = checkpoint_lsn;

  switch (log.format) {
    case LOG_HEADER_FORMAT_CURRENT:
      break;

    case LOG_HEADER_FORMAT_5_7_9:
    case LOG_HEADER_FORMAT_8_0_1:

      ib::info(ER_IB_MSG_732, ulong{log.format});

      /* Check if the redo log from an older known redo log
      version is from a clean shutdown. */
      err = recv_log_recover_pre_8_0_4(log, checkpoint_no, checkpoint_lsn);

      return (err);

    default:
      ib::error(ER_IB_MSG_733, ulong{log.format},
                ulong{LOG_HEADER_FORMAT_CURRENT});

      ut_ad(0);
      recv_sys->found_corrupt_log = true;
      return (DB_ERROR);
  }

  /* NOTE: we always do a 'recovery' at startup, but only if
  there is something wrong we will print a message to the
  user about recovery: */

  if (checkpoint_lsn != flush_lsn) {
    if (checkpoint_lsn < flush_lsn) {
      ib::warn(ER_IB_MSG_734, ulonglong{checkpoint_lsn}, ulonglong{flush_lsn});
    }

    if (!recv_needed_recovery) {
      ib::info(ER_IB_MSG_735, ulonglong{flush_lsn}, ulonglong{checkpoint_lsn});

      if (srv_read_only_mode) {
        ib::error(ER_IB_MSG_736);

        return (DB_READ_ONLY);
      }

      recv_init_crash_recovery();
    }
  }

  contiguous_lsn = checkpoint_lsn;

  recv_recovery_begin(log, &contiguous_lsn);

  lsn_t recovered_lsn;

  recovered_lsn = recv_sys->recovered_lsn;

  ut_a(recv_needed_recovery || checkpoint_lsn == recovered_lsn);

  log.recovered_lsn = recovered_lsn;

  /* If it is at block boundary, add header size. */
  auto check_scanned_lsn = log.scanned_lsn;
  if (check_scanned_lsn % OS_FILE_LOG_BLOCK_SIZE == 0) {
    check_scanned_lsn += LOG_BLOCK_HDR_SIZE;
  }

  if (check_scanned_lsn < checkpoint_lsn ||
      check_scanned_lsn < recv_max_page_lsn) {
    ib::error(ER_IB_MSG_737, ulonglong{log.scanned_lsn},
              ulonglong{checkpoint_lsn}, ulonglong{recv_max_page_lsn});
  }

  if (recovered_lsn < checkpoint_lsn) {
    /* No harm in trying to do RO access. */
    if (!srv_read_only_mode) {
      ut_error;
    }

    return (DB_ERROR);
  }

  if ((recv_sys->found_corrupt_log && srv_force_recovery == 0) ||
      recv_sys->found_corrupt_fs) {
    return (DB_ERROR);
  }

  /* Read the last recovered log block. */
  lsn_t start_lsn;
  lsn_t end_lsn;

  start_lsn = ut_uint64_align_down(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);

  end_lsn = ut_uint64_align_up(recovered_lsn, OS_FILE_LOG_BLOCK_SIZE);

  if (start_lsn < end_lsn) {
    ut_a(start_lsn % log.buf_size + OS_FILE_LOG_BLOCK_SIZE <= log.buf_size);

    recv_read_log_seg(log, recv_sys->last_block, start_lsn, end_lsn);

    memcpy(log.buf + start_lsn % log.buf_size, recv_sys->last_block,
           OS_FILE_LOG_BLOCK_SIZE);
  }

  log_start(log, checkpoint_no + 1, checkpoint_lsn, recovered_lsn);

  /* Copy the checkpoint info to the log; remember that we have
  incremented checkpoint_no by one, and the info will not be written
  over the max checkpoint info, thus making the preservation of max
  checkpoint info on disk certain */

  if (!srv_read_only_mode) {
    log_files_write_checkpoint(log, checkpoint_lsn);
  }

  mutex_enter(&recv_sys->mutex);
  recv_sys->apply_log_recs = true;
  mutex_exit(&recv_sys->mutex);

  /* The database is now ready to start almost normal processing of user
  transactions: transaction rollbacks and the application of the log
  records in the hash table can be run in background. */

  return (DB_SUCCESS);
}

/** Complete the recovery from the latest checkpoint.
@param[in,out]	log		redo log
@param[in]	aborting	true if the server has to abort due to an error
@return recovered persistent metadata or nullptr if aborting*/
MetadataRecover *recv_recovery_from_checkpoint_finish(log_t &log,
                                                      bool aborting) {
  /* Make sure that the recv_writer thread is done. This is
  required because it grabs various mutexes and we want to
  ensure that when we enable sync_order_checks there is no
  mutex currently held by any thread. */
  mutex_enter(&recv_sys->writer_mutex);

  /* Free the resources of the recovery system */
  recv_recovery_on = false;

  /* By acquiring the mutex we ensure that the recv_writer thread
  won't trigger any more LRU batches. Now wait for currently
  in progress batches to finish. */
  buf_flush_wait_LRU_batch_end();

  mutex_exit(&recv_sys->writer_mutex);

  ulint count = 0;

  while (recv_writer_thread_active) {
    ++count;

    os_thread_sleep(100000);

    if (count >= 600) {
      ib::info(ER_IB_MSG_738);
      count = 0;
    }
  }

  MetadataRecover *metadata;

  if (!aborting) {
    metadata = recv_sys->metadata_recover;

    recv_sys->metadata_recover = nullptr;
  } else {
    metadata = nullptr;
  }

  recv_sys_free();

  if (!aborting) {
    /* Validate a few system page types that were left uninitialized
    by older versions of MySQL. */
    mtr_t mtr;

    mtr.start();

    buf_block_t *block;

    /* Bitmap page types will be reset in buf_dblwr_check_block()
    without redo logging. */

    block = buf_page_get(page_id_t(IBUF_SPACE_ID, FSP_IBUF_HEADER_PAGE_NO),
                         univ_page_size, RW_X_LATCH, &mtr);

    fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);

    /* Already MySQL 3.23.53 initialized FSP_IBUF_TREE_ROOT_PAGE_NO
    to FIL_PAGE_INDEX. No need to reset that one. */

    block = buf_page_get(page_id_t(TRX_SYS_SPACE, TRX_SYS_PAGE_NO),
                         univ_page_size, RW_X_LATCH, &mtr);

    fil_block_check_type(block, FIL_PAGE_TYPE_TRX_SYS, &mtr);

    block = buf_page_get(page_id_t(TRX_SYS_SPACE, FSP_FIRST_RSEG_PAGE_NO),
                         univ_page_size, RW_X_LATCH, &mtr);

    fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);

    block = buf_page_get(page_id_t(TRX_SYS_SPACE, FSP_DICT_HDR_PAGE_NO),
                         univ_page_size, RW_X_LATCH, &mtr);

    fil_block_check_type(block, FIL_PAGE_TYPE_SYS, &mtr);

    mtr.commit();
  }

  /* Free up the flush_rbt. */
  buf_flush_free_flush_rbt();

  return (metadata);
}

#endif /* !UNIV_HOTBACKUP */

/** Find a doublewrite copy of a page.
@param[in]	space_id	tablespace identifier
@param[in]	page_no		page number
@return	page frame
@retval nullptr if no page was found */
const byte *recv_dblwr_t::find_page(space_id_t space_id, page_no_t page_no) {
  typedef std::vector<const byte *, ut_allocator<const byte *>> matches_t;

  matches_t matches;
  const byte *result = 0;

  for (auto i = pages.begin(); i != pages.end(); ++i) {
    if (page_get_space_id(*i) == space_id && page_get_page_no(*i) == page_no) {
      matches.push_back(*i);
    }
  }

  for (auto i = deferred.begin(); i != deferred.end(); ++i) {
    if (page_get_space_id(i->m_page) == space_id &&
        page_get_page_no(i->m_page) == page_no) {
      matches.push_back(i->m_page);
    }
  }

  if (matches.size() == 1) {
    result = matches[0];
  } else if (matches.size() > 1) {
    lsn_t max_lsn = 0;
    lsn_t page_lsn = 0;

    for (matches_t::iterator i = matches.begin(); i != matches.end(); ++i) {
      page_lsn = mach_read_from_8(*i + FIL_PAGE_LSN);

      if (page_lsn > max_lsn) {
        max_lsn = page_lsn;
        result = *i;
      }
    }
  }

  return (result);
}

#if defined(UNIV_DEBUG) || defined(UNIV_HOTBACKUP)
/** Return string name of the redo log record type.
@param[in]	type	record log record enum
@return string name of record log record */
const char *get_mlog_string(mlog_id_t type) {
  switch (type) {
    case MLOG_SINGLE_REC_FLAG:
      return ("MLOG_SINGLE_REC_FLAG");

    case MLOG_1BYTE:
      return ("MLOG_1BYTE");

    case MLOG_2BYTES:
      return ("MLOG_2BYTES");

    case MLOG_4BYTES:
      return ("MLOG_4BYTES");

    case MLOG_8BYTES:
      return ("MLOG_8BYTES");

    case MLOG_REC_INSERT:
      return ("MLOG_REC_INSERT");

    case MLOG_REC_CLUST_DELETE_MARK:
      return ("MLOG_REC_CLUST_DELETE_MARK");

    case MLOG_REC_SEC_DELETE_MARK:
      return ("MLOG_REC_SEC_DELETE_MARK");

    case MLOG_REC_UPDATE_IN_PLACE:
      return ("MLOG_REC_UPDATE_IN_PLACE");

    case MLOG_REC_DELETE:
      return ("MLOG_REC_DELETE");

    case MLOG_LIST_END_DELETE:
      return ("MLOG_LIST_END_DELETE");

    case MLOG_LIST_START_DELETE:
      return ("MLOG_LIST_START_DELETE");

    case MLOG_LIST_END_COPY_CREATED:
      return ("MLOG_LIST_END_COPY_CREATED");

    case MLOG_PAGE_REORGANIZE:
      return ("MLOG_PAGE_REORGANIZE");

    case MLOG_PAGE_CREATE:
      return ("MLOG_PAGE_CREATE");

    case MLOG_UNDO_INSERT:
      return ("MLOG_UNDO_INSERT");

    case MLOG_UNDO_ERASE_END:
      return ("MLOG_UNDO_ERASE_END");

    case MLOG_UNDO_INIT:
      return ("MLOG_UNDO_INIT");

    case MLOG_UNDO_HDR_REUSE:
      return ("MLOG_UNDO_HDR_REUSE");

    case MLOG_UNDO_HDR_CREATE:
      return ("MLOG_UNDO_HDR_CREATE");

    case MLOG_REC_MIN_MARK:
      return ("MLOG_REC_MIN_MARK");

    case MLOG_IBUF_BITMAP_INIT:
      return ("MLOG_IBUF_BITMAP_INIT");

#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      return ("MLOG_LSN");
#endif /* UNIV_LOG_LSN_DEBUG */

    case MLOG_INIT_FILE_PAGE:
      return ("MLOG_INIT_FILE_PAGE");

    case MLOG_WRITE_STRING:
      return ("MLOG_WRITE_STRING");

    case MLOG_MULTI_REC_END:
      return ("MLOG_MULTI_REC_END");

    case MLOG_DUMMY_RECORD:
      return ("MLOG_DUMMY_RECORD");

    case MLOG_FILE_DELETE:
      return ("MLOG_FILE_DELETE");

    case MLOG_COMP_REC_MIN_MARK:
      return ("MLOG_COMP_REC_MIN_MARK");

    case MLOG_COMP_PAGE_CREATE:
      return ("MLOG_COMP_PAGE_CREATE");

    case MLOG_COMP_REC_INSERT:
      return ("MLOG_COMP_REC_INSERT");

    case MLOG_COMP_REC_CLUST_DELETE_MARK:
      return ("MLOG_COMP_REC_CLUST_DELETE_MARK");

    case MLOG_COMP_REC_SEC_DELETE_MARK:
      return ("MLOG_COMP_REC_SEC_DELETE_MARK");

    case MLOG_COMP_REC_UPDATE_IN_PLACE:
      return ("MLOG_COMP_REC_UPDATE_IN_PLACE");

    case MLOG_COMP_REC_DELETE:
      return ("MLOG_COMP_REC_DELETE");

    case MLOG_COMP_LIST_END_DELETE:
      return ("MLOG_COMP_LIST_END_DELETE");

    case MLOG_COMP_LIST_START_DELETE:
      return ("MLOG_COMP_LIST_START_DELETE");

    case MLOG_COMP_LIST_END_COPY_CREATED:
      return ("MLOG_COMP_LIST_END_COPY_CREATED");

    case MLOG_COMP_PAGE_REORGANIZE:
      return ("MLOG_COMP_PAGE_REORGANIZE");

    case MLOG_FILE_CREATE:
      return ("MLOG_FILE_CREATE");

    case MLOG_ZIP_WRITE_NODE_PTR:
      return ("MLOG_ZIP_WRITE_NODE_PTR");

    case MLOG_ZIP_WRITE_BLOB_PTR:
      return ("MLOG_ZIP_WRITE_BLOB_PTR");

    case MLOG_ZIP_WRITE_HEADER:
      return ("MLOG_ZIP_WRITE_HEADER");

    case MLOG_ZIP_PAGE_COMPRESS:
      return ("MLOG_ZIP_PAGE_COMPRESS");

    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
      return ("MLOG_ZIP_PAGE_COMPRESS_NO_DATA");

    case MLOG_ZIP_PAGE_REORGANIZE:
      return ("MLOG_ZIP_PAGE_REORGANIZE");

    case MLOG_FILE_RENAME:
      return ("MLOG_FILE_RENAME");

    case MLOG_PAGE_CREATE_RTREE:
      return ("MLOG_PAGE_CREATE_RTREE");

    case MLOG_COMP_PAGE_CREATE_RTREE:
      return ("MLOG_COMP_PAGE_CREATE_RTREE");

    case MLOG_INIT_FILE_PAGE2:
      return ("MLOG_INIT_FILE_PAGE2");

    case MLOG_INDEX_LOAD:
      return ("MLOG_INDEX_LOAD");

      /* Disabled for WL6378
      case MLOG_TRUNCATE:
              return("MLOG_TRUNCATE");
      */

    case MLOG_TABLE_DYNAMIC_META:
      return ("MLOG_TABLE_DYNAMIC_META");

    case MLOG_PAGE_CREATE_SDI:
      return ("MLOG_PAGE_CREATE_SDI");

    case MLOG_COMP_PAGE_CREATE_SDI:
      return ("MLOG_COMP_PAGE_CREATE_SDI");

    case MLOG_TEST:
      return ("MLOG_TEST");
  }

  DBUG_ASSERT(0);

  return (nullptr);
}
#endif /* UNIV_DEBUG || UNIV_HOTBACKUP */

#if defined (UNIV_PMEMOBJ_PART_PL)
/* Functions from PL-NVM for recovery*/
//////////////////////////////////////////////////////
/*
 * Alternative to recv_sys_init
 * */
void 
pm_ppl_recv_init(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl
		) 
{

	uint32_t n, i;
    uint32_t size;
	ulint avail_mem;
	ulint hash_size;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_RECV_LINE* recv_line;

	n = ppl->n_buckets;
	
	//new (&ppl->recv_space_ids) PMEM_PAGE_PART_LOG::Recv_Space_IDs();
	
	if (IS_GLOBAL_HASHTABLE){
		//A: allocate global recv_line
		avail_mem = buf_pool_get_curr_size();

		ppl->recv_line = (PMEM_RECV_LINE*) malloc(sizeof(PMEM_RECV_LINE));
		recv_line = ppl->recv_line;
		
		/*MySQL 8.0 doesn't use heap on recv_sys*/
		//recv_line->heap = mem_heap_create_typed(256, MEM_HEAP_FOR_RECV_SYS);

		recv_line-> buf = NULL;
		recv_line->len = 0;
		recv_line->recovered_offset = 0;

		/*Hash table*/
		recv_line->hashed_id = n + 1;	
		//TODO: how to decide the size of the hashtable
		hash_size = avail_mem / 512;
		recv_line->alloc_hash_size = hash_size;
		//hash_size = avail_mem / 4;

		//recv_line->addr_hash = hash_create(hash_size);
		/*MySQL 8.0 uses Spaces instead of addr_hash*/
		using Spaces = recv_sys_t::Spaces;
		recv_line->spaces = UT_NEW(Spaces(), mem_log_recv_space_hash_key);

		printf("PMEM_INFO allocate %zu bytes for ppl->recv_line->addr_hash\n", hash_size);
		recv_line->n_addrs = 0;

		recv_line->n_read_done = 0;

		recv_line->apply_log_recs = FALSE;
		recv_line->apply_batch_on = FALSE;

		recv_line->found_corrupt_log = false;
		recv_line->found_corrupt_fs = false;
		recv_line->mlog_checkpoint_lsn = 0;

		new (&recv_line->recv_space_ids) PMEM_RECV_LINE::Recv_Space_IDs();
		recv_line->metadata_recover = UT_NEW_NOKEY(MetadataRecover());

		//recv_line->encryption_list = NULL;
	} else {
		//B: allocate for each line
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);

			/* the redo buffer has same length as log buffer */
			size = D_RW(pline->logbuf)->size;

			pline->recv_line = (PMEM_RECV_LINE*) malloc(sizeof(PMEM_RECV_LINE));
			recv_line = pline->recv_line;

			/*MySQL 8.0 doesn't use heap on recv_sys*/
			//recv_line->heap = mem_heap_create_typed(256, MEM_HEAP_FOR_RECV_SYS);

			/*the buffer*/
			recv_line->buf = static_cast<byte*>(
					ut_malloc_nokey(size));
			recv_line->len = size;
			recv_line->recovered_offset = 0;

			/*Hash table*/
			//recv_line->addr_hash = hash_create(avail_mem / 512);
			recv_line->hashed_id = i;	
			//number of cell in a hashtable
			//TODO: how to decide the size of the hashtable
			recv_line->alloc_hash_size = 2048;

			//recv_line->addr_hash = hash_create(2048);
			/*MySQL 8.0 uses Spaces instead of addr_hash*/
			using Spaces = recv_sys_t::Spaces;
			recv_line->spaces = UT_NEW(Spaces(), mem_log_recv_space_hash_key);

			recv_line->n_addrs = 0;
			recv_line->n_read_reqs = 0;
			recv_line->n_read_done = 0;
			recv_line->n_cache_done = 0;
			recv_line->n_skip_done = 0;

			recv_line->apply_log_recs = FALSE;
			recv_line->apply_batch_on = FALSE;

			recv_line->found_corrupt_log = false;
			recv_line->found_corrupt_fs = false;
			recv_line->mlog_checkpoint_lsn = 0;

			new (&recv_line->recv_space_ids) PMEM_RECV_LINE::Recv_Space_IDs();
			//recv_line->encryption_list = NULL;
			recv_line->metadata_recover = UT_NEW_NOKEY(MetadataRecover());
		} //end for
	}
}

/*
 * Free resource created during recovery process
 * */
void 
pm_ppl_recv_end(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl) {

	uint32_t n, i;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;
	PMEM_RECV_LINE* recv_line;

	n = ppl->n_buckets;

	//call_destructor(&ppl->recv_space_ids);

	if (IS_GLOBAL_HASHTABLE) {
		//A: free global recv_line
		recv_line = ppl->recv_line;
		recv_line-> buf = NULL;
		recv_line->len = 0;
		recv_line->recovered_offset = 0;
		recv_line->apply_log_recs = FALSE;
		recv_line->apply_batch_on = FALSE;

		//if (recv_line->addr_hash != NULL) {
		//	hash_table_free(recv_line->addr_hash);
		//}
		for (auto &space : *recv_line->spaces) {
			if (space.second.m_heap != nullptr) {
				mem_heap_free(space.second.m_heap);
				space.second.m_heap = nullptr;
			}
		}

		//if (recv_line->heap != NULL) {
		//	mem_heap_free(recv_line->heap);
		//}

		call_destructor(&recv_line->recv_space_ids);
		UT_DELETE(recv_line->metadata_recover);
		recv_line->metadata_recover = nullptr;

		//ut_free(recv_line->buf);
		free(recv_line);
		recv_line = NULL;
	} else {
		//B: free other recv_line
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;

			if (recv_line != NULL){
				//if (recv_line->addr_hash != NULL) {
				//	hash_table_free(recv_line->addr_hash);
				//}
				for (auto &space : *recv_line->spaces) {
					if (space.second.m_heap != nullptr) {
						mem_heap_free(space.second.m_heap);
						space.second.m_heap = nullptr;
					}
				}

				//if (recv_line->heap != NULL) {
				//	mem_heap_free(recv_line->heap);
				//}

				call_destructor(&recv_line->recv_space_ids);
				UT_DELETE(recv_line->metadata_recover);
				recv_line->metadata_recover = nullptr;

				ut_free(recv_line->buf);
				free(recv_line);
				recv_line = NULL;
			}
		}
	}

	//C: reset data structures in PPL
	pm_ppl_reset_all(pop, ppl);
}

/*
 * Util to open the spaces between REDO phase1 and REDO phase2
 * This function is only called by the single master recovery thread
 * */
void
pm_ppl_open_spaces(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl
		)
{
	uint32_t n, i;
	PMEM_PAGE_LOG_HASHED_LINE*	pline;
	PMEM_RECV_LINE*				recv_line;

	n = ppl->n_buckets;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;

		for (auto space_id : recv_line->recv_space_ids){
			fil_space_t* space_tem = fil_space_get(space_id);

			if (space_tem == nullptr){
				bool is_open_success = fil_tablespace_open_for_recovery(space_id);
				printf("===> DEBUG: fil_tablespace_open_for_recovery() space %u return %d\n", space_id, is_open_success);

				if (!is_open_success){
					ut_ad(!fil_tablespace_lookup_for_recovery(space_id) ||
							fsp_is_undo_tablespace(space_id));
				}
			} else {
			}
		} //end inner for
	} //end outter for
}

/*
 * Simulate recv_recovery_from_checkpoint_start()
 * */
dberr_t
pm_ppl_recovery(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		log_t &log,
        lsn_t flush_lsn
        ) 
{

	uint32_t i;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	lsn_t		max_recovered_lsn;
	lsn_t		global_max_lsn;
	dberr_t		err;
    
	ulint n = ppl->n_buckets;

	/*Init PL-NVM data structures for recovery*/
    pm_ppl_recv_init(pop, ppl);

	/* Initialize red-black tree for fast insertions into the
	flush_list during recovery process. */
	buf_flush_init_flush_rbt();

	if (srv_force_recovery >= SRV_FORCE_NO_LOG_REDO) {
		ib::info(ER_IB_MSG_728);

		/* We leave redo log not started and this is read-only mode. */
		ut_a(log.sn == 0);
		ut_a(srv_read_only_mode);

		return (DB_SUCCESS);
	}

	recv_recovery_on = true;
	//log_mutex_enter(); //MySQL 8.0 doesn't have this func
    
	/*Phase 1: Analysis and prepare data structure for recovery
     *pm_ppl_analysis() replace old codes until recv_recovery_begin()
     * */
    pm_ppl_analysis(pop, ppl, &global_max_lsn);
    printf("\nPMEM_REC: ====    ANALYSIS FINISH ====== \n");

	/*Phase 2: REDO
    Simulate recv_recovery_begin() that call recv_read_log_seg()
   	and recv_scan_log_recs() to read and parse REDO logs from 
	WAL files to recovery's buffer 
	*/
    pm_ppl_redo(pop, ppl);
		
	max_recovered_lsn = pm_ppl_recv_get_max_recovered_lsn(
		pop, ppl);	
	
	/*if the PL-NVM has no REDO logs, then max_recovered_lsn is 0, that cause some assertions*/	
	if (max_recovered_lsn < LOG_START_LSN){
		max_recovered_lsn = LOG_START_LSN + LOG_BLOCK_HDR_SIZE;
	}

    /* all code related with checkpoint is removed because we don't use checkpoint
    
    at the end of pm_ppl_redo, recv_sys->recovered_lsn is the last offset of the last line, update the log_sys lsn to avoid runtime error of assertions */
	
	log.lsn = max_recovered_lsn;
	log.sn.store(log_translate_lsn_to_sn(log.lsn));

	recv_needed_recovery = true;

	/*New in MySQL 8.0, Simulate log_start() assigns corresponding lsn values with checkpoint_lsn and recovered_lsn*/
	pm_ppl_log_start(log, global_max_lsn, max_recovered_lsn);

    /*
	 * In MySQL 5.7: recv_init_crash_recovery_spaces() call buf_dblwr_process() that correct tone page using DWB 
	 * In MySQL 8.0, that work is done in recv_init_crash_recovery() plus create recv_writer thread. 
	 * Since PL-NVM implements its own log writer we just call buf_dblwr_process() here
	 * */

	//err = pm_ppl_recv_init_crash_recovery_spaces(pop, ppl, max_recovered_lsn);
	buf_dblwr_process();

    //if (err != DB_SUCCESS) {
    //    //log_mutex_exit(); //not used in MySQL 8.0
    //    return(err);
    //}
	
	/*Open spaces in needed*/	
	pm_ppl_open_spaces(pop, ppl);

    //we don't need to rescan because we put log recs in HASH table in the first scan


	/* Synchronize the uncorrupted log groups to the most up-to-date log
	group; we also copy checkpoint info to groups */
	
	/*MySQL 8.0 removes this function
	 * simulate fil_names_clear(log_sys->last_checkpoint_lsn, true)*/
	//pm_ppl_fil_names_clear(max_recovered_lsn);

	/*IMPORTANT 
	 * Set apply_log_recs for each recv_line to TRUE, so that UNDO pages and other data pages can be applied (actual REDOing) when read on.
*/	
	
	n = ppl->n_buckets;
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		pline->recv_line->apply_log_recs = TRUE;
	}

	if (IS_GLOBAL_HASHTABLE){
		ppl->recv_line->apply_log_recs = TRUE;	

		/*this function only used in global hashtable approach*/	
		pm_ppl_recv_apply_prior_pages(pop, ppl, NULL, false);
	}

	
	//srv_start_lsn = max_recovered_lsn;

	mutex_enter(&recv_sys->mutex);

	recv_sys->apply_log_recs = TRUE;

	mutex_exit(&recv_sys->mutex);

	//log_mutex_exit(); //not used in MySQL 8.0 

	recv_lsn_checks_on = true;
    //pm_ppl_recv_end(pop, ppl);
	
	//printf("PMEM_RECV: RESET PPL after REDO phase 1\n");	
	//pm_page_part_log_bucket_reset(pop, ppl);

	return(DB_SUCCESS);
}

/*
 * (1) Compute the low-water mark recv_diskaddr and corresponding LSN for each lines
 *
 * (2) Build the hashtable on each pline for fast look up
 *
 * We still need this step even though  pline->oldest_block_off could tell the same info.
 *
 * pline->oldest_block_off could be out-of-date if the system crashes after the plogblock is reset but before the pm_ppl_update_oldest()
 * */
void 
pm_ppl_analysis(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		ulint*		global_max_lsn)
{

	uint32_t n, i, j;
	int32_t min_id1, min_id2;

    uint64_t low_diskaddr;
    uint64_t low_offset;
    uint64_t low_watermark;
	uint64_t min_lsn;

	uint64_t max_lsn;
	
	int64_t delta;
	uint64_t min_delta;
	uint64_t max_delta;
	uint64_t total_delta;
	uint64_t min_write_off;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE* pline;

	TOID(PMEM_PAGE_LOG_BLOCK) log_block;
	PMEM_PAGE_LOG_BLOCK*	plog_block;

	n = ppl->n_buckets;
	//k = ppl->n_blocks_per_bucket;
	
	min_delta = ULONG_MAX;
	max_delta = 0;
	total_delta = 0;
	*global_max_lsn = 0;
	
	//pm_page_part_log_hash_free(pop, ppl);
	//pm_page_part_log_hash_create(pop, ppl);

    //find low_watermark for each line
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

        low_watermark = ULONG_MAX;
        low_diskaddr = ULONG_MAX;
        low_offset = 0;
		min_lsn = ULONG_MAX;
		min_id1 = min_id2 = -1;

		max_lsn = 0;

        //for each block in the line
        for (j = 0; j < pline->max_blocks; j++){
            plog_block = D_RW(D_RW(pline->arr)[j]);
			
            if (!plog_block->is_free){

				if (plog_block->lastLSN == 0){
				/*
				 * There is a case that in pm_ppl_write_rec(), the crash occur after pm_ppl_hash_add() but before the other info updated in the plogblock
				 * */
					assert(plog_block->firstLSN == 0);
					assert(plog_block->first_rec_size == 0);
					assert(plog_block->first_rec_type == 0);
					D_RW(D_RW(pline->arr)[j])->is_free = true;
					D_RW(D_RW(pline->arr)[j])->state = PMEM_FREE_BLOCK;

					//plog_block->is_free = true;
					//plog_block->state = PMEM_FREE_BLOCK;
					continue;
				}
				/*the minimum write offset of each pline*/
				min_write_off = plog_block->start_diskaddr + plog_block->start_off;

				/*add block->key into the per-line hashtable */
				pline->key_map->insert(std::make_pair(plog_block->key, plog_block));

				/*add min_write_off into the per-line sorted map*/
				pline->offset_map->insert(std::make_pair(min_write_off, plog_block));

				//std::map<uint64_t, uint32_t>::iterator it;
				auto it = pline->offset_map->find(min_write_off);
				if (it != pline->offset_map->end()){
					assert(it->first == min_write_off);
				}	

				if(low_watermark > min_write_off) 
				{
				
                    low_watermark = min_write_off;
                    low_diskaddr = plog_block->start_diskaddr;
                    low_offset = plog_block->start_off;

					//assert(min_lsn > plog_block->firstLSN);
					min_id1 = j;
                }

				if (min_lsn > plog_block->firstLSN){
					min_lsn = plog_block->firstLSN;
					min_id2 = j;
				}

				if (max_lsn < plog_block->lastLSN) {
					max_lsn = plog_block->lastLSN;
				}
            }
        } //end for each block in the line

		//whether min water_mark and min lsn is on the same block?
		if (min_id1 != min_id2){

			PMEM_PAGE_LOG_BLOCK*	plog_block1 = 
				D_RW(D_RW(pline->arr)[min_id1]);

			PMEM_PAGE_LOG_BLOCK*	plog_block2 = 
				D_RW(D_RW(pline->arr)[min_id2]);
			printf("PMEM_WARN: ANALYSIS min lsn is not in same block with low watermark. plog_block1 id %u plog_block2 id %u \n", plog_block1->id, plog_block2->id);
			assert(min_id1 == min_id2);
		}

        pline->recv_diskaddr = low_diskaddr;
        pline->recv_off = low_offset;
		pline->recv_lsn = min_lsn;

		if (*global_max_lsn < max_lsn){
			*global_max_lsn = max_lsn;
		}

		PMEM_PAGE_LOG_BUF* plogbuf = D_RW(pline->logbuf);

		delta = (pline->diskaddr + plogbuf->cur_off) - (pline->recv_diskaddr + pline->recv_off);
		assert(delta >= 0);

		if (min_delta > (uint64_t) delta)
			min_delta = delta;
		if (max_delta < (uint64_t) delta)
			max_delta = delta;

		total_delta += delta;
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		printf ("ANALYSIS: pline %zu need to REDO %zu bytes\n", pline->hashed_id, delta);
#endif
    } //end for each line

	//dict_check_tablespaces_and_store_max_id(true);	
	//pm_ppl_load_spaces(pop, ppl);

	printf ("ANALYSIS: min_delta %zu bytes max_delta %zu bytes total MB need to parse %f\n", min_delta, max_delta, (total_delta * 1.0 / (1024*1024)));
}

/*
 *Start redoer threads to parallelism REDOing
 * */
void 
pm_ppl_redo(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl) {

	uint32_t n, i;

	/*disable is_multi_redo for debugging*/

	bool is_multi_redo = false;
	//bool is_multi_redo = true;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
    PMEM_LOG_REDOER*				redoer;

	n = ppl->n_buckets;

	/* (0) REDO line by line, only used for debugging */
	if (!is_multi_redo){	
		for (i = 0; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);

			/*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
			if (pline->recv_diskaddr == ULONG_MAX)
				continue;

			//test, serialize for easy debug
			bool is_err = pm_ppl_redo_line(pop, ppl, pline);
			assert(!is_err);

			printf("PMEM_RECV: done redoing PHASE 1 line %d\n", pline->hashed_id);
		}
		return;
	}

	/* Parallelism REDO */
    
	/*(1) Init the redoers */
    redoer = pm_log_redoer_init(n);
    ppl->redoer = redoer;
	
	redoer->phase = PMEM_REDO_PHASE1;

    redoer->n_remains = n;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);

		recv_line = pline->recv_line;
		
		recv_line->mlog_checkpoint_lsn = 0;

        if (pline->recv_diskaddr == ULONG_MAX){
            redoer->n_remains--;
        }
    }
    printf("PMEM_RECV: Need scan and parse %zu lines\n", redoer->n_remains);

    ppl->is_redoing_done = false;
    
    /*(2) Create the redoer threads */
	printf("PMEM_REDO1: create %zu redoer threads\n", srv_ppl_n_redoer_threads);

    for (i = 0; i < srv_ppl_n_redoer_threads; ++i) {
        //os_thread_create(pm_log_redoer_worker, NULL, NULL); //MySQL 5.7
        os_thread_create(pm_log_redoer_thread_key, pm_log_redoer_worker);
    }
    
    /* (3) Assign pointers in the array pointer to plines*/
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
        
        //this variable is set true after a thread accept to REDO this line
        pline->is_redoing = false;

        /*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
        if (pline->recv_diskaddr == ULONG_MAX)
            continue;

        //Asign pline to a redoer thread
        redoer->hashed_line_arr[i] = pline;
    }

    /*trigger REDOer workers */
    os_event_set(redoer->is_log_req_not_empty);

	/*Now redoers will wakeup and call pm_ppl_redo_line() */
    
    /*(4) wait for all threads finish */
    while (redoer->n_remains > 0){
        os_event_wait(redoer->is_log_all_finished);
    }

//finish: 
    //(4) finish
    pm_log_redoer_close(ppl->redoer);
    printf("\nPMEM_RECV: ===== REDO1 completed ======\n");
}

/*
* REDO1 (PARSE PHASE)
* Call back funtion from pm_log_redoer_worker()
* Simulate recv_group_scan_log_recs()
* Read all on-disk log rec from start_lsn to the buffer
* PARSE each log record, insert it in the hashtable if it is need.
* @return: true: error;  false: OK
*/
bool
pm_ppl_redo_line(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		PMEM_PAGE_LOG_HASHED_LINE* pline) {
    
    //for phase 1
    uint64_t start_addr, cur_addr;

    byte* recv_buf;
    uint64_t recv_buf_len;  //len of the buffer
    uint64_t read_off;  //len of the buffer
    
    //temp variable for memcpy
    byte* src;
    uint64_t scan_len;
    
    //for phase 2
    byte* ptr;
	
	/*read from header*/
    uint32_t actual_len;
    uint32_t n_recs;
	uint32_t w_hashed_id; // hash_id assigned during writing log, located in log block's header

	int64_t parsed_recs;
	uint64_t need_recs;
	uint64_t skip1_recs;
	uint64_t skip2_recs;

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	uint64_t p1, p2, p3, n1, n2, n3;
	uint64_t s1_1, s1_2, s1_3, s2_1, s2_2, s2_3;
	int64_t total_parsed_recs;
	uint64_t total_need_recs;
	uint64_t total_skip1_recs;
	uint64_t total_skip2_recs;
#endif

    dberr_t err;
	
	PMEM_RECV_LINE* recv_line = pline->recv_line;
    PMEM_PAGE_LOG_BUF* plogbuf;
    PMEM_PAGE_LOG_BUF* pcur_logbuf;

    TOID(PMEM_PAGE_LOG_BUF) next_logbuf;
    PMEM_PAGE_LOG_BUF* pnext_logbuf;

    start_addr = pline->recv_diskaddr; //this value computed in ANALYSIS step
    cur_addr = start_addr;

	//we start read from here
    read_off = start_addr;
	
	assert(recv_line != NULL);
	recv_line->recovered_addr = start_addr;	
	recv_line->recovered_offset = 0;	
	recv_line->n_addrs = 0;
	
	//why the old impl call this function?	
	//simulate recv_sys_empty_hash()
	//pm_ppl_recv_line_empty_hash(pop, ppl, pline);

	recv_line->parse_start_lsn = start_addr;
	recv_line->scanned_lsn = pline->recv_lsn;
	recv_line->recovered_lsn = pline->recv_lsn; // this value computed in ANALYSIS step
	recv_line->scanned_checkpoint_no = 0;

    recv_buf = recv_line->buf;
    recv_buf_len = recv_line->len;

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	total_parsed_recs = 0;
	total_need_recs = 0;
	total_skip1_recs = 0;
	total_skip2_recs = 0;
#endif    

    /* The orders of offsets used in REDO1 are:
	 * pline->recv_diskaddr --> pline->write_diskaddr --> flush_logbuf->cur_off --> logbuf->cur_off 
	 * */

read_log_file:
    memset(recv_buf, 0, pline->recv_line->len);

    ///////////////////////////////////////////////////
    // (1) Scan phase: Build the recv buffer
    // ////////////////////////////////////////////////
	
    if (cur_addr < pline->write_diskaddr){
        /*case A: pread n-bytes equal to logbuf size from log file on disk to recv_buf */
        scan_len = pline->write_diskaddr - cur_addr;

        if (scan_len > recv_buf_len)
            scan_len = recv_buf_len;
        else {
            /* the last read */
            //printf("==> PMEM_INFO the last read in case A scan_len %zu \n", scan_len);
        }
        
        err = pm_log_fil_read(pop, ppl, pline, recv_buf, read_off, scan_len);

        if (err != DB_SUCCESS){
            printf("PMEM_ERROR error in pm_log_fil_read() line %d", pline->hashed_id);
            assert(0);

			return true;
        }

        /* read the actual buffer len (included the header) */
        actual_len = mach_read_from_4(recv_buf);
		n_recs = mach_read_from_4(recv_buf + 4);
		w_hashed_id = mach_read_from_4(recv_buf + 8);


		if (actual_len == 0){
			/*I/O error*/
			assert(actual_len > 0);
			assert(n_recs > 0);
		}

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;

        /* parse log recs in the recv_buf and insert log recs into the per-line hashtable */
        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case A error after pm_ppl_parse_recs() \n");
            assert(0);
        }
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;
#endif

        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;
        read_off += scan_len;

        goto read_log_file; 
    }
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	p1 = total_parsed_recs;
	n1 = total_need_recs;
	s1_1 = total_skip1_recs;
	s2_1 = total_skip2_recs;
#endif

    /* If you reach this point, all log recs on disk before write_diskaddr are parsed successful. Now parse log recs from write_diskaddr */

    scan_len = 0;
    pcur_logbuf = D_RW(pline->tail_logbuf);

read_log_nvm:
    if (cur_addr < pline->diskaddr){
        /*case B: the flushing from logbuf to log file has not finished when the system crash
		 * memcpy from flushing log buffer in NVDIMM to buf
		 */
        src = ppl->p_align + pcur_logbuf->pmemaddr;
        scan_len = pcur_logbuf->size;

        memcpy(recv_buf, src, scan_len);

        actual_len = mach_read_from_4(recv_buf);
		n_recs = mach_read_from_4(recv_buf + 4);
		w_hashed_id = mach_read_from_4(recv_buf + 8);

        assert(actual_len > 0);
        assert(actual_len == pcur_logbuf->cur_off);
		assert(n_recs > 0);
		assert(n_recs == pcur_logbuf->n_recs);

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;

        /* parse log recs in the recv_buf and insert log recs into the per-line hashtable */
        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case B error after pm_ppl_parse_recs() \n");
            assert(0);

            return true; 
        }

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;
#endif
        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;

		pnext_logbuf = D_RW(pcur_logbuf->next);

		if ( (pnext_logbuf->diskaddr - pcur_logbuf->diskaddr) > pcur_logbuf->size) {
			printf("!!! WARN: there is a hole log in pline %u next diskaddr %zu - cur_diskaddr %zu = %zu > logbuf_size %zu, re-read from log file\n",
					recv_line->hashed_id,
					pnext_logbuf->diskaddr,
					pcur_logbuf->diskaddr,
					pnext_logbuf->diskaddr - pcur_logbuf->diskaddr,
					pcur_logbuf->size);
			/*there is hole(s) between the tail and the head
			 * we need to re-read from disk*/
			TOID_ASSIGN(pline->tail_logbuf, (pcur_logbuf->next).oid);
			pline->write_diskaddr = pnext_logbuf->diskaddr;
			goto read_log_file;
		} else {
			pcur_logbuf = pnext_logbuf;
			goto read_log_nvm;	
		}
    }
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	p2 = (total_parsed_recs - p1);
	n2 = (total_need_recs - n1);
	s1_2 = (total_skip1_recs - s1_1);
	s2_2 = (total_skip2_recs - s2_1);
#endif

	/*If you reach here, all on-disk log recs and in-flushing log recs are parsed
	 * now parse the last part of log recs in the logbuf*/

    plogbuf = D_RW(pline->logbuf);
	assert(cur_addr == plogbuf->diskaddr);

    if (plogbuf->cur_off > 0){
        src = ppl->p_align + plogbuf->pmemaddr;
        scan_len = plogbuf->cur_off;

        memcpy(recv_buf, src, scan_len);
        actual_len = scan_len;
		n_recs = plogbuf->n_recs;

        ptr = recv_buf + PMEM_LOG_BUF_HEADER_SIZE;

        /* parse log recs in the recv_buf and insert log recs into the per-line hashtable */
        parsed_recs = pm_ppl_parse_recs(pop, ppl, pline, ptr, actual_len - PMEM_LOG_BUF_HEADER_SIZE, &skip1_recs, &skip2_recs, &need_recs);

		assert(parsed_recs == n_recs);

        if (parsed_recs < 0){
            printf("PMEM_ERROR case C error after pm_ppl_parse_recs() \n");
            assert(0);

            return true; 
        }

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		total_parsed_recs += parsed_recs;
		total_need_recs += need_recs;
		total_skip1_recs += skip1_recs;
		total_skip2_recs += skip2_recs;
        cur_addr += scan_len;
		recv_line->recovered_addr = cur_addr;
#endif

    } //end if (plogbuf->cur_off > 0)

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	p3 = (total_parsed_recs - p1 - p2);
	n3 = (total_need_recs - n1 - n2);
	s1_3 = total_skip1_recs - s1_1 - s1_2;
	s2_3 = total_skip2_recs - s2_1 - s2_2;
	printf("PMEM_INFO: FINISH parse line %zu need/parsed %zu/%zu n1/s1_1/s2_1/p1 %zu/%zu/%zu/%zu n2/s1_2/s2_2/p2 %zu/%zu/%zu/%zu n3/s1_3/s2_3/p3 %zu/%zu/%zu/%zu \n",
		   	pline->hashed_id,
		   	total_need_recs, total_parsed_recs,
			n1, s1_1, s2_1, p1,
			n2, s1_2, s2_2, p2,
			n3, s1_3, s2_3, p3);
#endif
    return false; 
}

/*
 * Alternative to recv_parse_log_recs()
 * Parse log recs on a line from start_ptr to start_ptr + parse_len
 *
 *Because we don't use checkpoint in PPL
 We do not distinguish single_rec and multi_rec
 @param[in] pop
 @param[in] ppl
 @param[in] pline
 @param[in] start_ptr pointer to the start of logbuf
 @param[in] parse_len number bytes should be parsed
 @param[out] n_need_recs number of log recs need to add to hashtable after parsing
 @return: number of parsed recs
 * */
int64_t
pm_ppl_parse_recs(
		PMEMobjpool*		        pop,
		PMEM_PAGE_PART_LOG*	        ppl,
		PMEM_PAGE_LOG_HASHED_LINE*  pline,
		byte*                       start_ptr,
		uint64_t                    parse_len,
		uint64_t*					n_skip1_recs,
		uint64_t*					n_skip2_recs,
		uint64_t*					n_need_recs
		)
{
    byte* ptr;
    byte* end_ptr;
	mlog_id_t	type;
	//ulint		space;
	uint32_t		space;
	//ulint		page_no;
	uint32_t		page_no;
	byte*		body;

    uint64_t len;
    uint64_t cur_off;
	uint64_t rec_lsn;

	uint64_t n_parsed_recs;

	bool	is_need;

	PMEM_RECV_LINE* recv_line = pline->recv_line;
    end_ptr = start_ptr + parse_len;
	PMEM_PARSE_RESULT parse_res;

    cur_off = 0;
	recv_line->recovered_offset = cur_off;

	n_parsed_recs = 0;
	*n_need_recs = 0;
	*n_skip1_recs = *n_skip2_recs = 0;

//parse each log recs, increase the recovered_off after each loop
loop:
    //ptr = start_ptr + recv_line->recovered_offset;
    ptr = start_ptr + cur_off;

    if (ptr == end_ptr) {
        return n_parsed_recs; //done, all log recs are scanned
    }
	is_need = false;

	
	//parse a log rec from ptr, return rec len
	len = pm_ppl_recv_parse_log_rec(
			pop, ppl, pline,
			&type, ptr, end_ptr, &space,
			&page_no, true, &rec_lsn, &body,
			&parse_res, &is_need);

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
	switch (parse_res){
		case PMEM_PARSE_BLOCK_NOT_EXISTED:
			*n_skip1_recs = *n_skip1_recs + 1;
			break;
		case PMEM_PARSE_LSN_OLD:
			*n_skip2_recs = *n_skip2_recs + 1;
			break;
		case PMEM_PARSE_NEED:
		default:
			assert(is_need);
			break;
	}
#endif
	
	recv_line->found_corrupt_log = recv_sys->found_corrupt_log;
	recv_line->found_corrupt_fs = recv_sys->found_corrupt_fs;
	
	if (len == 0) {
		//return(false);
		assert(0);
        return n_parsed_recs; // reach MLOG_MULTI_REC_END
	}

	if (recv_line->found_corrupt_log) {
		recv_report_corrupt_log(
				ptr, type, space, page_no);
		//return(true);
		return -1;
	}

	if (recv_line->found_corrupt_fs) {
		//return(true);
		return -1;
	}

	//update
	n_parsed_recs++;
	cur_off += len;
	recv_line->recovered_offset = cur_off;

	
	/* update recovered_lsn, this value is need in rebuilding tone page in DWB */
	recv_line->recovered_lsn = rec_lsn;

	/* add the parsed log record to per-line hashtable.
	 * This hashtable is used in apply phase */
	switch (type) {
		case MLOG_MULTI_REC_END:
		/* we simply skip and don't handle this as original */
			goto loop;
		case MLOG_DUMMY_RECORD:
		/* Do nothing */
		break;
		case MLOG_FILE_DELETE:
		case MLOG_FILE_CREATE:
		case MLOG_FILE_RENAME:
		case MLOG_TABLE_DYNAMIC_META:
		/* These were already handled by
		   recv_parse_log_rec() and
		   recv_parse_or_apply_log_rec_body(). */
		break;
		default:
		/*In MySLQ */
		if (is_need){
			/*new in MySQL 8.0 insert the space in the global space set*/
			//pmemobj_rwlock_wrlock(pop, &ppl->recv_lock);
			recv_line->recv_space_ids.insert(space);
			//pmemobj_rwlock_unlock(pop, &ppl->recv_lock);

			if (!IS_GLOBAL_HASHTABLE){
				pm_ppl_recv_add_to_hash_table(
						pop, ppl, recv_line,
						type, space, page_no,
						body, ptr + len,
						rec_lsn, rec_lsn + len);
				*n_need_recs = *n_need_recs + 1;
			} else {
				/*the global hashtable approach*/
				pmemobj_rwlock_wrlock(pop, &ppl->recv_lock);
				/*put the recv_addr to the global hashtable in ppl->recv_line.
				 * Contention between redoer workers
				 * */
				pm_ppl_recv_add_to_hash_table(
						pop, ppl, ppl->recv_line,
						type, space, page_no,
						body, ptr + len,
						rec_lsn, rec_lsn + len);

				*n_need_recs = *n_need_recs + 1;

				pmemobj_rwlock_unlock(pop, &ppl->recv_lock);
			}
		}
		/* fall through */
	}

	goto loop;
}
/*
 * Simulate recv_parse_log_rec()
 *Note that in PPL, the structure of log record header has 
 extra bytes for rec_len and rec_lsn (type, space, page_no, rec_len, rec_lsn, body)
@param[in]	pop		pop
@param[in]	ppl		partition page log
@param[in]	recv_line	recv_line
@param[out]	type		log record type
@param[in]	ptr		pointer to a log rec on recv_line->buf
@param[in]	end_ptr		end of the buffer in the recv_line
@param[out]	space_id	tablespace identifier
@param[out]	page_no		page number
@param[in]	apply		whether to apply MLOG_FILE_* records
@param [out] rec_lsn	lsn of the log record
@param[out]	body		start of log record body
@param[out] parse_res	parsed response (1: need, 2: plogblock is not existed, 3: 
@param[out]	is_need		true if we need to insert this log rec in hashtable.
 * */
uint64_t
pm_ppl_recv_parse_log_rec(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	mlog_id_t*	type,
	byte*		ptr,
	byte*		end_ptr,
	uint32_t*		space,
	uint32_t*		page_no,
	bool		apply,
	uint64_t*	rec_lsn,
	byte**		body,
	PMEM_PARSE_RESULT*	parse_res,
	bool*		is_need) {
	
	plog_hash_t* item;
	PMEM_PAGE_LOG_BLOCK* plog_block;
	PMEM_RECV_LINE* recv_line = pline->recv_line;

	uint32_t rec_len;

	byte*	new_ptr;
	
	*is_need = false;
	*body = NULL;
	
	//(1) Do some checks
	UNIV_MEM_INVALID(type, sizeof *type);
	UNIV_MEM_INVALID(space, sizeof *space);
	UNIV_MEM_INVALID(page_no, sizeof *page_no);
	UNIV_MEM_INVALID(body, sizeof *body);

	if (ptr == end_ptr) {
		assert(0);
		return(0);
	}
	
	//skip those log types
	switch (*ptr) {
	case MLOG_MULTI_REC_END:
	case MLOG_DUMMY_RECORD:
		*type = static_cast<mlog_id_t>(*ptr);
		return(1);
	case MLOG_MULTI_REC_END | MLOG_SINGLE_REC_FLAG:
	case MLOG_DUMMY_RECORD | MLOG_SINGLE_REC_FLAG:
		recv_line->found_corrupt_log = true;
		assert(0);
		return(0);
    case MLOG_TABLE_DYNAMIC_META:
    case MLOG_TABLE_DYNAMIC_META | MLOG_SINGLE_REC_FLAG:

      table_id_t id;
      uint64 version;

      *page_no = FIL_NULL;
      *space = SPACE_UNKNOWN;
	  /*parse a dictionary log record is similar to a regular log record
	   *we also read 2-byte rec_len and 8-byte lsn
	   * */
      new_ptr =
          mlog_parse_initial_dict_log_record(ptr, end_ptr, type, &id, &version);

	  rec_len = mach_read_from_2(new_ptr);
	  assert(rec_len > 0);
	  new_ptr += 2;

	  *rec_lsn = mach_read_from_8(new_ptr);
	  new_ptr += 8;

      if (new_ptr != nullptr) {
        new_ptr = recv_line->metadata_recover->parseMetadataLog(
            id, version, new_ptr, end_ptr);
      }

	  if ( (new_ptr - ptr) != rec_len) {
		  printf("PMEM_WARN parsed len %zu differ to check len %u at rec type %d id %zu version %zu\n", (new_ptr - ptr), rec_len, *type, id, version);
		  assert ( (new_ptr - ptr) == rec_len);
	  }

      return (new_ptr == nullptr ? 0 : new_ptr - ptr);
	}//end switch
	
	//(2) Parse the log header to get space, page_no
		
	//get the header of the log record, the return pointer should point to "rec_len"
	new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);
	
	rec_len = mach_read_from_2(new_ptr);
	assert(rec_len > 0);
	new_ptr += 2;

	*rec_lsn = mach_read_from_8(new_ptr);
	new_ptr += 8;
	
	*body = new_ptr;

	//(3) Varify whether this log rec is need
	ulint key;
	PMEM_FOLD(key, *space, *page_no);

	/*Get the log_block by hashtable. This hashtable is built during analysis phase*/
	//item = pm_ppl_hash_get(pop, ppl, pline, key);

	//if (item == NULL) {
	//	plog_block = NULL;
	//} else {
	//	plog_block = D_RW(D_RW(pline->arr)[item->block_off]);
	//}
	
	plog_block = pm_ppl_hash_get(pop, ppl, pline, key);

	//We don't use this function because it has high overhead O(k)
	//plog_block = pm_ppl_get_log_block_by_key(pop, ppl, key);
	if (plog_block == NULL){
		//the page (space, page_no) has been flushed to disk before the crash
		//do not need recover this page
		*parse_res = PMEM_PARSE_BLOCK_NOT_EXISTED;
		return rec_len;
	}

	assert(plog_block->key == key);	
	
	//if (plog_block->state != PMEM_IN_USED_BLOCK){
	//	//this is ok
	//	//printf("~~ WARN pm_ppl_recv_parse_log_rec() plog_block (%zu, %zu) has state %zu\n", *space, *page_no, plog_block->state);
	//	//assert(0);
	//}

	//test, skip this
	if ((*rec_lsn) < plog_block->firstLSN){
		//this log rec is old, skip it
		*parse_res = PMEM_PARSE_LSN_OLD;
		return rec_len;
	}

	/* We strictly check the first log rec here.
	 * The plog_block->first_rec_found is set false at init and only set true here,
	 * when the first log rec info in plogblock is match with the info in the log rec.
	 * */
	if (!plog_block->first_rec_found) {
		assert(plog_block->first_rec_size == rec_len);
		assert(plog_block->first_rec_type == *type);
		
		/*There is a case one page is flush then read again many times, the info in the plog_block is the last read*/
		/*if plog_block->firstLSN < *rec_lsn, we miss some log recs of this plog_block*/
		/*if plog_block->firstLSN > *rec_lsn, then the corresponding page is modified-flush-read-modified skip log records up to the plogblock->firstLSN*/

		assert(plog_block->firstLSN == *rec_lsn);
		assert(plog_block->start_diskaddr == recv_line->recovered_addr);
		assert(plog_block->start_off == (recv_line->recovered_offset + PMEM_LOG_BUF_HEADER_SIZE));
		//all checks are passed
		plog_block->first_rec_found = true;	
	}

	/* (4) Parse the body as original InnoDB */
	if (UNIV_UNLIKELY(!new_ptr)) {
		assert(0);
		return(0);
	}
	
	/*we reuse this function, at this time only parse, not apply. The return pointer is the next byte after the body 
	 * Note: In MySQL 8.0, there is an additional param at the end of recv_parse_or_apply_log_rec_body() compared to MySQL 5.7. The param just for assert(), just set it to 0
	 * */
	
//	new_ptr = recv_parse_or_apply_log_rec_body(
//		*type, new_ptr, end_ptr, *space, *page_no, NULL, NULL, 0);

	recv_parse_or_apply_log_rec_body(
		*type, new_ptr, end_ptr, *space, *page_no, NULL, NULL, 0);

	if (recv_line != NULL){
		recv_line->found_corrupt_log = recv_sys->found_corrupt_log;
		recv_line->found_corrupt_fs = recv_sys->found_corrupt_fs;
	}

	if (UNIV_UNLIKELY(new_ptr == NULL)) {
		assert(0);
		return(0);
	}

	//check ( (new_ptr - ptr) == rec_len)
	//if ( (new_ptr - ptr) != rec_len) {
	//	printf("PMEM_WARN parsed len %zu differ to check len %u at rec type %d space %u page %u\n", (new_ptr - ptr), rec_len, *type, *space, *page_no);
	//	assert ( (new_ptr - ptr) == rec_len);
	//}

	*is_need = true;
	*parse_res = PMEM_PARSE_NEED;

	//return(new_ptr - ptr);
	return(rec_len);
}

/*simulate recv_sys_empty_hash()
 * Empty the pline's hashtable after all REDO log records in this pline are redone
 *used for both global recv_line and partition recv_line
pline: NULL - use global recv_line
 * */
void 
pm_ppl_recv_line_empty_hash(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline)
{
	uint64_t size;	
	PMEM_RECV_LINE* recv_line;
	ulint avail_mem;

	avail_mem = buf_pool_get_curr_size();
	
	if (pline == NULL){
		recv_line = ppl->recv_line;
		size = avail_mem / 512;
	}
	else {
		recv_line = pline->recv_line;
		size = D_RW(pline->logbuf)->size;
	}

	for (auto &space : *recv_line->spaces) {
		if (space.second.m_heap != nullptr) {
			mem_heap_free(space.second.m_heap);
			space.second.m_heap = nullptr;
		}
	}

	UT_DELETE(recv_line->spaces);
	using Spaces = recv_sys_t::Spaces;

	recv_line->spaces = UT_NEW(Spaces(), mem_log_recv_space_hash_key);
}

/*simulate recv_add_to_hash_table()
 *The caller reponse for holding the lock of hashtable
 * */
void
pm_ppl_recv_add_to_hash_table(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	mlog_id_t	type,		/*!< in: log record type */
	uint32_t		space_id,		/*!< in: space id */
	uint32_t		page_no,	/*!< in: page number */
	byte*		body,		/*!< in: log record body */
	byte*		rec_end,	/*!< in: log record end */
	lsn_t		start_lsn,
	lsn_t		end_lsn) 
{
	recv_t*		recv;
	ulint		len;
	recv_data_t*	recv_data;
	recv_data_t**	prev_field;


	ut_ad(type != MLOG_FILE_DELETE);
	ut_ad(type != MLOG_FILE_CREATE);
	ut_ad(type != MLOG_FILE_RENAME);
	ut_ad(type != MLOG_DUMMY_RECORD);
	ut_ad(type != MLOG_INDEX_LOAD);

	//recv_addr_t*	recv_addr; //MySQL 5.7
	recv_sys_t::Space *space; //MySQL 8.0
	/*get the Space map from the hashtable.
	 * Create the new one if the Space is not existed*/
	space = pm_ppl_recv_get_page_map(recv_line, space_id, true);

	len = rec_end - body;
	
	/*(1) allocate recv obj using space's heap to capture the log record 
	 * MySQL 5.7: Using heap from recv_line
	 * MySQL 8.0: Using heap from the space
	 * */
	//recv = static_cast<recv_t*>(mem_heap_alloc(recv_line->heap, sizeof(recv_t))); //MySQL 5.7
	recv = static_cast<recv_t*>(mem_heap_alloc(space->m_heap, sizeof(recv_t))); //MySQL 8.0

	recv->type = type;
	recv->len = rec_end - body;
	recv->start_lsn = start_lsn;
	recv->end_lsn = end_lsn;
	
	/*In PPL MLOG_UNDO_ERASE_END also has rec_len == 0
	 *REDOing MLOG_UNDO_ERASE_END just need the type, not the rec body
	 * */	
	if (recv->len == 0){
		assert(type == MLOG_INIT_FILE_PAGE2 
				|| type == MLOG_COMP_PAGE_CREATE
				|| type == MLOG_IBUF_BITMAP_INIT
				|| type == MLOG_UNDO_ERASE_END);
	}	

	// (2) get the recv_addr_t from the hash table in this recv_line
	// or create the new one

	auto it = space->m_pages.find(page_no);
	recv_addr_t *recv_addr;

	if (it != space->m_pages.end()) {
		/*found */
		recv_addr = it->second;

	} else {
		/* recv_addr is not existed, create a new one */
		recv_addr = static_cast<recv_addr_t *>(
				mem_heap_alloc(space->m_heap, sizeof(*recv_addr)));

		recv_addr->space = space_id;
		recv_addr->page_no = page_no;
		recv_addr->state = RECV_NOT_PROCESSED;

		UT_LIST_INIT(recv_addr->rec_list, &recv_t::rec_list);

		using value_type = recv_sys_t::Pages::value_type;

		space->m_pages.insert(it, value_type(page_no, recv_addr));

		++recv_line->n_addrs;
	}
	/*Add the REDO log in the recv_addr's list*/
	UT_LIST_ADD_LAST(recv_addr->rec_list, recv);

	
	/*(3) recv_addr only has the header, now link it with the log record's body */
	prev_field = &(recv->data);

	/* Store the log record body in chunks of less than UNIV_PAGE_SIZE:
	 * recv_line->heap grows into the buffer pool, and bigger chunks could not
	be allocated */

	while (rec_end > body) {

		len = rec_end - body;

		if (len > RECV_DATA_BLOCK_SIZE) {
			len = RECV_DATA_BLOCK_SIZE;
		}

		recv_data = static_cast<recv_data_t*>
			//(mem_heap_alloc(recv_line->heap, sizeof(recv_data_t) + len));
			(mem_heap_alloc(space->m_heap, sizeof(recv_data_t) + len));

		*prev_field = recv_data;

		memcpy(recv_data + 1, body, len);

		prev_field = &(recv_data->next);

		body += len;
	}

	*prev_field = nullptr;
}

recv_sys_t::Space*
pm_ppl_recv_get_page_map(
		PMEM_RECV_LINE* recv_line,
		space_id_t space_id,
		bool create) 
{
  auto it = recv_line->spaces->find(space_id);

  if (it != recv_line->spaces->end()) {
    return (&it->second);

  } else if (create) {
    mem_heap_t *heap;

    heap = mem_heap_create_typed(256, MEM_HEAP_FOR_RECV_SYS);

    using Space = recv_sys_t::Space;
    using value_type = recv_sys_t::Spaces::value_type;

    auto where =
        recv_line->spaces->insert(it, value_type(space_id, Space(heap)));

    return (&where->second);
  }
  /*if create==false then return NULL*/
  return (nullptr);
}

ulint
pm_ppl_recv_get_max_recovered_lsn(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl)
{
	uint32_t n, i;
	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
	
	ulint max_lsn = 0;
	
	n = ppl->n_buckets;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;

		if (max_lsn < recv_line->recovered_lsn){
			max_lsn = recv_line->recovered_lsn;
		}
	}

	return max_lsn;
}

/*Simulate recv_get_rec
 * Search the object from the partition hashtable given the space_id and page_no
 *
 *@param[in] recv_line: The partition recv line
 @param[in] space_id: space id
 @param[in] page_no: page no
 @return the matched recv_addr_t struct in the hashtable if the object found. Otherwise return NULL
 * */
recv_addr_t*
pm_ppl_recv_get_rec(
	PMEM_RECV_LINE* recv_line,
	uint32_t	space_id,
	uint32_t	page_no) 
{
  recv_sys_t::Space *space;
  space = pm_ppl_recv_get_page_map(recv_line, space_id, false);

  if (space != nullptr) {
    auto it = space->m_pages.find(page_no);

    if (it != space->m_pages.end()) {
      return (it->second);
    }
  }

  return (nullptr);
}

/*
 *Simulate recv_recover_page_func()
 Be awared that this is multiple thread access function
 Note that this function is called from two different threads
 Thread 1: recovery thread in pm_ppl_recv_apply_hashed_line -> pm_ppl_recv_recover_page_func() 
 Thread 2: IO thread -> fil_io_wait() -> buf_page_io_complete() -> recv_recover_page_func() -> pm_ppl_recv_recover_page_func()

 So we need to lock the line to avoid inconsistent access
 * */
void
pm_ppl_recv_recover_page_func(
	PMEMobjpool*				pop,
	PMEM_PAGE_PART_LOG*			ppl,
	PMEM_PAGE_LOG_HASHED_LINE*	pline,
	ibool						just_read_in,
				/*!< in: TRUE if the i/o handler calls
				this for a freshly read page */
	buf_block_t*				block)	/*!< in/out: buffer block */
{
	PMEM_RECV_LINE* recv_line;

	page_t*		page;
	page_zip_des_t*	page_zip;
	recv_addr_t*	recv_addr;
	recv_t*		recv;
	recv_t*		prev_recv;

	byte*		buf;
	lsn_t		start_lsn;
	lsn_t		end_lsn;
	lsn_t		page_lsn;
	lsn_t		page_newest_lsn;
	ibool		modification_to_page;
	mtr_t		mtr;
	
	bool is_gb_ht = (pline == nullptr);

	if(is_gb_ht){	
		recv_line = ppl->recv_line;
	}
	else {
		recv_line = pline->recv_line;
	}

	if (recv_line->apply_log_recs == FALSE) {
		/* This must be the REDO phase 1
		 * Log records should not be applied now */
		return;
	}
	
	/*why we need the lock here?
	 *If we ensure that at most one thread could access on the corresponding recv_addr of a page at the time, then mutex is unnecessary.
	 The problem with the old design is pm_ppl_recv_read_in_area() read the seqnential pages as batch. So there is contention between two threads (the redoer worker and the IO thread) access on a page.
	 * */
	//pmemobj_rwlock_wrlock(pop, &recv_line->lock);	

	recv_addr = pm_ppl_recv_get_rec(recv_line, block->page.id.space(), block->page.id.page_no());

	if ((recv_addr == nullptr)
	    || (recv_addr->state == RECV_BEING_PROCESSED)
	    || (recv_addr->state == RECV_PROCESSED)) {
		ut_ad(recv_addr == nullptr || recv_needed_recovery);

		//pmemobj_rwlock_unlock(pop, &recv_line->lock);	
		return;
	}

	buf_page_t bpage = block->page;

	if (!fsp_is_system_temporary(bpage.id.space()) &&
			(arch_page_sys != nullptr && arch_page_sys->is_active())) {
		page_t *frame;
		lsn_t frame_lsn;

		frame = bpage.zip.data;

		if (!frame) {
			frame = block->frame;
		}
		frame_lsn = mach_read_from_8(frame + FIL_PAGE_LSN);

		arch_page_sys->track_page(&bpage, LSN_MAX, frame_lsn, true);
	}

	recv_addr->state = RECV_BEING_PROCESSED;

	//pmemobj_rwlock_unlock(pop, &recv_line->lock);	

	mtr_start(&mtr);
	mtr_set_log_mode(&mtr, MTR_LOG_NONE);

	page = block->frame;
	page_zip = buf_block_get_page_zip(block);

	if (just_read_in) {
		/*PL-NVM doesn't use n_read_done now*/
		/*
		pmemobj_rwlock_wrlock(pop, &recv_line->lock);	
		recv_line->n_read_done++;
		pmemobj_rwlock_unlock(pop, &recv_line->lock);	
		*/

		/* Move the ownership of the x-latch on the page to
		this OS thread, so that we can acquire a second
		x-latch on it.  This is needed for the operations to
		the page to pass the debug checks. */

		rw_lock_x_lock_move_ownership(&block->lock);
	}

	bool success = buf_page_get_known_nowait(
			RW_X_LATCH, block, Cache_hint::KEEP_OLD, __FILE__, __LINE__, &mtr);
	ut_a(success);

	buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

	/* Read the newest modification lsn from the page */
	page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

	page_newest_lsn = buf_page_get_newest_modification(&block->page);

	if (page_newest_lsn) {

		page_lsn = page_newest_lsn;
	}

	modification_to_page = FALSE;
	start_lsn = end_lsn = 0;
	
	fil_space_t* space = fil_space_get(recv_addr->space);
	assert(space != NULL);

	recv = UT_LIST_GET_FIRST(recv_addr->rec_list);
	
	//parse and apply each recv in the hashtable
	//ulint prev_start_lsn = 0;
	prev_recv = NULL;	

	for (auto recv = UT_LIST_GET_FIRST(recv_addr->rec_list); recv != nullptr;
			recv = UT_LIST_GET_NEXT(rec_list, recv)) {
#ifndef UNIV_HOTBACKUP
		end_lsn = recv->end_lsn;
#endif /* !UNIV_HOTBACKUP */

		byte *buf;

		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			/* We have to copy the record body to a separate
			   buffer */

			buf = static_cast<byte *>(ut_malloc_nokey(recv->len));

			recv_data_copy_to_buf(buf, recv);
		} else {
			buf = ((byte *)(recv->data)) + sizeof(recv_data_t);
		}

		if (recv->type == MLOG_INIT_FILE_PAGE) {
			page_lsn = page_newest_lsn;

			memset(FIL_PAGE_LSN + page, 0, 8);
			memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page, 0, 8);

			if (page_zip) {
				memset(FIL_PAGE_LSN + page_zip->data, 0, 8);
			}
		}

		/* Ignore applying the redo logs for tablespace that is
		   truncated. Truncated tablespaces are handled explicitly
		   post-recovery, where we will restore the tablespace back
		   to a normal state.

		   Applying redo at this stage will cause problems because the
		   redo will have action recorded on page before tablespace
		   was re-inited and that would lead to a problem later. */

		if (recv->start_lsn >= page_lsn
#ifndef UNIV_HOTBACKUP
				/*PL_NVM skip this check (high overhead check)*/
				//&& undo::is_active(recv_addr->space)
#endif /* !UNIV_HOTBACKUP */
		   ) {

			lsn_t end_lsn;

			if (!modification_to_page) {
#ifndef UNIV_HOTBACKUP
				ut_a(recv_needed_recovery);
#endif /* !UNIV_HOTBACKUP */
				modification_to_page = true;
				start_lsn = recv->start_lsn;
			}

			DBUG_PRINT("ib_log", ("apply " LSN_PF ":"
						" %s len " ULINTPF " page %u:%u",
						recv->start_lsn, get_mlog_string(recv->type),
						recv->len, recv_addr->space, recv_addr->page_no));

			recv_parse_or_apply_log_rec_body(recv->type, buf, buf + recv->len,
					recv_addr->space, recv_addr->page_no,
					block, &mtr, ULINT_UNDEFINED);

			end_lsn = recv->start_lsn + recv->len;

			mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);

			mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page,
					end_lsn);

			if (page_zip) {
				mach_write_to_8(FIL_PAGE_LSN + page_zip->data, end_lsn);
			}
#ifdef UNIV_HOTBACKUP
			++applied_recs;
		} else {
			++skipped_recs;
#endif /* UNIV_HOTBACKUP */
		}

		if (recv->len > RECV_DATA_BLOCK_SIZE) {
			ut_free(buf);
		}
	} //end for loop


	if (modification_to_page) {
		ut_a(block);
		buf_flush_recv_note_modification(block, start_lsn, end_lsn);
	}

	/* Make sure that committing mtr does not change the modification lsn values of page */

	mtr.discard_modifications();

	mtr_commit(&mtr);

	if (recv_max_page_lsn < page_lsn) {
		recv_max_page_lsn = page_lsn;
	}

	recv_addr->state = RECV_PROCESSED;

	pmemobj_rwlock_wrlock(pop, &recv_line->lock);	

	ut_a(recv_line->n_addrs);

	recv_line->n_addrs--;

	/*the last IO thread handle post-processing*/

	if (recv_line->n_addrs == 0)
	{
		recv_line->apply_log_recs = FALSE;
		recv_line->apply_batch_on = FALSE;

		//simulate recv_sys_empty_hash()
		pm_ppl_recv_line_empty_hash(pop, ppl, pline);
		
		/*update the total redoing lines 
		 * The last finished redo pline response for waking up the main recovery thread*/
		pmemobj_rwlock_wrlock(pop, &ppl->recv_lock);	

		ppl->n_redoing_lines--;

		printf("PMEM_RECV: ppl->n_redoing_lines %d \n", ppl->n_redoing_lines);
		if (ppl->n_redoing_lines == 0){
			os_event_set(ppl->redoing_done_event);
		}

		pmemobj_rwlock_unlock(pop, &ppl->recv_lock);	
	}

	pmemobj_rwlock_unlock(pop, &recv_line->lock);	
}

/*
 * Simulate recv_read_in_area()
 * read pages near the input page
 * the read pages are 
 * (1) in the hashtable of recv_line
 * (2) has the same space_no with input page_id
 * (3) near the input page (page_no +/- RECV_READ_AHEAD_AREA)
 * */
ulint
pm_ppl_recv_read_in_area(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_RECV_LINE* recv_line,
	const page_id_t&	page_id)
{
	recv_addr_t* recv_addr;
	ulint	low_limit;
	ulint	n;

	low_limit = page_id.page_no()
		- (page_id.page_no() % RECV_READ_AHEAD_AREA);

	n = 0;

	std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;
	
	/*This loop collect page_no in the hashtable of the recv_line*/
	for (page_no_t page_no = low_limit;
			page_no < low_limit + RECV_READ_AHEAD_AREA; ++page_no) {
		recv_addr_t *recv_addr;

		recv_addr = pm_ppl_recv_get_rec(recv_line, page_id.space(), page_no);

		const page_id_t cur_page_id(page_id.space(), page_no);

		if (recv_addr != nullptr && !buf_page_peek(cur_page_id)) {
			/* Only read pages that:
			 * 1) exist in hashtable
			 * 2) not in cache*/
			if (recv_addr->state == RECV_NOT_PROCESSED) {
				recv_addr->state = RECV_BEING_READ;

				page_nos[n] = page_no;

				++n;
			}
		}
	}
	pm_ppl_buf_read_recv_pages(pop, ppl, recv_line, FALSE, page_id.space(), &page_nos[0], n);

	return(n);
}

///////////////// APPLY PHASE //////////////

void
pm_ppl_recv_apply_single_page(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ulint space, ulint page_no)
{

	mtr_t	mtr;

	const page_id_t		page_id(space, page_no);
	bool			found;
	const page_size_t&	page_size
		= fil_space_get_page_size(space, &found);

	ut_ad(found);
	if (buf_page_peek(page_id)) {
		buf_block_t*	block;

		mtr_start(&mtr);

		block = buf_page_get(
				page_id, page_size,
				RW_X_LATCH, &mtr);

		buf_block_dbg_add_level(
				block, SYNC_NO_ORDER_CHECK);

		pm_ppl_recv_recover_page_func( 
				pop, ppl, pline, FALSE, block);

		mtr_commit(&mtr);
	} else {
		// apply is done in IO thread after this read call is called
		bool read_ok = buf_read_page(
				page_id, page_size);

		assert(read_ok);
	}
}
/*
 *Apply important pages prior to apply other 
 For system tablespace
 1) page 0
 2) trx_sys page (page 5)
 For each dirty space we apply
 1) page 0
 2) INODE (page 2)
 3) root clustered index page
 4) root non-clustered index page
 * */

void
pm_ppl_recv_apply_prior_pages(
	PMEMobjpool*		pop,
	PMEM_PAGE_PART_LOG*	ppl,
	PMEM_PAGE_LOG_HASHED_LINE* pline,
	ibool	allow_ibuf)
{
	fil_space_t* space;
	space_id_t space_id;

	recv_addr_t* recv_addr;
	uint32_t j;
	
	PMEM_RECV_LINE* recv_line;

	uint32_t page_no_arr[] = {0, 2, 3, 4};
	uint32_t page_no_arr_len = 4;

	if (pline != NULL){ 
		recv_line	= pline->recv_line;
	} else {
		recv_line = ppl->recv_line;
	}

	recv_line->apply_log_recs = TRUE;
	recv_line->apply_batch_on = TRUE;

	//Apply first page System tablespace
	recv_addr = pm_ppl_recv_get_rec(recv_line, 0, 0);
	if (recv_addr != NULL){
		printf("Apply space 0 page 0\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}

	recv_addr = pm_ppl_recv_get_rec(recv_line, 0, 2);
	if (recv_addr != NULL){
		printf("Apply space 0 page 2 (INODE)\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}

	recv_addr = pm_ppl_recv_get_rec(recv_line, 0, 5);
	if (recv_addr != NULL){
		printf("Apply space 0 page 5 (SYS_TRX)\n");
		pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
	}
	
	//Apply first page for remain spaces

	for (auto &it : *recv_sys->spaces) {
		space_id = it.first;	

		for (j = 0; j < page_no_arr_len; j++)
		{
			uint32_t page_no = page_no_arr[j];

			recv_addr = pm_ppl_recv_get_rec(recv_line, space_id, page_no);

			if (recv_addr != NULL){
				//printf("Apply page %u for space %zu name %s\n", page_no, space->id, space->name);
				pm_ppl_recv_apply_single_page(pop, ppl, pline, recv_addr->space, recv_addr->page_no);
			} else {
				//printf("Page %u of space %zu name %s is NOT in hashtable\n",page_no, space->id, space->name);
			}
		}
	} //end outer for

	printf("PMEM_INFO: END apply prior pages\n");
}
/*
 * REDO2 (APPLY PHASE) 
 * Simulate recv_apply_hashed_log_recs()
 * Thread coordinator (create/close threads). The logic steps are:
 *
 * (1) create redoer threads
 * (2) assign each redoer thread with number of plines
 * (3) trigger the redoer thread. Worker thread in turned, call pm_ppl_recv_apply_hashed_line()
 * (4) wait until the last pline is completly applied REDO logs then return to the caller
 *
 * See pm_log_redoer_worker()
 * */
void
pm_ppl_recv_apply_hashed_log_recs(
		PMEMobjpool*		pop,
		PMEM_PAGE_PART_LOG*	ppl,
		ibool	allow_ibuf)
{
	//bool			is_multi_redo = false;	
	bool			is_multi_redo = true;	
	uint32_t		n, i;
	uint32_t		begin;

	TOID(PMEM_PAGE_LOG_HASHED_LINE) line;
	PMEM_PAGE_LOG_HASHED_LINE*		pline;
	PMEM_RECV_LINE*					recv_line;
    PMEM_LOG_REDOER*				redoer;
	
	/*set this variable to a value > n lines to skip applying (debug only)*/
	begin = 0;

	n = ppl->n_buckets;

	if (IS_GLOBAL_HASHTABLE) {
		//test use global hashtable
		recv_line = ppl->recv_line;
		recv_line->is_ibuf_avail = allow_ibuf;

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		printf("===> PMEM_RECV Phase 2: START Applying GLOBAL line n_cell %zu\n", recv_line->n_addrs);
#endif

		pm_ppl_recv_apply_hashed_line(pop, ppl, NULL, recv_line->is_ibuf_avail);

		return;
		//end test use global hashtable
	}
	
	if (!is_multi_redo){
		/*Sequentially REDO each line (debug only) */
		ppl->n_redoing_lines = 0;
		ppl->is_redoing_done = false;
		
		/*Filter out what we need*/
		for (i = begin; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;
			if (recv_line->n_addrs > 0)
				ppl->n_redoing_lines++;
		}

		for (i = begin; i < n; i++) {
			pline = D_RW(D_RW(ppl->buckets)[i]);
			recv_line = pline->recv_line;

			if (recv_line->n_addrs == 0)
				continue;

			recv_line->is_ibuf_avail = allow_ibuf;

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
			printf("===> PMEM_RECV Phase 2: START Applying line %zu n_cell %zu\n", pline->hashed_id, pline->recv_line->n_addrs);
#endif
			pm_ppl_recv_apply_hashed_line(pop, ppl, pline, pline->recv_line->is_ibuf_avail);

#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
			printf("===>done Applying line %zu n_cell %zu\n", pline->hashed_id, pline->recv_line->n_addrs);
#endif
		}
		printf("\nPMEM_RECV: ===== wait for all AIO threads finish REDOing...\n");
		while (ppl->n_redoing_lines > 0){
			os_event_wait(ppl->redoing_done_event);
		}
#if defined (UNIV_PMEMOBJ_PART_PL_DEBUG)
		printf("\nPMEM_RECV: ===== Applying completed ======\n");
#endif
		return;
	}
	
	/* Parallelism APPLYING*/

    //(1) Init the redoers and create threads
    redoer = pm_log_redoer_init(n);
	
	//set type as APPLY
	redoer->phase = PMEM_REDO_PHASE2;

    ppl->redoer = redoer;

    redoer->n_remains = n;

	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;
		
        if (recv_line->n_addrs == 0){
            redoer->n_remains--;
        }
    }

    printf("PMEM_RECV: Need applying %zu lines\n", redoer->n_remains);

    ppl->is_redoing_done = false;
	ppl->n_redoing_lines = redoer->n_remains;

    //create threads
    for (i = 0; i < srv_ppl_n_redoer_threads; ++i) {
        //os_thread_create(pm_log_redoer_worker, NULL, NULL);
        os_thread_create(pm_log_redoer_thread_key, pm_log_redoer_worker);
    }
    
    // (2) Assign pointers
	for (i = 0; i < n; i++) {
		pline = D_RW(D_RW(ppl->buckets)[i]);
		recv_line = pline->recv_line;
        
        //this variable is set true after a thread accept to REDO this line
        pline->is_redoing = false;

        /*if a line has recv_diskaddr == ULONG_MAX, that line has all free page block => skip REDO*/
        if (recv_line->n_addrs == 0)
            continue;
		
		recv_line->is_ibuf_avail = allow_ibuf;
        //Asign pline to a redoer thread
        redoer->hashed_line_arr[i] = pline;
    }

    /*trigger REDOer threads phase 2. 
	 * Call pm_ppl_recv_apply_hashed_line() */
    os_event_set(redoer->is_log_req_not_empty);
    
    //(3) wait for all REDOER threads finish REDOing
    printf("\nPMEM_RECV: ===== wait for all REDOer threads finish REDOing...\n");
    while (redoer->n_remains > 0){
        os_event_wait(redoer->is_log_all_finished);
    }

    //(4) wait for all AIO threads finish REDOing
    printf("\nPMEM_RECV: ===== wait for all AIO threads finish REDOing...\n");
    while (ppl->n_redoing_lines > 0){
        os_event_wait(ppl->redoing_done_event);
    }

//finish: 
    //(5) finish
    pm_log_redoer_close(ppl->redoer);
    printf("\nPMEM_RECV: ===== Applying completed ======\n");
}
/*
 * will not used in MySQL 8.0
pm_ppl_recv_check_hashed_line()
*/

/*
 * Apply log recs from pline's hashtable to data pages.
 * This function is the alter version of recv_apply_hashed_log_recs() and recv_apply_log_rec()
 *
 *allow_ibuf only is set FALSE in the begining of recv_group_scan_log_recs()
 
 @param[in] pop
 @param[in] ppl
 @param[in/out] pline - pline to apply log recs 
 @param[in] allow_ibuf
 * */
void
pm_ppl_recv_apply_hashed_line(
	PMEMobjpool*				pop,
	PMEM_PAGE_PART_LOG*			ppl,
	PMEM_PAGE_LOG_HASHED_LINE*	pline,
	ibool						allow_ibuf)
{
	recv_addr_t*	recv_addr;
	ulint			i;
	ulint			cnt, cnt2;
	ulint			n, n_cells;
	mtr_t			mtr;
	PMEM_RECV_LINE* recv_line;

	if (pline != NULL){ 
		recv_line	= pline->recv_line;
	} else {
		recv_line = ppl->recv_line;
	}

	if (!allow_ibuf) {
		recv_no_ibuf_operations = true;
	}

	recv_line->apply_log_recs = TRUE;
	recv_line->apply_batch_on = TRUE;
	
	recv_line->skip_zero_page = true;
	
	/*Note that building UNDO pages could cause n_read_done increase, we reset it here to match with n_read_reqs*/
	recv_line->n_read_reqs = 0;
	recv_line->n_read_done = 0;

	cnt = 0;
	cnt2 = 10;

	n = recv_line->n_addrs;
	
	/*A. New implementation, combine read in area*/
	std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;
	ulint n_pages = 0;

	/*for each space in the map Spaces*/
	for (const auto &space : *recv_line->spaces) {

		/*for each page in the hashmap Pages */
		for (auto pages : space.second.m_pages) {

			recv_addr = pages.second;

			if (recv_addr->state == RECV_DISCARDED) {
				pmemobj_rwlock_wrlock(pop, &recv_line->lock);	
				ut_a(recv_line->n_addrs);
				recv_line->n_addrs--;
				recv_line->n_skip_done++;
				pmemobj_rwlock_unlock(pop, &recv_line->lock);	
				continue;
			}

			const page_id_t		page_id(recv_addr->space, recv_addr->page_no);
			bool				found;
			const page_size_t page_size =
				fil_space_get_page_size(recv_addr->space, &found);

			ut_ad(found);

			if (recv_addr->state == RECV_NOT_PROCESSED) {
				if (buf_page_peek(page_id)) {
					/*page is cached, directly recover it*/
					buf_block_t*	block;

					mtr_start(&mtr);

					block = buf_page_get( page_id, page_size, RW_X_LATCH, &mtr);

					buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);
					
					/*apply REDO logs from the hashtable on cache page
					 *This function also response for reduce recv_line->n_addrs by one
					 * */
					pm_ppl_recv_recover_page_func(pop, ppl, pline, FALSE, block);

					mtr_commit(&mtr);
				} else {
					/*we combine what recv_read_in_area here*/
					recv_addr->state = RECV_BEING_READ;
					page_nos[n_pages] = recv_addr->page_no;
					++n_pages;

					if (n_pages >= RECV_READ_AHEAD_AREA){
						pm_ppl_buf_read_recv_pages(pop, ppl, recv_line, FALSE, space.first, &page_nos[0], n_pages);

						/*reset*/
						//std::fill_n(page_nos, n_pages, 0);
						page_nos.fill(0);
						n_pages = 0;
					}
					//pm_ppl_recv_read_in_area (pop, ppl, recv_line, page_id);
				}
			}

			if (recv_line->n_addrs == 0){
				//The REDO finished by AIO threads
				return;
			}
		} //end inner for
		/*now we read all remains pages in current space for REDOing*/
		pm_ppl_buf_read_recv_pages(pop, ppl, recv_line, FALSE, space.first, &page_nos[0], n_pages);

		/*reset*/
		page_nos.fill(0);
		n_pages = 0;

		if (recv_line->n_addrs == 0){
			//The REDO finished by AIO threads
			return;
		}
	} //end outer for

	/*B. Old implementation*/
//
//	/*for each space in the hashmap Spaces*/
//	for (const auto &space : *recv_line->spaces) {
//		/*for each page in the hashmap Pages */
//		for (auto pages : space.second.m_pages) {
//
//			//recv_apply_log_rec(pages.second);
//			/*simulate recv_apply_log_rec()*/
//			//TODO: see recv_apply_log_rec() and compare with our function in MySQL 5.7 to continue.
//			recv_addr = pages.second;
//
//			if (recv_addr->state == RECV_DISCARDED) {
//				pmemobj_rwlock_wrlock(pop, &recv_line->lock);	
//				ut_a(recv_line->n_addrs);
//				recv_line->n_addrs--;
//				recv_line->n_skip_done++;
//				pmemobj_rwlock_unlock(pop, &recv_line->lock);	
//				continue;
//			}
//
//			const page_id_t		page_id(recv_addr->space, recv_addr->page_no);
//			bool				found;
//			const page_size_t page_size =
//				fil_space_get_page_size(recv_addr->space, &found);
//
//			ut_ad(found);
//
//			if (recv_addr->state == RECV_NOT_PROCESSED) {
//				if (buf_page_peek(page_id)) {
//					/*page is cached, directly recover it*/
//					buf_block_t*	block;
//
//					mtr_start(&mtr);
//
//					block = buf_page_get( page_id, page_size, RW_X_LATCH, &mtr);
//
//					buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);
//					
//					/*apply REDO logs from the hashtable on cache page
//					 *This function also response for reduce recv_line->n_addrs by one
//					 * */
//					pm_ppl_recv_recover_page_func(pop, ppl, pline, FALSE, block);
//
//					mtr_commit(&mtr);
//				} else {
//					/* page is not cached, fetch it from disk and apply is done in IO thread -> pm_ppl_recv_recover_page_func */
//					pm_ppl_recv_read_in_area (pop, ppl, recv_line, page_id);
//				}
//			}
//
//			cnt++;
//
//			if (IS_GLOBAL_HASHTABLE){
//				if (cnt * 1.0 * 100 / n > cnt2){
//					printf("Global HT, Apply cell %zu / %zu cnt %zu cnt2 %zu percentage processed \n", i, n_cells, cnt, cnt2);
//					cnt2 += 10;
//				}
//			}
//
//			if (recv_line->n_addrs == 0){
//				//The REDO finished by AIO threads
//				return;
//			}
//		} //end inner for
//
//		if (recv_line->n_addrs == 0){
//			//The REDO finished by AIO threads
//			return;
//		}
//	} //end outer for
	
}
////////////////////////////////////////////////////////
#endif //UNIV_PMEMOBJ_PART_PL
