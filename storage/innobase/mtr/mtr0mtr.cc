/*****************************************************************************

Copyright (c) 1995, 2019, Oracle and/or its affiliates. All Rights Reserved.

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

/** @file mtr/mtr0mtr.cc
 Mini-transaction buffer

 Created 11/26/1995 Heikki Tuuri
 *******************************************************/

#include "mtr0mtr.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "fsp0sysspace.h"
#ifndef UNIV_HOTBACKUP
#include "log0log.h"
#include "log0recv.h"
#include "mtr0log.h"
#endif /* !UNIV_HOTBACKUP */
#include "my_dbug.h"
#ifndef UNIV_HOTBACKUP
#include "page0types.h"
#include "trx0purge.h"
#endif /* !UNIV_HOTBACKUP */

#if defined (UNIV_TRACE_FLUSH_TIME)
extern volatile int64 gb_write_log_time;
extern volatile int64 gb_n_write_log;
#endif

#if defined (UNIV_PMEMOBJ_PART_PL) || defined (UNIV_PMEMOBJ_WAL_ELR) || defined(UNIV_PMEMOBJ_WAL)
#include "my_pmemobj.h"
extern PMEM_WRAPPER* gb_pmw; 
#endif /* UNIV_PMEMOBJ_PART_PL */

static_assert(static_cast<int>(MTR_MEMO_PAGE_S_FIX) ==
                  static_cast<int>(RW_S_LATCH),
              "");

static_assert(static_cast<int>(MTR_MEMO_PAGE_X_FIX) ==
                  static_cast<int>(RW_X_LATCH),
              "");

static_assert(static_cast<int>(MTR_MEMO_PAGE_SX_FIX) ==
                  static_cast<int>(RW_SX_LATCH),
              "");

/** Iterate over a memo block in reverse. */
template <typename Functor>
struct Iterate {
  /** Release specific object */
  explicit Iterate(Functor &functor) : m_functor(functor) { /* Do nothing */
  }

  /** @return false if the functor returns false. */
  bool operator()(mtr_buf_t::block_t *block) {
    const mtr_memo_slot_t *start =
        reinterpret_cast<const mtr_memo_slot_t *>(block->begin());

    mtr_memo_slot_t *slot = reinterpret_cast<mtr_memo_slot_t *>(block->end());

    ut_ad(!(block->used() % sizeof(*slot)));

    while (slot-- != start) {
      if (!m_functor(slot)) {
        return (false);
      }
    }

    return (true);
  }

  Functor &m_functor;
};

/** Find specific object */
struct Find {
  /** Constructor */
  Find(const void *object, ulint type)
      : m_slot(), m_type(type), m_object(object) {
    ut_a(object != NULL);
  }

  /** @return false if the object was found. */
  bool operator()(mtr_memo_slot_t *slot) {
    if (m_object == slot->object && m_type == slot->type) {
      m_slot = slot;
      return (false);
    }

    return (true);
  }

  /** Slot if found */
  mtr_memo_slot_t *m_slot;

  /** Type of the object to look for */
  ulint m_type;

  /** The object instance to look for */
  const void *m_object;
};

/** Find a page frame */
struct Find_page {
  /** Constructor
  @param[in]	ptr	pointer to within a page frame
  @param[in]	flags	MTR_MEMO flags to look for */
  Find_page(const void *ptr, ulint flags)
      : m_ptr(ptr), m_flags(flags), m_slot(NULL) {
    /* We can only look for page-related flags. */
    ut_ad(!(flags &
            ~(MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX |
              MTR_MEMO_BUF_FIX | MTR_MEMO_MODIFY)));
  }

  /** Visit a memo entry.
  @param[in]	slot	memo entry to visit
  @retval	false	if a page was found
  @retval	true	if the iteration should continue */
  bool operator()(mtr_memo_slot_t *slot) {
    ut_ad(m_slot == NULL);

    if (!(m_flags & slot->type) || slot->object == NULL) {
      return (true);
    }

    buf_block_t *block = reinterpret_cast<buf_block_t *>(slot->object);

    if (m_ptr < block->frame ||
        m_ptr >= block->frame + block->page.size.logical()) {
      return (true);
    }

    m_slot = slot;
    return (false);
  }

  /** @return the slot that was found */
  mtr_memo_slot_t *get_slot() const {
    ut_ad(m_slot != NULL);
    return (m_slot);
  }
  /** @return the block that was found */
  buf_block_t *get_block() const {
    return (reinterpret_cast<buf_block_t *>(get_slot()->object));
  }

 private:
  /** Pointer inside a page frame to look for */
  const void *const m_ptr;
  /** MTR_MEMO flags to look for */
  const ulint m_flags;
  /** The slot corresponding to m_ptr */
  mtr_memo_slot_t *m_slot;
};

/** Release latches and decrement the buffer fix count.
@param[in]	slot	memo slot */
static void memo_slot_release(mtr_memo_slot_t *slot) {
  switch (slot->type) {
#ifndef UNIV_HOTBACKUP
    buf_block_t *block;
#endif /* !UNIV_HOTBACKUP */

    case MTR_MEMO_BUF_FIX:
    case MTR_MEMO_PAGE_S_FIX:
    case MTR_MEMO_PAGE_SX_FIX:
    case MTR_MEMO_PAGE_X_FIX:
#ifndef UNIV_HOTBACKUP
      block = reinterpret_cast<buf_block_t *>(slot->object);

      buf_block_unfix(block);
      buf_page_release_latch(block, slot->type);
#endif /* !UNIV_HOTBACKUP */
      break;

    case MTR_MEMO_S_LOCK:
      rw_lock_s_unlock(reinterpret_cast<rw_lock_t *>(slot->object));
      break;

    case MTR_MEMO_SX_LOCK:
      rw_lock_sx_unlock(reinterpret_cast<rw_lock_t *>(slot->object));
      break;

    case MTR_MEMO_X_LOCK:
      rw_lock_x_unlock(reinterpret_cast<rw_lock_t *>(slot->object));
      break;

#ifdef UNIV_DEBUG
    default:
      ut_ad(slot->type == MTR_MEMO_MODIFY);
#endif /* UNIV_DEBUG */
  }

  slot->object = NULL;
}

/** Release the latches and blocks acquired by the mini-transaction. */
struct Release_all {
  /** @return true always. */
  bool operator()(mtr_memo_slot_t *slot) const {
    if (slot->object != NULL) {
      memo_slot_release(slot);
    }

    return (true);
  }
};

/** Check that all slots have been handled. */
struct Debug_check {
  /** @return true always. */
  bool operator()(const mtr_memo_slot_t *slot) const {
    ut_a(slot->object == NULL);
    return (true);
  }
};

/** Add blocks modified by the mini-transaction to the flush list. */
struct Add_dirty_blocks_to_flush_list {
  /** Constructor.
  @param[in]	start_lsn	LSN of the first entry that was
                                  added to REDO by the MTR
  @param[in]	end_lsn		LSN after the last entry was
                                  added to REDO by the MTR
  @param[in,out]	observer	flush observer */
  Add_dirty_blocks_to_flush_list(lsn_t start_lsn, lsn_t end_lsn,
                                 FlushObserver *observer);

  /** Add the modified page to the buffer flush list. */
  void add_dirty_page_to_flush_list(mtr_memo_slot_t *slot) const {
#if defined (UNIV_PMEMOBJ_PART_PL)
    ut_ad(m_end_lsn >= m_start_lsn || (m_end_lsn == 0 && m_start_lsn == 0));
#else //original
    ut_ad(m_end_lsn > m_start_lsn || (m_end_lsn == 0 && m_start_lsn == 0));
#endif //UNIV_PMEMOBJ_PART_PL

#ifndef UNIV_HOTBACKUP
    buf_block_t *block;

    block = reinterpret_cast<buf_block_t *>(slot->object);

    buf_flush_note_modification(block, m_start_lsn, m_end_lsn,
                                m_flush_observer);
#endif /* !UNIV_HOTBACKUP */
  }

  /** @return true always. */
  bool operator()(mtr_memo_slot_t *slot) const {
    if (slot->object != NULL) {
      if (slot->type == MTR_MEMO_PAGE_X_FIX ||
          slot->type == MTR_MEMO_PAGE_SX_FIX) {
        add_dirty_page_to_flush_list(slot);

      } else if (slot->type == MTR_MEMO_BUF_FIX) {
        buf_block_t *block;
        block = reinterpret_cast<buf_block_t *>(slot->object);
        if (block->made_dirty_with_no_latch) {
          add_dirty_page_to_flush_list(slot);
          block->made_dirty_with_no_latch = false;
        }
      }
    }

    return (true);
  }

  /** Mini-transaction REDO end LSN */
  const lsn_t m_end_lsn;

  /** Mini-transaction REDO start LSN */
  const lsn_t m_start_lsn;

  /** Flush observer */
  FlushObserver *const m_flush_observer;
};

/** Constructor.
@param[in]	start_lsn	LSN of the first entry that was added
                                to REDO by the MTR
@param[in]	end_lsn		LSN after the last entry was added
                                to REDO by the MTR
@param[in,out]	observer	flush observer */
Add_dirty_blocks_to_flush_list::Add_dirty_blocks_to_flush_list(
    lsn_t start_lsn, lsn_t end_lsn, FlushObserver *observer)
    : m_end_lsn(end_lsn), m_start_lsn(start_lsn), m_flush_observer(observer) {
  /* Do nothing */
}

class mtr_t::Command {
 public:
  /** Constructor.
  Takes ownership of the mtr->m_impl, is responsible for deleting it.
  @param[in,out]	mtr	mini-transaction */
  explicit Command(mtr_t *mtr) : m_locks_released() { init(mtr); }

  void init(mtr_t *mtr) {
    m_impl = &mtr->m_impl;
    m_sync = mtr->m_sync;
  }

  /** Destructor */
  ~Command() { ut_ad(m_impl == 0); }

  /** Write the redo log record, add dirty pages to the flush list and
  release the resources. */
  void execute();

  /** Add blocks modified in this mini-transaction to the flush list. */
  void add_dirty_blocks_to_flush_list(lsn_t start_lsn, lsn_t end_lsn);

  /** Release both the latches and blocks used in the mini-transaction. */
  void release_all();

  /** Release the resources */
  void release_resources();

 private:
#ifndef UNIV_HOTBACKUP
  /** Prepare to write the mini-transaction log to the redo log buffer.
  @return number of bytes to write in finish_write() */
  ulint prepare_write();
#endif /* !UNIV_HOTBACKUP */

  /** true if it is a sync mini-transaction. */
  bool m_sync;

  /** The mini-transaction state. */
  mtr_t::Impl *m_impl;

  /** Set to 1 after the user thread releases the latches. The log
  writer thread must wait for this to be set to 1. */
  volatile ulint m_locks_released;
};

/** Check if a mini-transaction is dirtying a clean page.
@return true if the mtr is dirtying a clean page. */
bool mtr_t::is_block_dirtied(const buf_block_t *block) {
  ut_ad(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);
  ut_ad(block->page.buf_fix_count > 0);

  /* It is OK to read oldest_modification because no
  other thread can be performing a write of it and it
  is only during write that the value is reset to 0. */
  return (block->page.oldest_modification == 0);
}

#ifndef UNIV_HOTBACKUP
/** Write the block contents to the REDO log */
struct mtr_write_log_t {
  /** Append a block to the redo log buffer.
  @return whether the appending should continue */
  bool operator()(const mtr_buf_t::block_t *block) {
    lsn_t start_lsn;
    lsn_t end_lsn;

    ut_ad(block != nullptr);

    if (block->used() == 0) {
      return (true);
    }

    start_lsn = m_lsn;

    end_lsn = log_buffer_write(*log_sys, m_handle, block->begin(),
                               block->used(), start_lsn);

    ut_a(end_lsn % OS_FILE_LOG_BLOCK_SIZE <
         OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE);

    m_left_to_write -= block->used();

    if (m_left_to_write == 0
        /* This write was up to the end of record group,
        the last record in group has been written.

        Therefore next group of records starts at m_lsn.
        We need to find out, if the next group is the first group,
        that starts in this log block.

        In such case we need to set first_rec_group.

        Now, we could have two cases:
        1. This group of log records has started in previous block
           to block containing m_lsn.
        2. This group of log records has started in the same block
           as block containing m_lsn.

        Only in case 1), the next group of records is the first group
        of log records in block containing m_lsn. */
        && m_handle.start_lsn / OS_FILE_LOG_BLOCK_SIZE !=
               end_lsn / OS_FILE_LOG_BLOCK_SIZE) {
      log_buffer_set_first_record_group(*log_sys, m_handle, end_lsn);
    }

    log_buffer_write_completed(*log_sys, m_handle, start_lsn, end_lsn);

    m_lsn = end_lsn;

    return (true);
  }

  Log_handle m_handle;
  lsn_t m_lsn;
  ulint m_left_to_write;
};
#endif /* !UNIV_HOTBACKUP */

/** Start a mini-transaction.
@param sync		true if it is a synchronous mini-transaction
@param read_only	true if read only mini-transaction */
void mtr_t::start(bool sync, bool read_only) {
  UNIV_MEM_INVALID(this, sizeof(*this));

  UNIV_MEM_INVALID(&m_impl, sizeof(m_impl));

  m_sync = sync;

  m_commit_lsn = 0;

  new (&m_impl.m_log) mtr_buf_t();
  new (&m_impl.m_memo) mtr_buf_t();

  m_impl.m_mtr = this;
  m_impl.m_log_mode = MTR_LOG_ALL;
  m_impl.m_inside_ibuf = false;
  m_impl.m_modifications = false;
  m_impl.m_made_dirty = false;
  m_impl.m_n_log_recs = 0;
  m_impl.m_state = MTR_STATE_ACTIVE;
  m_impl.m_flush_observer = NULL;

  ut_d(m_impl.m_magic_n = MTR_MAGIC_N);
#if defined (UNIV_PMEMOBJ_PART_PL)
  /*maximize number of REDO recs a mtr's buffer has */
  //ulint MAX_RECS = 512; //MySQL 5.7
  ulint MAX_RECS = 1024; //MySQL 8.0

  m_impl.key_arr = (uint64_t*) calloc(MAX_RECS, sizeof(uint64_t));
  m_impl.LSN_arr = (uint64_t*) calloc(MAX_RECS, sizeof(uint64_t));
  m_impl.off_arr = (uint16_t*) calloc(MAX_RECS, sizeof(uint16_t));
  m_impl.len_off_arr = (uint16_t*) calloc(MAX_RECS, sizeof(uint16_t));
#if defined (UNIV_PMEMOBJ_VALID_MTR)	
  m_impl.space_arr = (uint64_t*) calloc(MAX_RECS, sizeof(uint64_t));
  m_impl.page_arr = (uint64_t*) calloc(MAX_RECS, sizeof(uint64_t));
  m_impl.size_arr = (uint64_t*) calloc(MAX_RECS, sizeof(uint64_t));
  m_impl.type_arr = (uint16_t*) calloc(MAX_RECS, sizeof(uint16_t));
#endif //UNIV_PMEMOBJ_VALID_MTR

  //ulint max_init_size = 16384;	
  //ulint max_init_size = 8192;	//debug OK
  //ulint max_init_size = 4096;	
  
  /*Note for Linkbench
   * Linkbench use large data record that leads to large REDO log record.
   * The heap buffer of mini-transaction will be filled up quickly.
   * Thus we should init a large buffer at the first time to avoid realloc
   * */
  //ulint max_init_size = 2048;	// for Linkbench
  //ulint max_init_size = 1024;	// for Linkbench
  ulint max_init_size = 512;	// original value

  m_impl.buf = (byte*) calloc(max_init_size, sizeof(byte));
  m_impl.cur_off = 0;
  m_impl.max_buf_size = max_init_size;
#endif //UNIV_PMEMOBJ_PART_PL
}

/** Release the resources */
void mtr_t::Command::release_resources() {
  ut_ad(m_impl->m_magic_n == MTR_MAGIC_N);

  /* Currently only used in commit */
  ut_ad(m_impl->m_state == MTR_STATE_COMMITTING);

#ifdef UNIV_DEBUG
  Debug_check release;
  Iterate<Debug_check> iterator(release);

  m_impl->m_memo.for_each_block_in_reverse(iterator);
#endif /* UNIV_DEBUG */

  /* Reset the mtr buffers */
  m_impl->m_log.erase();

  m_impl->m_memo.erase();

#if defined (UNIV_PMEMOBJ_PART_PL)
	free(m_impl->key_arr);
	free(m_impl->LSN_arr);
	free(m_impl->off_arr);
	free(m_impl->len_off_arr);
	free(m_impl->buf);
#if defined (UNIV_PMEMOBJ_VALID_MTR)	
	free(m_impl->space_arr);
	free(m_impl->page_arr);
	free(m_impl->size_arr);
	free(m_impl->type_arr);
#endif //UNIV_PMEMOBJ_VALID_MTR
#endif // UNIV_PMEMOBJ_PART_PL

  m_impl->m_state = MTR_STATE_COMMITTED;

  m_impl = 0;
}

/** Commit a mini-transaction. */
void mtr_t::commit() {
  ut_ad(is_active());
  ut_ad(!is_inside_ibuf());
  ut_ad(m_impl.m_magic_n == MTR_MAGIC_N);
  m_impl.m_state = MTR_STATE_COMMITTING;

  Command cmd(this);

  if (m_impl.m_n_log_recs > 0 ||
      (m_impl.m_modifications && m_impl.m_log_mode == MTR_LOG_NO_REDO)) {
    ut_ad(!srv_read_only_mode || m_impl.m_log_mode == MTR_LOG_NO_REDO);

    cmd.execute();
  } else {
    cmd.release_all();
    cmd.release_resources();
  }
}

#ifndef UNIV_HOTBACKUP
/** Acquire a tablespace X-latch.
@param[in]	space		tablespace instance
@param[in]	file		file name from where called
@param[in]	line		line number in file */
void mtr_t::x_lock_space(fil_space_t *space, const char *file, ulint line) {
  ut_ad(m_impl.m_magic_n == MTR_MAGIC_N);
  ut_ad(is_active());

  x_lock(&space->latch, file, line);
}

/** Release an object in the memo stack. */
void mtr_t::memo_release(const void *object, ulint type) {
  ut_ad(m_impl.m_magic_n == MTR_MAGIC_N);
  ut_ad(is_active());

  /* We cannot release a page that has been written to in the
  middle of a mini-transaction. */
  ut_ad(!m_impl.m_modifications || type != MTR_MEMO_PAGE_X_FIX);

  Find find(object, type);
  Iterate<Find> iterator(find);

  if (!m_impl.m_memo.for_each_block_in_reverse(iterator)) {
    memo_slot_release(find.m_slot);
  }
}

/** Release a page latch.
@param[in]	ptr	pointer to within a page frame
@param[in]	type	object type: MTR_MEMO_PAGE_X_FIX, ... */
void mtr_t::release_page(const void *ptr, mtr_memo_type_t type) {
  ut_ad(m_impl.m_magic_n == MTR_MAGIC_N);
  ut_ad(is_active());

  /* We cannot release a page that has been written to in the
  middle of a mini-transaction. */
  ut_ad(!m_impl.m_modifications || type != MTR_MEMO_PAGE_X_FIX);

  Find_page find(ptr, type);
  Iterate<Find_page> iterator(find);

  if (!m_impl.m_memo.for_each_block_in_reverse(iterator)) {
    memo_slot_release(find.get_slot());
    return;
  }

  /* The page was not found! */
  ut_ad(0);
}

/** Prepare to write the mini-transaction log to the redo log buffer.
@return number of bytes to write in finish_write() */
ulint mtr_t::Command::prepare_write() {
  switch (m_impl->m_log_mode) {
    case MTR_LOG_SHORT_INSERTS:
      ut_ad(0);
      /* fall through (write no redo log) */
    case MTR_LOG_NO_REDO:
    case MTR_LOG_NONE:
      ut_ad(m_impl->m_log.size() == 0);
      return (0);
    case MTR_LOG_ALL:
      break;
  }

  /* An ibuf merge could happen when loading page to apply log
  records during recovery. During the ibuf merge mtr is used. */

  ut_a(!recv_recovery_is_on() || !recv_no_ibuf_operations);

  ulint len = m_impl->m_log.size();
  ut_ad(len > 0);

  ulint n_recs = m_impl->m_n_log_recs;
  ut_ad(n_recs > 0);

  ut_ad(log_sys != nullptr);

  ut_ad(m_impl->m_n_log_recs == n_recs);

  /* This was not the first time of dirtying a
  tablespace since the latest checkpoint. */

  ut_ad(n_recs == m_impl->m_n_log_recs);

  if (n_recs <= 1) {
    ut_ad(n_recs == 1);

    /* Flag the single log record as the
    only record in this mini-transaction. */

    *m_impl->m_log.front()->begin() |= MLOG_SINGLE_REC_FLAG;

  } else {
    /* Because this mini-transaction comprises
    multiple log records, append MLOG_MULTI_REC_END
    at the end. */

    mlog_catenate_ulint(&m_impl->m_log, MLOG_MULTI_REC_END, MLOG_1BYTE);
    ++len;
  }

  ut_ad(m_impl->m_log_mode == MTR_LOG_ALL);
  ut_ad(m_impl->m_log.size() == len);
  ut_ad(len > 0);

  return (len);
}
#endif /* !UNIV_HOTBACKUP */

/** Release the latches and blocks acquired by this mini-transaction */
void mtr_t::Command::release_all() {
  Release_all release;
  Iterate<Release_all> iterator(release);

  m_impl->m_memo.for_each_block_in_reverse(iterator);

  /* Note that we have released the latches. */
  m_locks_released = 1;
}

/** Add blocks modified in this mini-transaction to the flush list. */
void mtr_t::Command::add_dirty_blocks_to_flush_list(lsn_t start_lsn,
                                                    lsn_t end_lsn) {
  Add_dirty_blocks_to_flush_list add_to_flush(start_lsn, end_lsn,
                                              m_impl->m_flush_observer);

  Iterate<Add_dirty_blocks_to_flush_list> iterator(add_to_flush);

  m_impl->m_memo.for_each_block_in_reverse(iterator);
}

#if defined (UNIV_PMEMOBJ_PART_PL) || defined (UNIV_SKIPLOG)


#if defined (UNIV_PMEMOBJ_VALID_MTR)	
/*This function used in debugging PL-NVM
 *
 * */
void
mtr_t::pmem_check_mtrlog(mtr_t* mtr)
{
	ulint n_recs;
	ulint len;
	ulint i;

	mlog_id_t type;
	mlog_id_t check_type;

	byte* begin_ptr;
	byte* ptr;
	byte* temp_ptr;
	byte* end_ptr;

	ulint parsed_len;
	ulint check_len;

	ulint parsed_lsn;
	ulint check_lsn;

	ulint n_parsed;
	//ulint space_no, page_no;
	uint32_t space_no, page_no;
	byte* body;

	ulint check_space, check_page;

	n_recs	= m_impl.m_n_log_recs;

	n_parsed = 0;
	i = 0;	

	ptr = mtr->get_buf();
	end_ptr = mtr->open_buf(0);	

	while (ptr < end_ptr){
		if (*ptr == MLOG_MULTI_REC_END){
			ptr++;
			continue;
		}

		assert(i < n_recs);

		check_type = (mlog_id_t) m_impl.type_arr[i];
		check_space = m_impl.space_arr[i];
		check_page = m_impl.page_arr[i]; 
		check_len = m_impl.size_arr[i];

		temp_ptr = mlog_parse_initial_log_record(ptr, end_ptr, &type, &space_no, &page_no);

		if (check_type != type ||
			check_space != space_no ||
			check_page != page_no ||
			type >= MLOG_BIGGEST_TYPE
			){

			printf("ERROR: parsed type %zu space %zu page %zu are differ to CHECK type %zu space %zu page %zu\n", type, space_no, page_no, check_type, check_space, check_page);
			assert(0);

		}
		//now check rec_len field
		parsed_len = mach_read_from_2(temp_ptr);
		
		if (parsed_len != check_len){
			printf("ERROR: parsed len %zu differ to check len %zu\n ", parsed_len, check_len);
			assert(0);
		}
		temp_ptr += 2;
		//skip the rec_lsn
		temp_ptr += 8;
		
		if ( (temp_ptr - ptr) == parsed_len){
			/*empty body rec*/
			if (type != MLOG_INIT_FILE_PAGE2
					&& type != MLOG_COMP_PAGE_CREATE 
					&& type != MLOG_IBUF_BITMAP_INIT
					&& type != MLOG_UNDO_ERASE_END){
				printf("pm_check_mtrlog() ERROR: empty body rec has type %zu is not valid!\n", type);
				assert(0);
			}
		}

		//check for MLOG_COMP_LIST_END_COPY_CREATED (type == 45) 
		if (type == 45){
			//read the log_data_len to check whether it != 0
			// parse 2 + 2 + (n * 2) bytes
			dict_index_t*   index = NULL;
			byte* temp_ptr2 = mlog_parse_index(temp_ptr, end_ptr, 1, &index);
			ulint log_data_len = mach_read_from_4(temp_ptr2);
			//if (log_data_len == 0){
			//	printf("mtr::exec ERROR log_data_len is ZERO mtr %zu log_ptr %zu type %zu space %zu page %zu\n", mtr, temp_ptr2, type, space_no, page_no);
			//	assert(log_data_len);
			//}
			//else{
			//	printf("mtr::exec OK log_data_len  %zu mtr %zu log_ptr %zu type %zu space %zu page %zu\n", log_data_len, mtr, temp_ptr2, type, space_no, page_no);

			//}

			temp_ptr2 += 4;

			if (log_data_len + (temp_ptr2 - ptr) != check_len){
				printf("mtr::exec ERROR  mtr %zu log_ptr %zu  type 45 error, the_first %zu + log_data_len %zu differ to check_len %zu\n", mtr, (temp_ptr2 - 4), (temp_ptr2-ptr), log_data_len, check_len);
				assert(0);
			}
			else{
				//printf("mtr::exec OK mtr %zu type %zu space %zu page %zu the first %zu log_data_len %zu \n", mtr, type, space_no, page_no, (temp_ptr2-ptr), log_data_len);
			}
		}
		ptr += parsed_len;
		i++;
	}//end while
	
}
#endif // UNIV_PMEMOBJ_VALID_MTR
#endif // UNIV_PMEMOBJ_PART_PL || UNIV_SKIPLOG

#if defined (UNIV_PMEMOBJ_PART_PL)
/*
 * Directly add log rec to PPL
 * @param[in]: key - the fold value of space and page_no
 * @param[in]: src - log src
 * @param[in]: size - log rec size
 * */
uint64_t mtr_t::add_rec_to_ppl(
		uint64_t	key,
	   	byte*		log_src,
	   	uint32_t	rec_size)
{
	return pm_ppl_write_rec(gb_pmw->pop, gb_pmw, gb_pmw->ppl, key, log_src, rec_size);
}
#endif // UNIV_PMEMOBJ_PART_PL

#if defined (UNIV_PMEMOBJ_PART_PL)

/*Case 1: PL-NVM*/

void mtr_t::Command::execute() {
  ut_ad(m_impl->m_log_mode != MTR_LOG_NONE);

  ulint len;
  fil_space_t*	space;

  byte* begin_ptr;

  uint16_t	n_recs;
  uint16_t	prev_off;
  uint16_t	prev_len_off;
  uint16_t	rec_size;
  uint64_t	prev_key;

  lsn_t start_lsn;
  lsn_t end_lsn;

  mtr_t*			mtr;

  mtr = m_impl->m_mtr;
  begin_ptr = mtr->get_buf();

  len	= mtr->get_cur_off();
  n_recs	= m_impl->m_n_log_recs;

  /////////////////////////////////////////////////
  // begin simulate Command::prepare_write()
  /////////////////////////////////////////////////
  /*simulate the lsn 
   * start lsn is the smallest lsn in the LSN_arr
   * end_lsn is the largest lsn in the LSN_arr
   * */

  switch (m_impl->m_log_mode) {
    case MTR_LOG_SHORT_INSERTS:
      ut_ad(0);
      /* fall through (write no redo log) */
    case MTR_LOG_NO_REDO:
    case MTR_LOG_NONE:
      ut_ad(m_impl->m_log.size() == 0);
      len =  0;
    case MTR_LOG_ALL:
      break;
  }
  /*We don't append either MLOG_SINGLE_REC_FLAG or MLOG_MULTI_REC_END to m_impl_m_log*/

  /*end simulated Commad::prepare_write()*/

  if (len > 0) {
	  /*write REDO logs from mtr to partition logs in NVM*/
	//(2) Compute "rec_len" for the last log rec 
	prev_off = mtr->get_off_at(n_recs - 1);

	rec_size = len - prev_off ;

	assert(rec_size > 0);

	prev_len_off = mtr->get_len_off_at(n_recs - 1); 

	mach_write_to_2(begin_ptr + prev_len_off, rec_size);

#if defined (UNIV_PMEMOBJ_VALID_MTR)	
	mtr->add_size_at(rec_size, n_recs - 1);
#endif	

	prev_key = mtr->get_key_at(n_recs - 1);	
	
	/*single-entry call to add REDO logs to PL-NVM*/
	end_lsn = mtr->add_rec_to_ppl(prev_key, begin_ptr + prev_off, rec_size);

	mtr->add_LSN_at(end_lsn, n_recs - 1);
	
	start_lsn = mtr->get_LSN_at(0);

	/* TODO:
	if (len > 0){
		if (was_clean){
			space->max_lsn = m_end_lsn;
		}
	}
	*/
#if defined (UNIV_PMEMOBJ_VALID_MTR)	
	//(3) Check, remove this section in run mode
	mtr->pmem_check_mtrlog(mtr);
#endif		

	/*update pageLSN and add dirty pages to flush list
	 * this call replace release_blocks() in MySQL 5.7
	 */
    add_dirty_blocks_to_flush_list(start_lsn, end_lsn);

	m_impl->m_mtr->m_commit_lsn = end_lsn;

  } else {
    DEBUG_SYNC_C("mtr_noredo_before_add_dirty_blocks");

    add_dirty_blocks_to_flush_list(0, 0);
  }
	
  /*release resources*/
  release_all();
  release_resources();
}
#elif defined (UNIV_SKIPLOG)
/* Skip writing log records from mtr's heap to log buffer
 * Release mtr's in-memory resources
 * */
void mtr_t::Command::execute() {
	ut_ad(m_impl->m_log_mode != MTR_LOG_NONE);
	
	ulint len;
	len = prepare_write();

	if (len > 0) {
		/*
		 * use timestamp as the current lsn
		 * lsns are need for add_dirty_blocks_to_flush_list() work
		 * */
		Log_handle handle;
		log_t &log = *log_sys;

		handle.start_lsn = ut_time_us(NULL);
		handle.end_lsn = handle.start_lsn + len;

		add_dirty_blocks_to_flush_list(handle.start_lsn, handle.end_lsn);

		m_impl->m_mtr->m_commit_lsn = handle.end_lsn;
	} else {
		DEBUG_SYNC_C("mtr_noredo_before_add_dirty_blocks");

		add_dirty_blocks_to_flush_list(0, 0);
	}

	release_all();
	release_resources();

}
#else //original

/** Write the redo log record, add dirty pages to the flush list and release
the resources. */
void mtr_t::Command::execute() {
  ut_ad(m_impl->m_log_mode != MTR_LOG_NONE);

  ulint len;

#ifndef UNIV_HOTBACKUP
  len = prepare_write();

  if (len > 0) {
    mtr_write_log_t write_log;

    write_log.m_left_to_write = len;

    auto handle = log_buffer_reserve(*log_sys, len);

    write_log.m_handle = handle;
    write_log.m_lsn = handle.start_lsn;

    m_impl->m_log.for_each_block(write_log);

    ut_ad(write_log.m_left_to_write == 0);
    ut_ad(write_log.m_lsn == handle.end_lsn);

    log_wait_for_space_in_log_recent_closed(*log_sys, handle.start_lsn);

    DEBUG_SYNC_C("mtr_redo_before_add_dirty_blocks");

    add_dirty_blocks_to_flush_list(handle.start_lsn, handle.end_lsn);

    log_buffer_close(*log_sys, handle);

    m_impl->m_mtr->m_commit_lsn = handle.end_lsn;

  } else {
    DEBUG_SYNC_C("mtr_noredo_before_add_dirty_blocks");

    add_dirty_blocks_to_flush_list(0, 0);
  }
#endif /* !UNIV_HOTBACKUP */

  release_all();
  release_resources();
}
#endif //UNIV_PMEMOBJ_PART_PL

#ifndef UNIV_HOTBACKUP
#ifdef UNIV_DEBUG
/** Check if memo contains the given item.
@return	true if contains */
bool mtr_t::memo_contains(mtr_buf_t *memo, const void *object, ulint type) {
  Find find(object, type);
  Iterate<Find> iterator(find);

  return (!memo->for_each_block_in_reverse(iterator));
}

/** Debug check for flags */
struct FlaggedCheck {
  FlaggedCheck(const void *ptr, ulint flags) : m_ptr(ptr), m_flags(flags) {
    // Do nothing
  }

  bool operator()(const mtr_memo_slot_t *slot) const {
    if (m_ptr == slot->object && (m_flags & slot->type)) {
      return (false);
    }

    return (true);
  }

  const void *m_ptr;
  ulint m_flags;
};

/** Check if memo contains the given item.
@param ptr		object to search
@param flags		specify types of object (can be ORred) of
                        MTR_MEMO_PAGE_S_FIX ... values
@return true if contains */
bool mtr_t::memo_contains_flagged(const void *ptr, ulint flags) const {
  ut_ad(m_impl.m_magic_n == MTR_MAGIC_N);
  ut_ad(is_committing() || is_active());

  FlaggedCheck check(ptr, flags);
  Iterate<FlaggedCheck> iterator(check);

  return (!m_impl.m_memo.for_each_block_in_reverse(iterator));
}

/** Check if memo contains the given page.
@param[in]	ptr	pointer to within buffer frame
@param[in]	flags	specify types of object with OR of
                        MTR_MEMO_PAGE_S_FIX... values
@return	the block
@retval	NULL	if not found */
buf_block_t *mtr_t::memo_contains_page_flagged(const byte *ptr,
                                               ulint flags) const {
  Find_page check(ptr, flags);
  Iterate<Find_page> iterator(check);

  return (m_impl.m_memo.for_each_block_in_reverse(iterator)
              ? NULL
              : check.get_block());
}

/** Mark the given latched page as modified.
@param[in]	ptr	pointer to within buffer frame */
void mtr_t::memo_modify_page(const byte *ptr) {
  buf_block_t *block = memo_contains_page_flagged(
      ptr, MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX);
  ut_ad(block != NULL);

  if (!memo_contains(get_memo(), block, MTR_MEMO_MODIFY)) {
    memo_push(block, MTR_MEMO_MODIFY);
  }
}

/** Print info of an mtr handle. */
void mtr_t::print() const {
  ib::info(ER_IB_MSG_1275) << "Mini-transaction handle: memo size "
                           << m_impl.m_memo.size() << " bytes log size "
                           << get_log()->size() << " bytes";
}

#endif /* UNIV_DEBUG */
#endif /* !UNIV_HOTBACKUP */
