/*****************************************************************************

Copyright (c) 2005, 2012, Oracle and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file handler/handler0alter.cc
Smart ALTER TABLE
*******************************************************/

#include <unireg.h>
#include <mysqld_error.h>
#include <sql_class.h>
#include <sql_lex.h>                            // SQLCOM_CREATE_INDEX
#include <mysql/innodb_priv.h>
#include <sql_table.h>							// check_geometry_equal_special

extern "C" {

#include "log0log.h"
#include "row0merge.h"
#include "srv0srv.h"
#include "trx0trx.h"
#include "trx0roll.h"
#include "ha_prototypes.h"
#include "handler0alter.h"

#include "pars0pars.h"
#include "que0que.h"

#include "dict0mem.h"

}

#include "ha_innodb.h"


/*************************************************************//**
Copies an InnoDB column to a MySQL field.  This function is
adapted from row_sel_field_store_in_mysql_format(). */
static
void
innobase_col_to_mysql(
/*==================*/
	const dict_col_t*	col,	/*!< in: InnoDB column */
	const uchar*		data,	/*!< in: InnoDB column data */
	ulint			len,	/*!< in: length of data, in bytes */
	Field*			field)	/*!< in/out: MySQL field */
{
	uchar*	ptr;
	uchar*	dest	= field->ptr;
	ulint	flen	= field->pack_length();

	switch (col->mtype) {
	case DATA_INT:
		ut_ad(len == flen);

		/* Convert integer data from Innobase to little-endian
		format, sign bit restored to normal */

		for (ptr = dest + len; ptr != dest; ) {
			*--ptr = *data++;
		}

		if (!(field->flags & UNSIGNED_FLAG)) {
			((byte*) dest)[len - 1] ^= 0x80;
		}

		break;

	case DATA_VARCHAR:
	case DATA_VARMYSQL:
	case DATA_BINARY:
		field->reset();

		if (field->type() == MYSQL_TYPE_VARCHAR) {
			/* This is a >= 5.0.3 type true VARCHAR. Store the
			length of the data to the first byte or the first
			two bytes of dest. */

			dest = row_mysql_store_true_var_len(
				dest, len, flen - field->key_length());
		}

		/* Copy the actual data */
		memcpy(dest, data, len);
		break;

	case DATA_BLOB:
		/* Store a pointer to the BLOB buffer to dest: the BLOB was
		already copied to the buffer in row_sel_store_mysql_rec */

		row_mysql_store_blob_ref(dest, flen, data, len);
		break;

#ifdef UNIV_DEBUG
	case DATA_MYSQL:
		ut_ad(flen >= len);
		ut_ad(DATA_MBMAXLEN(col->mbminmaxlen)
		      >= DATA_MBMINLEN(col->mbminmaxlen));
		ut_ad(DATA_MBMAXLEN(col->mbminmaxlen)
		      > DATA_MBMINLEN(col->mbminmaxlen) || flen == len);
		memcpy(dest, data, len);
		break;

	default:
	case DATA_SYS_CHILD:
	case DATA_SYS:
		/* These column types should never be shipped to MySQL. */
		ut_ad(0);

	case DATA_CHAR:
	case DATA_FIXBINARY:
	case DATA_FLOAT:
	case DATA_DOUBLE:
	case DATA_DECIMAL:
		/* Above are the valid column types for MySQL data. */
		ut_ad(flen == len);
#else /* UNIV_DEBUG */
	default:
#endif /* UNIV_DEBUG */
		memcpy(dest, data, len);
	}
}

/*************************************************************//**
Copies an InnoDB record to table->record[0]. */
extern "C" UNIV_INTERN
void
innobase_rec_to_mysql(
/*==================*/
	TABLE*			table,		/*!< in/out: MySQL table */
	const rec_t*		rec,		/*!< in: record */
	const dict_index_t*	index,		/*!< in: index */
	const ulint*		offsets)	/*!< in: rec_get_offsets(
						rec, index, ...) */
{
	uint	n_fields	= table->s->fields;
	uint	i;

	ut_ad(n_fields == dict_table_get_n_user_cols(index->table));

	for (i = 0; i < n_fields; i++) {
		Field*		field	= table->field[i];
		ulint		ipos;
		ulint		ilen;
		const uchar*	ifield;

		field->reset();

		ipos = dict_index_get_nth_col_pos(index, i);

		if (UNIV_UNLIKELY(ipos == ULINT_UNDEFINED)) {
null_field:
			field->set_null();
			continue;
		}

		ifield = rec_get_nth_field(rec, offsets, ipos, &ilen);

		/* Assign the NULL flag */
		if (ilen == UNIV_SQL_NULL) {
			ut_ad(field->real_maybe_null());
			goto null_field;
		}

        if (ilen == UNIV_SQL_DEFAULT)
            ifield = dict_index_get_nth_col_def(index, ipos, &ilen);

		field->set_notnull();

		innobase_col_to_mysql(
			dict_field_get_col(
				dict_index_get_nth_field(index, ipos)),
			ifield, ilen, field);
	}
}

/*************************************************************//**
Resets table->record[0]. */
extern "C" UNIV_INTERN
void
innobase_rec_reset(
/*===============*/
	TABLE*			table)		/*!< in/out: MySQL table */
{
	uint	n_fields	= table->s->fields;
	uint	i;

	for (i = 0; i < n_fields; i++) {
		table->field[i]->set_default();
	}
}

/******************************************************************//**
Removes the filename encoding of a database and table name. */
static
void
innobase_convert_tablename(
/*=======================*/
	char*	s)	/*!< in: identifier; out: decoded identifier */
{
	uint	errors;

	char*	slash = strchr(s, '/');

	if (slash) {
		char*	t;
		/* Temporarily replace the '/' with NUL. */
		*slash = 0;
		/* Convert the database name. */
		strconvert(&my_charset_filename, s, system_charset_info,
			   s, slash - s + 1, &errors);

		t = s + strlen(s);
		ut_ad(slash >= t);
		/* Append a  '.' after the database name. */
		*t++ = '.';
		slash++;
		/* Convert the table name. */
		strconvert(&my_charset_filename, slash, system_charset_info,
			   t, slash - t + strlen(slash), &errors);
	} else {
		strconvert(&my_charset_filename, s,
			   system_charset_info, s, strlen(s), &errors);
	}
}

/*******************************************************************//**
This function checks that index keys are sensible.
@return	0 or error number */
static
int
innobase_check_index_keys(
/*======================*/
	const KEY*		key_info,	/*!< in: Indexes to be
						created */
	ulint			num_of_keys,	/*!< in: Number of
						indexes to be created */
	const dict_table_t*	table)		/*!< in: Existing indexes */
{
	ulint		key_num;

	ut_ad(key_info);
	ut_ad(num_of_keys);

	for (key_num = 0; key_num < num_of_keys; key_num++) {
		const KEY&	key = key_info[key_num];

		/* Check that the same index name does not appear
		twice in indexes to be created. */

		for (ulint i = 0; i < key_num; i++) {
			const KEY&	key2 = key_info[i];

			if (0 == strcmp(key.name, key2.name)) {
				my_error(ER_WRONG_NAME_FOR_INDEX, MYF(0),
					 key.name);

				return(ER_WRONG_NAME_FOR_INDEX);
			}
		}

		/* Check that the same index name does not already exist. */

		for (const dict_index_t* index
			     = dict_table_get_first_index(table);
		     index; index = dict_table_get_next_index(index)) {

			if (0 == strcmp(key.name, index->name)) {
				my_error(ER_WRONG_NAME_FOR_INDEX, MYF(0),
					 key.name);

				return(ER_WRONG_NAME_FOR_INDEX);
			}
		}

		/* Check that MySQL does not try to create a column
		prefix index field on an inappropriate data type and
		that the same column does not appear twice in the index. */

		for (ulint i = 0; i < key.key_parts; i++) {
			const KEY_PART_INFO&	key_part1
				= key.key_part[i];
			const Field*		field
				= key_part1.field;
			ibool			is_unsigned;

			switch (get_innobase_type_from_mysql_type(
					&is_unsigned, field)) {
			default:
				break;
			case DATA_INT:
			case DATA_FLOAT:
			case DATA_DOUBLE:
			case DATA_DECIMAL:
				if (field->type() == MYSQL_TYPE_VARCHAR) {
					if (key_part1.length
					    >= field->pack_length()
					    - ((Field_varstring*) field)
					    ->length_bytes) {
						break;
					}
				} else {
					if (key_part1.length
					    >= field->pack_length()) {
						break;
					}
				}

				my_error(ER_WRONG_KEY_COLUMN, MYF(0),
					 field->field_name);
				return(ER_WRONG_KEY_COLUMN);
			}

			for (ulint j = 0; j < i; j++) {
				const KEY_PART_INFO&	key_part2
					= key.key_part[j];

				if (strcmp(key_part1.field->field_name,
					   key_part2.field->field_name)) {
					continue;
				}

				my_error(ER_WRONG_KEY_COLUMN, MYF(0),
					 key_part1.field->field_name);
				return(ER_WRONG_KEY_COLUMN);
			}
		}
	}

	return(0);
}

/*******************************************************************//**
Create index field definition for key part */
static
void
innobase_create_index_field_def(
/*============================*/
	KEY_PART_INFO*		key_part,	/*!< in: MySQL key definition */
	mem_heap_t*		heap,		/*!< in: memory heap */
	merge_index_field_t*	index_field)	/*!< out: index field
						definition for key_part */
{
	Field*		field;
	ibool		is_unsigned;
	ulint		col_type;

	DBUG_ENTER("innobase_create_index_field_def");

	ut_ad(key_part);
	ut_ad(index_field);

	field = key_part->field;
	ut_a(field);

	col_type = get_innobase_type_from_mysql_type(&is_unsigned, field);

	if (DATA_BLOB == col_type
	    || (key_part->length < field->pack_length()
		&& field->type() != MYSQL_TYPE_VARCHAR)
	    || (field->type() == MYSQL_TYPE_VARCHAR
		&& key_part->length < field->pack_length()
			- ((Field_varstring*)field)->length_bytes)) {

		index_field->prefix_len = key_part->length;
	} else {
		index_field->prefix_len = 0;
	}

	index_field->field_name = mem_heap_strdup(heap, field->field_name);

	DBUG_VOID_RETURN;
}

/*******************************************************************//**
Create index definition for key */
static
void
innobase_create_index_def(
/*======================*/
	KEY*			key,		/*!< in: key definition */
	bool			new_primary,	/*!< in: TRUE=generating
						a new primary key
						on the table */
	bool			key_primary,	/*!< in: TRUE if this key
						is a primary key */
	merge_index_def_t*	index,		/*!< out: index definition */
	mem_heap_t*		heap)		/*!< in: heap where memory
						is allocated */
{
	ulint	i;
	ulint	len;
	ulint	n_fields = key->key_parts;
	char*	index_name;

	DBUG_ENTER("innobase_create_index_def");

	index->fields = (merge_index_field_t*) mem_heap_alloc(
		heap, n_fields * sizeof *index->fields);

	index->ind_type = 0;
	index->n_fields = n_fields;
	len = strlen(key->name) + 1;
	index->name = index_name = (char*) mem_heap_alloc(heap,
							  len + !new_primary);

	if (UNIV_LIKELY(!new_primary)) {
		*index_name++ = TEMP_INDEX_PREFIX;
	}

	memcpy(index_name, key->name, len);

	if (key->flags & HA_NOSAME) {
		index->ind_type |= DICT_UNIQUE;
	}

	if (key_primary) {
		index->ind_type |= DICT_CLUSTERED;
	}

	for (i = 0; i < n_fields; i++) {
		innobase_create_index_field_def(&key->key_part[i], heap,
						&index->fields[i]);
	}

	DBUG_VOID_RETURN;
}

/*******************************************************************//**
Copy index field definition */
static
void
innobase_copy_index_field_def(
/*==========================*/
	const dict_field_t*	field,		/*!< in: definition to copy */
	merge_index_field_t*	index_field)	/*!< out: copied definition */
{
	DBUG_ENTER("innobase_copy_index_field_def");
	DBUG_ASSERT(field != NULL);
	DBUG_ASSERT(index_field != NULL);

	index_field->field_name = field->name;
	index_field->prefix_len = field->prefix_len;

	DBUG_VOID_RETURN;
}

/*******************************************************************//**
Copy index definition for the index */
static
void
innobase_copy_index_def(
/*====================*/
	const dict_index_t*	index,	/*!< in: index definition to copy */
	merge_index_def_t*	new_index,/*!< out: Index definition */
	mem_heap_t*		heap)	/*!< in: heap where allocated */
{
	ulint	n_fields;
	ulint	i;

	DBUG_ENTER("innobase_copy_index_def");

	/* Note that we take only those fields that user defined to be
	in the index.  In the internal representation more colums were
	added and those colums are not copied .*/

	n_fields = index->n_user_defined_cols;

	new_index->fields = (merge_index_field_t*) mem_heap_alloc(
		heap, n_fields * sizeof *new_index->fields);

	/* When adding a PRIMARY KEY, we may convert a previous
	clustered index to a secondary index (UNIQUE NOT NULL). */
	new_index->ind_type = index->type & ~DICT_CLUSTERED;
	new_index->n_fields = n_fields;
	new_index->name = index->name;

	for (i = 0; i < n_fields; i++) {
		innobase_copy_index_field_def(&index->fields[i],
					      &new_index->fields[i]);
	}

	DBUG_VOID_RETURN;
}

/*******************************************************************//**
Create an index table where indexes are ordered as follows:

IF a new primary key is defined for the table THEN

	1) New primary key
	2) Original secondary indexes
	3) New secondary indexes

ELSE

	1) All new indexes in the order they arrive from MySQL

ENDIF


@return	key definitions or NULL */
static
merge_index_def_t*
innobase_create_key_def(
/*====================*/
	trx_t*		trx,		/*!< in: trx */
	const dict_table_t*table,		/*!< in: table definition */
	mem_heap_t*	heap,		/*!< in: heap where space for key
					definitions are allocated */
	KEY*		key_info,	/*!< in: Indexes to be created */
	ulint&		n_keys)		/*!< in/out: Number of indexes to
					be created */
{
	ulint			i = 0;
	merge_index_def_t*	indexdef;
	merge_index_def_t*	indexdefs;
	bool			new_primary;

	DBUG_ENTER("innobase_create_key_def");

	indexdef = indexdefs = (merge_index_def_t*)
		mem_heap_alloc(heap, sizeof *indexdef
			       * (n_keys + UT_LIST_GET_LEN(table->indexes)));

	/* If there is a primary key, it is always the first index
	defined for the table. */

	new_primary = !my_strcasecmp(system_charset_info,
				     key_info->name, "PRIMARY");

	/* If there is a UNIQUE INDEX consisting entirely of NOT NULL
	columns and if the index does not contain column prefix(es)
	(only prefix/part of the column is indexed), MySQL will treat the
	index as a PRIMARY KEY unless the table already has one. */

	if (!new_primary && (key_info->flags & HA_NOSAME)
	    && (!(key_info->flags & HA_KEY_HAS_PART_KEY_SEG))
	    && row_table_got_default_clust_index(table)) {
		uint	key_part = key_info->key_parts;

		new_primary = TRUE;

		while (key_part--) {
			if (key_info->key_part[key_part].key_type
			    & FIELDFLAG_MAYBE_NULL) {
				new_primary = FALSE;
				break;
			}
		}
	}

	if (new_primary) {
		const dict_index_t*	index;

		/* Create the PRIMARY key index definition */
		innobase_create_index_def(&key_info[i++], TRUE, TRUE,
					  indexdef++, heap);

		row_mysql_lock_data_dictionary(trx);

		index = dict_table_get_first_index(table);

		/* Copy the index definitions of the old table.  Skip
		the old clustered index if it is a generated clustered
		index or a PRIMARY KEY.  If the clustered index is a
		UNIQUE INDEX, it must be converted to a secondary index. */

		if (dict_index_get_nth_col(index, 0)->mtype == DATA_SYS
		    || !my_strcasecmp(system_charset_info,
				      index->name, "PRIMARY")) {
			index = dict_table_get_next_index(index);
		}

		while (index) {
			innobase_copy_index_def(index, indexdef++, heap);
			index = dict_table_get_next_index(index);
		}

		row_mysql_unlock_data_dictionary(trx);
	}

	/* Create definitions for added secondary indexes. */

	while (i < n_keys) {
		innobase_create_index_def(&key_info[i++], new_primary, FALSE,
					  indexdef++, heap);
	}

	n_keys = indexdef - indexdefs;

	DBUG_RETURN(indexdefs);
}

/*******************************************************************//**
Check each index column size, make sure they do not exceed the max limit
@return	HA_ERR_INDEX_COL_TOO_LONG if index column size exceeds limit */
static
int
innobase_check_column_length(
/*=========================*/
	const dict_table_t*table,	/*!< in: table definition */
	const KEY*	key_info)	/*!< in: Indexes to be created */
{
	ulint	max_col_len = DICT_MAX_FIELD_LEN_BY_FORMAT(table);

	for (ulint key_part = 0; key_part < key_info->key_parts; key_part++) {
		if (key_info->key_part[key_part].length > max_col_len) {
			my_error(ER_INDEX_COLUMN_TOO_LONG, MYF(0), max_col_len);
			return(HA_ERR_INDEX_COL_TOO_LONG);
		}
	}
	return(0);
}

/*******************************************************************//**
Create a temporary tablename using query id, thread id, and id
@return	temporary tablename */
static
char*
innobase_create_temporary_tablename(
/*================================*/
	mem_heap_t*	heap,		/*!< in: memory heap */
	char		id,		/*!< in: identifier [0-9a-zA-Z] */
	const char*     table_name)	/*!< in: table name */
{
	char*			name;
	ulint			len;
	static const char	suffix[] = "@0023 "; /* "# " */

	len = strlen(table_name);

	name = (char*) mem_heap_alloc(heap, len + sizeof suffix);
	memcpy(name, table_name, len);
	memcpy(name + len, suffix, sizeof suffix);
	name[len + (sizeof suffix - 2)] = id;

	return(name);
}

class ha_innobase_add_index : public handler_add_index
{
public:
	/** table where the indexes are being created */
	dict_table_t* indexed_table;
	ha_innobase_add_index(TABLE* table, KEY* key_info, uint num_of_keys,
			      dict_table_t* indexed_table_arg) :
		handler_add_index(table, key_info, num_of_keys),
		indexed_table (indexed_table_arg) {}
	~ha_innobase_add_index() {}
};

/*******************************************************************//**
Create indexes.
@return	0 or error number */
UNIV_INTERN
int
ha_innobase::add_index(
/*===================*/
	TABLE*			table,		/*!< in: Table where indexes
						are created */
	KEY*			key_info,	/*!< in: Indexes
						to be created */
	uint			num_of_keys,	/*!< in: Number of indexes
						to be created */
	handler_add_index**	add)		/*!< out: context */
{
	dict_index_t**	index;		/*!< Index to be created */
	dict_table_t*	indexed_table;	/*!< Table where indexes are created */
	merge_index_def_t* index_defs;	/*!< Index definitions */
	mem_heap_t*     heap;		/*!< Heap for index definitions */
	trx_t*		trx;		/*!< Transaction */
	ulint		num_of_idx;
	ulint		num_created	= 0;
	ibool		dict_locked	= FALSE;
	ulint		new_primary;
	int		error;

	DBUG_ENTER("ha_innobase::add_index");
	ut_a(table);
	ut_a(key_info);
	ut_a(num_of_keys);

	*add = NULL;

	if (srv_created_new_raw || srv_force_recovery) {
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}

	update_thd();

	/* In case MySQL calls this in the middle of a SELECT query, release
	possible adaptive hash latch to avoid deadlocks of threads. */
	trx_search_latch_release_if_reserved(prebuilt->trx);

	/* Check if the index name is reserved. */
	if (innobase_index_name_is_reserved(user_thd, key_info, num_of_keys)) {
		DBUG_RETURN(-1);
	}

	indexed_table = dict_table_get(prebuilt->table->name, FALSE);

	if (UNIV_UNLIKELY(!indexed_table)) {
		DBUG_RETURN(HA_ERR_NO_SUCH_TABLE);
	}

	ut_a(indexed_table == prebuilt->table);

	/* Check that index keys are sensible */
	error = innobase_check_index_keys(key_info, num_of_keys, prebuilt->table);

	if (UNIV_UNLIKELY(error)) {
		DBUG_RETURN(error);
	}

	/* Check each index's column length to make sure they do not
	exceed limit */
	for (ulint i = 0; i < num_of_keys; i++) {
		error = innobase_check_column_length(prebuilt->table,
						     &key_info[i]);

		if (error) {
			DBUG_RETURN(error);
		}
	}

	heap = mem_heap_create(1024);
	trx_start_if_not_started(prebuilt->trx);

	/* Create a background transaction for the operations on
	the data dictionary tables. */
	trx = innobase_trx_allocate(user_thd);
	trx_start_if_not_started(trx);

	/* Create table containing all indexes to be built in this
	alter table add index so that they are in the correct order
	in the table. */

	num_of_idx = num_of_keys;

	index_defs = innobase_create_key_def(
		trx, prebuilt->table, heap, key_info, num_of_idx);

	new_primary = DICT_CLUSTERED & index_defs[0].ind_type;

	/* Allocate memory for dictionary index definitions */

	index = (dict_index_t**) mem_heap_alloc(
		heap, num_of_idx * sizeof *index);

	/* Flag this transaction as a dictionary operation, so that
	the data dictionary will be locked in crash recovery. */
	trx_set_dict_operation(trx, TRX_DICT_OP_INDEX);

	/* Acquire a lock on the table before creating any indexes. */
	error = row_merge_lock_table(prebuilt->trx, prebuilt->table,
				     new_primary ? LOCK_X : LOCK_S);

	if (UNIV_UNLIKELY(error != DB_SUCCESS)) {

		goto error_handling;
	}

	/* Latch the InnoDB data dictionary exclusively so that no deadlocks
	or lock waits can happen in it during an index create operation. */

	row_mysql_lock_data_dictionary(trx);
	dict_locked = TRUE;

	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));

	/* If a new primary key is defined for the table we need
	to drop the original table and rebuild all indexes. */

	if (UNIV_UNLIKELY(new_primary)) {
		/* This transaction should be the only one
		operating on the table. */
		ut_a(prebuilt->table->n_mysql_handles_opened == 1);

		char*	new_table_name = innobase_create_temporary_tablename(
			heap, '1', prebuilt->table->name);

		/* Clone the table. */
		trx_set_dict_operation(trx, TRX_DICT_OP_TABLE);
		indexed_table = row_merge_create_temporary_table(
			new_table_name, index_defs, prebuilt->table, trx);

		if (!indexed_table) {

			switch (trx->error_state) {
			case DB_TABLESPACE_ALREADY_EXISTS:
			case DB_DUPLICATE_KEY:
				innobase_convert_tablename(new_table_name);
				my_error(HA_ERR_TABLE_EXIST, MYF(0),
					 new_table_name);
				error = HA_ERR_TABLE_EXIST;
				break;
			default:
				error = convert_error_code_to_mysql(
					trx->error_state,
					prebuilt->table->flags,
					user_thd);
			}

			ut_d(dict_table_check_for_dup_indexes(prebuilt->table,
							      FALSE));
			mem_heap_free(heap);
			trx_general_rollback_for_mysql(trx, NULL);
			row_mysql_unlock_data_dictionary(trx);
			trx_free_for_mysql(trx);
			trx_commit_for_mysql(prebuilt->trx);
			DBUG_RETURN(error);
		}

		trx->table_id = indexed_table->id;
	}

	/* Create the indexes in SYS_INDEXES and load into dictionary. */

	for (num_created = 0; num_created < num_of_idx; num_created++) {

		index[num_created] = row_merge_create_index(
			trx, indexed_table, &index_defs[num_created]);

		if (!index[num_created]) {
			error = trx->error_state;
			goto error_handling;
		}
	}

	ut_ad(error == DB_SUCCESS);

	/* Commit the data dictionary transaction in order to release
	the table locks on the system tables.  This means that if
	MySQL crashes while creating a new primary key inside
	row_merge_build_indexes(), indexed_table will not be dropped
	by trx_rollback_active().  It will have to be recovered or
	dropped by the database administrator. */
	trx_commit_for_mysql(trx);

	row_mysql_unlock_data_dictionary(trx);
	dict_locked = FALSE;

	ut_a(trx->n_active_thrs == 0);
	ut_a(UT_LIST_GET_LEN(trx->signals) == 0);

	if (UNIV_UNLIKELY(new_primary)) {
		/* A primary key is to be built.  Acquire an exclusive
		table lock also on the table that is being created. */
		ut_ad(indexed_table != prebuilt->table);

		error = row_merge_lock_table(prebuilt->trx, indexed_table,
					     LOCK_X);

		if (UNIV_UNLIKELY(error != DB_SUCCESS)) {

			goto error_handling;
		}
	}

	/* Read the clustered index of the table and build indexes
	based on this information using temporary files and merge sort. */
	error = row_merge_build_indexes(prebuilt->trx,
					prebuilt->table, indexed_table,
					index, num_of_idx, table);

error_handling:
	/* After an error, remove all those index definitions from the
	dictionary which were defined. */

	switch (error) {
	case DB_SUCCESS:
		ut_a(!dict_locked);

		ut_d(mutex_enter(&dict_sys->mutex));
		ut_d(dict_table_check_for_dup_indexes(prebuilt->table, TRUE));
		ut_d(mutex_exit(&dict_sys->mutex));
                *add = new ha_innobase_add_index(table, key_info, num_of_keys,
                                                 indexed_table);
		break;

	case DB_TOO_BIG_RECORD:
		my_error(HA_ERR_TO_BIG_ROW, MYF(0));
		goto error;
	case DB_PRIMARY_KEY_IS_NULL:
		my_error(ER_PRIMARY_CANT_HAVE_NULL, MYF(0));
		/* fall through */
	case DB_DUPLICATE_KEY:
error:
		prebuilt->trx->error_info = NULL;
		/* fall through */
	default:
		trx->error_state = DB_SUCCESS;

		if (new_primary) {
			if (indexed_table != prebuilt->table) {
				row_merge_drop_table(trx, indexed_table);
			}
		} else {
			if (!dict_locked) {
				row_mysql_lock_data_dictionary(trx);
				dict_locked = TRUE;
			}

			row_merge_drop_indexes(trx, indexed_table,
					       index, num_created);
		}
	}

	trx_commit_for_mysql(trx);
	if (prebuilt->trx) {
		trx_commit_for_mysql(prebuilt->trx);
	}

	if (dict_locked) {
		row_mysql_unlock_data_dictionary(trx);
	}

	trx_free_for_mysql(trx);
	mem_heap_free(heap);

	/* There might be work for utility threads.*/
	srv_active_wake_master_thread();

	DBUG_RETURN(convert_error_code_to_mysql(error, prebuilt->table->flags,
						user_thd));
}

/*******************************************************************//**
Finalize or undo add_index().
@return	0 or error number */
UNIV_INTERN
int
ha_innobase::final_add_index(
/*=========================*/
	handler_add_index*	add_arg,/*!< in: context from add_index() */
	bool			commit)	/*!< in: true=commit, false=rollback */
{
	ha_innobase_add_index*	add;
	trx_t*			trx;
	int			err	= 0;

	DBUG_ENTER("ha_innobase::final_add_index");

	ut_ad(add_arg);
	add = static_cast<class ha_innobase_add_index*>(add_arg);

	/* Create a background transaction for the operations on
	the data dictionary tables. */
	trx = innobase_trx_allocate(user_thd);
	trx_start_if_not_started(trx);

	/* Flag this transaction as a dictionary operation, so that
	the data dictionary will be locked in crash recovery. */
	trx_set_dict_operation(trx, TRX_DICT_OP_INDEX);

	/* Latch the InnoDB data dictionary exclusively so that no deadlocks
	or lock waits can happen in it during an index create operation. */
	row_mysql_lock_data_dictionary(trx);

	if (add->indexed_table != prebuilt->table) {
		ulint	error;

		/* We copied the table (new_primary). */
		if (commit) {
			mem_heap_t*	heap;
			char*		tmp_name;

			heap = mem_heap_create(1024);

			/* A new primary key was defined for the table
			and there was no error at this point. We can
			now rename the old table as a temporary table,
			rename the new temporary table as the old
			table and drop the old table. */
			tmp_name = innobase_create_temporary_tablename(
				heap, '2', prebuilt->table->name);

			error = row_merge_rename_tables(
				prebuilt->table, add->indexed_table,
				tmp_name, trx);

			switch (error) {
			case DB_TABLESPACE_ALREADY_EXISTS:
			case DB_DUPLICATE_KEY:
				innobase_convert_tablename(tmp_name);
				my_error(HA_ERR_TABLE_EXIST, MYF(0), tmp_name);
				err = HA_ERR_TABLE_EXIST;
				break;
			default:
				err = convert_error_code_to_mysql(
					error, prebuilt->table->flags,
					user_thd);
				break;
			}

			mem_heap_free(heap);
		}

		if (!commit || err) {
			error = row_merge_drop_table(trx, add->indexed_table);
			trx_commit_for_mysql(prebuilt->trx);
		} else {
			dict_table_t*	old_table = prebuilt->table;
			trx_commit_for_mysql(prebuilt->trx);
			row_prebuilt_free(prebuilt, TRUE);
			error = row_merge_drop_table(trx, old_table);
			add->indexed_table->n_mysql_handles_opened++;
			prebuilt = row_create_prebuilt(add->indexed_table,
				0 /* XXX Do we know the mysql_row_len here?
				Before the addition of this parameter to
				row_create_prebuilt() the mysql_row_len
				member was left 0 (from zalloc) in the
				prebuilt object. */);
		}

		err = convert_error_code_to_mysql(
			error, prebuilt->table->flags, user_thd);
	} else {
		/* We created secondary indexes (!new_primary). */

		if (commit) {
			err = convert_error_code_to_mysql(
				row_merge_rename_indexes(trx, prebuilt->table),
				prebuilt->table->flags, user_thd);
		}

		if (!commit || err) {
			dict_index_t*	index;
			dict_index_t*	next_index;

			for (index = dict_table_get_first_index(
				     prebuilt->table);
			     index; index = next_index) {

				next_index = dict_table_get_next_index(index);

				if (*index->name == TEMP_INDEX_PREFIX) {
					row_merge_drop_index(
						index, prebuilt->table, trx);
				}
			}
		}
	}

	/* If index is successfully built, we will need to rebuild index
	translation table. Set valid index entry count in the translation
	table to zero. */
	if (err == 0 && commit) {
		share->idx_trans_tbl.index_count = 0;
	}

	trx_commit_for_mysql(trx);
	if (prebuilt->trx) {
		trx_commit_for_mysql(prebuilt->trx);
	}

	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));
	row_mysql_unlock_data_dictionary(trx);

	trx_free_for_mysql(trx);

	/* There might be work for utility threads.*/
	srv_active_wake_master_thread();

	delete add;
	DBUG_RETURN(err);
}

/*******************************************************************//**
Prepare to drop some indexes of a table.
@return	0 or error number */
UNIV_INTERN
int
ha_innobase::prepare_drop_index(
/*============================*/
	TABLE*	table,		/*!< in: Table where indexes are dropped */
	uint*	key_num,	/*!< in: Key nums to be dropped */
	uint	num_of_keys)	/*!< in: Number of keys to be dropped */
{
	trx_t*		trx;
	int		err = 0;
	uint 		n_key;

	DBUG_ENTER("ha_innobase::prepare_drop_index");
	ut_ad(table);
	ut_ad(key_num);
	ut_ad(num_of_keys);
	if (srv_created_new_raw || srv_force_recovery) {
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}

	update_thd();

	trx_search_latch_release_if_reserved(prebuilt->trx);
	trx = prebuilt->trx;

	/* Test and mark all the indexes to be dropped */

	row_mysql_lock_data_dictionary(trx);
	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));

	/* Check that none of the indexes have previously been flagged
	for deletion. */
	{
		const dict_index_t*	index
			= dict_table_get_first_index(prebuilt->table);
		do {
			ut_a(!index->to_be_dropped);
			index = dict_table_get_next_index(index);
		} while (index);
	}

	for (n_key = 0; n_key < num_of_keys; n_key++) {
		const KEY*	key;
		dict_index_t*	index;

		key = table->key_info + key_num[n_key];
		index = dict_table_get_index_on_name_and_min_id(
			prebuilt->table, key->name);

		if (!index) {
			sql_print_error("InnoDB could not find key n:o %u "
					"with name %s for table %s",
					key_num[n_key],
					key ? key->name : "NULL",
					prebuilt->table->name);

			err = HA_ERR_KEY_NOT_FOUND;
			goto func_exit;
		}

		/* Refuse to drop the clustered index.  It would be
		better to automatically generate a clustered index,
		but mysql_alter_table() will call this method only
		after ha_innobase::add_index(). */

		if (dict_index_is_clust(index)) {
			my_error(ER_REQUIRES_PRIMARY_KEY, MYF(0));
			err = -1;
			goto func_exit;
		}

		rw_lock_x_lock(dict_index_get_lock(index));
		index->to_be_dropped = TRUE;
		rw_lock_x_unlock(dict_index_get_lock(index));
	}

	/* If FOREIGN_KEY_CHECKS = 1 you may not drop an index defined
	for a foreign key constraint because InnoDB requires that both
	tables contain indexes for the constraint. Such index can
	be dropped only if FOREIGN_KEY_CHECKS is set to 0.
	Note that CREATE INDEX id ON table does a CREATE INDEX and
	DROP INDEX, and we can ignore here foreign keys because a
	new index for the foreign key has already been created.

	We check for the foreign key constraints after marking the
	candidate indexes for deletion, because when we check for an
	equivalent foreign index we don't want to select an index that
	is later deleted. */

	if (trx->check_foreigns
	    && thd_sql_command(user_thd) != SQLCOM_CREATE_INDEX) {
		dict_index_t*	index;

		for (index = dict_table_get_first_index(prebuilt->table);
		     index;
		     index = dict_table_get_next_index(index)) {
			dict_foreign_t*	foreign;

			if (!index->to_be_dropped) {

				continue;
			}

			/* Check if the index is referenced. */
			foreign = dict_table_get_referenced_constraint(
				prebuilt->table, index);

			if (foreign) {
index_needed:
				trx_set_detailed_error(
					trx,
					"Index needed in foreign key "
					"constraint");

				trx->error_info = index;

				err = HA_ERR_DROP_INDEX_FK;
				break;
			} else {
				/* Check if this index references some
				other table */
				foreign = dict_table_get_foreign_constraint(
					prebuilt->table, index);

				if (foreign) {
					ut_a(foreign->foreign_index == index);

					/* Search for an equivalent index that
					the foreign key constraint could use
					if this index were to be deleted. */
					if (!dict_foreign_find_equiv_index(
						foreign)) {

						goto index_needed;
					}
				}
			}
		}
	} else if (thd_sql_command(user_thd) == SQLCOM_CREATE_INDEX) {
		/* This is a drop of a foreign key constraint index that
		was created by MySQL when the constraint was added.  MySQL
		does this when the user creates an index explicitly which
		can be used in place of the automatically generated index. */

		dict_index_t*	index;

		for (index = dict_table_get_first_index(prebuilt->table);
		     index;
		     index = dict_table_get_next_index(index)) {
			dict_foreign_t*	foreign;

			if (!index->to_be_dropped) {

				continue;
			}

			/* Check if this index references some other table */
			foreign = dict_table_get_foreign_constraint(
				prebuilt->table, index);

			if (foreign == NULL) {

				continue;
			}

			ut_a(foreign->foreign_index == index);

			/* Search for an equivalent index that the
			foreign key constraint could use if this index
			were to be deleted. */

			if (!dict_foreign_find_equiv_index(foreign)) {
				trx_set_detailed_error(
					trx,
					"Index needed in foreign key "
					"constraint");

				trx->error_info = foreign->foreign_index;

				err = HA_ERR_DROP_INDEX_FK;
				break;
			}
		}
	}

func_exit:
	if (err) {
		/* Undo our changes since there was some sort of error. */
		dict_index_t*	index
			= dict_table_get_first_index(prebuilt->table);

		do {
			rw_lock_x_lock(dict_index_get_lock(index));
			index->to_be_dropped = FALSE;
			rw_lock_x_unlock(dict_index_get_lock(index));
			index = dict_table_get_next_index(index);
		} while (index);
	}

	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));
	row_mysql_unlock_data_dictionary(trx);

	DBUG_RETURN(err);
}

/*******************************************************************//**
Drop the indexes that were passed to a successful prepare_drop_index().
@return	0 or error number */
UNIV_INTERN
int
ha_innobase::final_drop_index(
/*==========================*/
	TABLE*	table)		/*!< in: Table where indexes are dropped */
{
	dict_index_t*	index;		/*!< Index to be dropped */
	trx_t*		trx;		/*!< Transaction */
	int		err;

	DBUG_ENTER("ha_innobase::final_drop_index");
	ut_ad(table);

	if (srv_created_new_raw || srv_force_recovery) {
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}

	update_thd();

	trx_search_latch_release_if_reserved(prebuilt->trx);
	trx_start_if_not_started(prebuilt->trx);

	/* Create a background transaction for the operations on
	the data dictionary tables. */
	trx = innobase_trx_allocate(user_thd);
	trx_start_if_not_started(trx);

	/* Flag this transaction as a dictionary operation, so that
	the data dictionary will be locked in crash recovery. */
	trx_set_dict_operation(trx, TRX_DICT_OP_INDEX);

	/* Lock the table exclusively, to ensure that no active
	transaction depends on an index that is being dropped. */
	err = convert_error_code_to_mysql(
		row_merge_lock_table(prebuilt->trx, prebuilt->table, LOCK_X),
		prebuilt->table->flags, user_thd);

	row_mysql_lock_data_dictionary(trx);
	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));

	if (UNIV_UNLIKELY(err)) {

		/* Unmark the indexes to be dropped. */
		for (index = dict_table_get_first_index(prebuilt->table);
		     index; index = dict_table_get_next_index(index)) {

			rw_lock_x_lock(dict_index_get_lock(index));
			index->to_be_dropped = FALSE;
			rw_lock_x_unlock(dict_index_get_lock(index));
		}

		goto func_exit;
	}

	/* Drop indexes marked to be dropped */

	index = dict_table_get_first_index(prebuilt->table);

	while (index) {
		dict_index_t*	next_index;

		next_index = dict_table_get_next_index(index);

		if (index->to_be_dropped) {

			row_merge_drop_index(index, prebuilt->table, trx);
		}

		index = next_index;
	}

	/* Check that all flagged indexes were dropped. */
	for (index = dict_table_get_first_index(prebuilt->table);
	     index; index = dict_table_get_next_index(index)) {
		ut_a(!index->to_be_dropped);
	}

	/* We will need to rebuild index translation table. Set
	valid index entry count in the translation table to zero */
	share->idx_trans_tbl.index_count = 0;

func_exit:
	ut_d(dict_table_check_for_dup_indexes(prebuilt->table, FALSE));
	trx_commit_for_mysql(trx);
	trx_commit_for_mysql(prebuilt->trx);
	row_mysql_unlock_data_dictionary(trx);

	/* Flush the log to reduce probability that the .frm files and
	the InnoDB data dictionary get out-of-sync if the user runs
	with innodb_flush_log_at_trx_commit = 0 */

	log_buffer_flush_to_disk();

	trx_free_for_mysql(trx);

	/* Tell the InnoDB server that there might be work for
	utility threads: */

	srv_active_wake_master_thread();

	DBUG_RETURN(err);
}

/* FROM 5.6 */

/** Operations for creating an index in place */
static const Alter_inplace_info::HA_ALTER_FLAGS INNOBASE_INPLACE_CREATE
= Alter_inplace_info::ADD_INDEX_FLAG
| Alter_inplace_info::ADD_UNIQUE_INDEX_FLAG
| Alter_inplace_info::ADD_PK_INDEX_FLAG;// not online

/** Operations for altering a table that InnoDB does not care about */
static const Alter_inplace_info::HA_ALTER_FLAGS INNOBASE_INPLACE_IGNORE
= Alter_inplace_info::ALTER_COLUMN_DEFAULT_FLAG
| Alter_inplace_info::ALTER_RENAME_FLAG
| Alter_inplace_info::CHANGE_CREATE_OPTION_FLAG;

/** Operations that InnoDB can perform online */
static const Alter_inplace_info::HA_ALTER_FLAGS INNOBASE_ONLINE_OPERATIONS
= INNOBASE_INPLACE_IGNORE
| Alter_inplace_info::ADD_INDEX_FLAG
| Alter_inplace_info::DROP_INDEX_FLAG
| Alter_inplace_info::ADD_UNIQUE_INDEX_FLAG
| Alter_inplace_info::DROP_UNIQUE_INDEX_FLAG
| Alter_inplace_info::DROP_INDEX_FLAG
| Alter_inplace_info::DROP_FOREIGN_KEY_FLAG
| Alter_inplace_info::ALTER_COLUMN_NAME_FLAG;


/*******************************************************************//**
Get field default value from frm's default value record
@return	DB_SUCCESS or DB_ERROR number */
ulint 
get_field_def_value_from_frm(
    Field   *field,    
    char    *def,
    uint    *def_length,
    TABLE   *table ,
    Alter_inplace_info  *inplace_info,
    dict_col_t          *col,
    dict_table_t        *dct_table){
        DBUG_ENTER("get_field_def_value_from_frm");

        uchar*     default_ptr = table->s->default_values;
        ut_ad(default_ptr != NULL && def != NULL);        
        
        Alter_info *alter_info  = (Alter_info *)inplace_info->alter_info;
        HA_CREATE_INFO *create_info = inplace_info->create_info;        
        List_iterator<Create_field> def_it(alter_info->create_list);

        //table fields == create_list.element.
        ut_ad(alter_info->create_list.elements == table->s->fields);                  
     
        Field **    pf;
        Field *     p_field;         
        ulong       data_offset;
        uint        copy_length;
        unsigned char   *pos;
        Create_field *  c_field;

        // 1.compute the record off set (need to judge the create_info->table_options or not?)
        data_offset = (create_info->null_bits + 7) / 8;
        pf=table->field;

        // 2.get the field default value
        while(c_field = def_it++){
            ut_ad(*pf);
            p_field=*pf;
            pf++;            

            if(p_field == field)
            {             
                pos = default_ptr+c_field->offset+data_offset;

                dfield_t		dfield;
                const dtype_t*	dtype;
                ulint           type;
                unsigned char buff[5];

                dict_col_copy_type(col,dfield_get_type(&dfield));
                dtype  = dfield_get_type(&dfield);
                type   = dtype->mtype;

                /*
                    if the field is VARCHAR/VARMYSQL/BINARY,the start copy pos should get rid of the length's byte(1 or 2)
                    the pack_length is 2-256 258+
                */
                if( type == DATA_VARCHAR   || 
                    type == DATA_VARMYSQL  ||
                    type == DATA_BINARY
                    ){                  
                    char * def_val = (char *) my_malloc(c_field->def->max_length,MYF(MY_WME));
                    String mstr(def_val,c_field->def->max_length,c_field->def->default_charset()); 

                    String *tstr=&mstr;
                    const char *well_formed_error_pos;
                    const char *cannot_convert_error_pos;
                    const char *from_end_pos;

                    tstr = c_field->def->val_str(tstr);
                    copy_length = well_formed_copy_nchars(c_field->charset,def_val,c_field->def->max_length,c_field->def->default_charset(),
                        tstr->ptr(),tstr->length(),tstr->length(),&well_formed_error_pos,&cannot_convert_error_pos,&from_end_pos);
                    my_free(def_val);                    
                }else{
                    //ut_ad(0);
                    /* here we should use the pack_length */

                    copy_length = c_field->pack_length;
                }  

                row_mysql_store_col_in_innobase_format(&dfield,(unsigned char *)&buff,TRUE,
                    pos,copy_length,dict_table_is_comp(dct_table));

                ut_ad(copy_length);

                memcpy(def,dfield.data,copy_length);
                *def_length = copy_length;

                DBUG_RETURN(DB_SUCCESS);
            } 
        }
        DBUG_PRINT("warn",("Cannot find the givin Field!"));

        DBUG_RETURN(DB_ERROR);
}


ulint
innodbase_fill_col_info(
    trx_t               *trx,
    dict_col_t          *col,
    dict_table_t        *table,
    TABLE               *tmp_table,
    ulint               field_idx,
    mem_heap_t*         heap,
    Create_field *      cfield,
    Alter_inplace_info* inplace_info
)
{
    ulint		col_type;
    ulint		col_len;
    ulint		nulls_allowed;
    ulint		unsigned_type;
    ulint		binary_type;
    ulint		long_true_varchar;
    ulint		charset_no;
    ulint       error = DB_SUCCESS;
    ulint       prtype;

    Field               *field;

    DBUG_ENTER("innodbase_fill_col_info");

    field = tmp_table->field[field_idx];

    col_type = get_innobase_type_from_mysql_type(&unsigned_type, field);

    if (!col_type) {
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "Error alter table '%s' with add"
            "column '%s'. Please check its "
            "column type and try to re-create "
            "the table with an appropriate "
            "column type.",
            table->name, (char*) field->field_name);
        
        error = DB_ERROR;
        goto err_exit;
    }

    if (field->null_ptr) {
        nulls_allowed = 0;
    } else {
        nulls_allowed = DATA_NOT_NULL;
    }

    if (field->binary()) {
        binary_type = DATA_BINARY_TYPE;
    } else {
        binary_type = 0;
    }

    charset_no = 0;

    if (dtype_is_string_type(col_type)) {

        charset_no = (ulint)field->charset()->number;

        if (UNIV_UNLIKELY(charset_no >= 256)) {
            /* in data0type.h we assume that the
            number fits in one byte in prtype */
            push_warning_printf(
                (THD*) trx->mysql_thd,
                MYSQL_ERROR::WARN_LEVEL_WARN,
                ER_CANT_CREATE_TABLE,
                "In InnoDB, charset-collation codes"
                " must be below 256."
                " Unsupported code %lu.",
                (ulong) charset_no);
            DBUG_RETURN(ER_CANT_CREATE_TABLE);
        }
    }

    ut_a(field->type() < 256); /* we assume in dtype_form_prtype()
                               that this fits in one byte */
    col_len = field->pack_length();

    /* The MySQL pack length contains 1 or 2 bytes length field
    for a true VARCHAR. Let us subtract that, so that the InnoDB
    column length in the InnoDB data dictionary is the real
    maximum byte length of the actual data. */

    long_true_varchar = 0;

    if (field->type() == MYSQL_TYPE_VARCHAR) {
        col_len -= ((Field_varstring*)field)->length_bytes;

        if (((Field_varstring*)field)->length_bytes == 2) {
            long_true_varchar = DATA_LONG_TRUE_VARCHAR;
        }
    }

    /* First check whether the column to be added has a
    system reserved name. */
    if (dict_col_name_is_reserved(field->field_name)){
        my_error(ER_WRONG_COLUMN_NAME, MYF(0),
            field->field_name);

        error = DB_ERROR;
        goto err_exit;
    }

    

    prtype = dtype_form_prtype((ulint)field->type() | nulls_allowed | unsigned_type
                                    | binary_type | long_true_varchar,
                                charset_no);

    dict_mem_fill_column_struct(col, field_idx, col_type, prtype, col_len);

    if (!dict_col_is_nullable(col))
    {
        //TODO(GCS) : set default value，考虑大小端问题  done    
      
        /* 测试代码 */     
      
        char *buff = (char *) mem_heap_alloc(heap,8000);     
        uint defleng;
        error=get_field_def_value_from_frm(field,buff,(uint *)&defleng,tmp_table,inplace_info,col,table);
        if(error!=DB_SUCCESS){
            my_free(buff);
            goto err_exit;
        }
        dict_mem_table_add_col_default(table, col, heap, (char*)buff,defleng);       
    }
    
err_exit:
    DBUG_RETURN(error);
}

/*
    由于默认值是多个不同类型，因此插入系统表中是使用二进制串形式

*/
static
char*
get_default_hex_str(
    byte*           def_val,
    ulint           def_val_len,
    byte*           buffer,
    ulint           buffer_len
)
{
    ulint       i;  
    ulint       j = 0;
    static const char* hex_array = "0123456789ABCDEF";
    ut_a(buffer_len > def_val_len * 2 + 2 + 1);

    buffer[j++] = '0';
    buffer[j++] = 'x';
    for (i = 0; i < def_val_len; ++i)
    {
        buffer[j++] = hex_array[def_val[i] >> 4];
        buffer[j++] = hex_array[def_val[i] & 0x0F];
    }

    buffer[j] = 0;

    return (char*)buffer;
}



ulint
innobase_add_column_to_dictionary_for_gcs(
   dict_table_t*            table,                                  
   ulint                    pos,
   const char*              name,
   ulint                    mtype,
   ulint                    prtype,
   ulint                    len,
   trx_t*                   trx
)
{
    pars_info_t*	info;
    ulint   		error_no = DB_SUCCESS;

    info = pars_info_create();  /* que_eval_sql执行完会释放 */

    pars_info_add_ull_literal(info, "table_id", table->id);
    pars_info_add_int4_literal(info, "pos", pos);
    pars_info_add_str_literal(info, "name", name);
    pars_info_add_int4_literal(info, "mtype", mtype);
    pars_info_add_int4_literal(info, "prtype", prtype);
    pars_info_add_int4_literal(info, "len", len);

    /* SYS_COLUMNS(table_id, pos, name, mtype, prtype, len, prec) */
    error_no = que_eval_sql(
        info,
        "PROCEDURE ADD_SYS_COLUMNS_PROC () IS\n"
        "BEGIN\n"
        "INSERT INTO SYS_COLUMNS VALUES(:table_id, :pos,\n"
        ":name, :mtype, :prtype, :len, 0);\n"
        "END;\n",
        FALSE, trx);

    DBUG_EXECUTE_IF("ib_add_column_error",
        error_no = DB_OUT_OF_FILE_SPACE;);

    if (error_no != DB_SUCCESS)
    {
        //for safe
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "ADD_SYS_COLUMNS_PROC error %s %d field_name(%s)", __FILE__, __LINE__, name);
    }

    return error_no;
}


ulint
innobase_update_systable_n_cols_for_gcs(
    dict_table_t*               table,
    ulint                       new_n_field,
    trx_t*                      trx
)
{
    pars_info_t*	info;
    ulint   		error_no = DB_SUCCESS;
    lint            n_cols_before_alter;

    info = pars_info_create();  /* que_eval_sql执行完会释放 */

    //ut_ad(dict_table_get_n_cols(table) - DATA_N_SYS_COLS + n_add == new_n_field);

    ut_ad(dict_table_is_gcs(table));

    pars_info_add_int4_literal(info, "n_col", new_n_field | (1 << 31) | (1 << 30));

    if (table->n_cols_before_alter_table > 0)
    {
        n_cols_before_alter = table->n_cols_before_alter_table - DATA_N_SYS_COLS;
        ut_ad(n_cols_before_alter > 0);
    }
    else
    {
        /* 第一次alter table add column */
        n_cols_before_alter = dict_table_get_n_cols(table) - DATA_N_SYS_COLS;
    }


    ut_ad(!(table->flags >> DICT_TF2_SHIFT));
    pars_info_add_int4_literal(info, "mix_len", n_cols_before_alter << 16);     /* 存在高两字节！ */
    pars_info_add_str_literal(info, "table_name", table->name);

    /* 更新列数 */
    error_no = que_eval_sql(
        info,
        "PROCEDURE UPDATE_SYS_TABLES_N_COLS_PROC () IS\n"
        "BEGIN\n"
        "UPDATE SYS_TABLES SET N_COLS = :n_col, MIX_LEN = :mix_len \n"
        "WHERE NAME = :table_name;\n"
        "END;\n",
        FALSE, trx);

    if (error_no != DB_SUCCESS)
    {
        //for safe
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "UPDATE_SYS_TABLES_N_COLS_PROC error %s %d table_name(%s)", __FILE__, __LINE__, table->name);
    }

    return error_no;
}

ulint
innobase_add_column_default_to_dictionary_for_gcs(
    dict_table_t*           table,
    dict_col_t*             col,
    trx_t*                  trx
)
{
    pars_info_t*	info;
    ulint   		error_no = DB_SUCCESS;

    info = pars_info_create();  /* que_eval_sql执行完会释放 */

    ut_ad(dict_table_is_gcs(table) && !dict_col_is_nullable(col));
    
    pars_info_add_ull_literal(info, "table_id", table->id);
    pars_info_add_int4_literal(info, "pos", col->ind);

    //TODO(GCS) ,whether default value too big?
    pars_info_add_binary_literal(info, "def_val", col->def_val->def_val, col->def_val->def_val_len);
    pars_info_add_int4_literal(info, "def_val_len", col->def_val->def_val_len);

    error_no = que_eval_sql(
        info,
        "PROCEDURE ADD_SYS_ADDED_COLS_DEFAULT_PROC () IS\n"
        "BEGIN\n"
        "INSERT INTO SYS_ADDED_COLS_DEFAULT VALUES(:table_id, :pos, :def_val, :def_val_len);\n"
        "END;\n",
        FALSE, trx);

    if (error_no != DB_SUCCESS)
    {
        //for safe
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "ADD_SYS_ADDED_COLS_DEFAULT_PROC error %s %d table_name(%s) colid(%d)", __FILE__, __LINE__, table->name, col->ind);
    }

    return error_no;
    
}


/*

Return
    true : Error
    false : success
*/
ulint
innobase_add_columns_simple(
    /*===================*/
    mem_heap_t*         heap,
    trx_t*			    trx,
    dict_table_t*       table,
    TABLE*              tmp_table,
    Alter_inplace_info* inplace_info
)
{
    //pars_info_t*	info;
    ulint   		error_no = DB_SUCCESS;
    Alter_info*     alter_info = static_cast<Alter_info*>(inplace_info->alter_info);
    Create_field*   cfield;
    List_iterator<Create_field> def_it(alter_info->create_list);
    ulint           prev_add_idx = ULINT_UNDEFINED;
    ulint           idx = 0;
    ulint           add_idx = 0;
    Field           *field;
    dict_col_t      *col_arr = NULL;
    ulint           n_add = 0;
    char*           col_names = NULL;
    char*           col_name = NULL;
    ulint           lock_retry = 0;
    ibool           locked = FALSE;
    ulint           n_cols_before_alter = 0;

    DBUG_ENTER("innobase_add_columns_simple");

    DBUG_ASSERT(trx_get_dict_operation(trx) == TRX_DICT_OP_INDEX);
    ut_ad(trx->dict_operation_lock_mode == RW_X_LATCH);
    ut_ad(mutex_own(&dict_sys->mutex));
#ifdef UNIV_SYNC_DEBUG
    ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

    col_arr = (dict_col_t *)mem_heap_zalloc(heap, sizeof(dict_col_t) * tmp_table->s->fields);
    col_names = (char*)mem_heap_zalloc(heap, tmp_table->s->fields * 200);       /* 每个字段长度必小于200 */
    if (col_arr == NULL || col_names == NULL)
    {
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "mem_heap_alloc error %s %d", __FILE__, __LINE__);

        goto err_exit;
    }

    col_name = col_names;

    def_it.rewind();
    while (cfield =def_it++)
    {
        //new field，并且是最后若干列
        if (!cfield->field)
        {
            ut_ad(!cfield->change && !cfield->after);

            if (cfield->change || cfield->after)
            {
                push_warning_printf(
                    (THD*) trx->mysql_thd,
                    MYSQL_ERROR::WARN_LEVEL_WARN,
                    ER_CANT_CREATE_TABLE,
                    "!cfield->change && !cfield->after error %s %d", __FILE__, __LINE__);

                //for safe
                goto err_exit;
            }

            add_idx = idx;
            ut_ad( prev_add_idx == ULINT_UNDEFINED || add_idx == prev_add_idx + 1);
            
            if (prev_add_idx != ULINT_UNDEFINED && add_idx != prev_add_idx + 1)
            {
                //for safe
                push_warning_printf(
                    (THD*) trx->mysql_thd,
                    MYSQL_ERROR::WARN_LEVEL_WARN,
                    ER_CANT_CREATE_TABLE,
                    "prev_lock_idx != ULINT_UNDEFINED && lock_idx != prev_lock_idx + 1 %s %d", __FILE__, __LINE__);

                goto err_exit;
            }
            
            prev_add_idx = add_idx;

            field = tmp_table->field[idx];

            error_no = innodbase_fill_col_info(trx, &col_arr[idx], table, tmp_table, idx, heap,cfield,inplace_info);
            if (error_no != DB_SUCCESS)
                goto err_exit;

            error_no = innobase_add_column_to_dictionary_for_gcs(table, 
                                            col_arr[idx].ind, field->field_name, 
                                            col_arr[idx].mtype, col_arr[idx].prtype, 
                                            col_arr[idx].len, 
                                            trx);
            if (error_no != DB_SUCCESS )
            {
                goto err_exit;
            }

            if (!dict_col_is_nullable(&col_arr[idx]))
            {
                /* 必含默认值 */
                error_no = innobase_add_column_default_to_dictionary_for_gcs(table, &col_arr[idx], trx);
                if (error_no != DB_SUCCESS )
                {
                    goto err_exit;
                }
            }

            n_add++;

            strcpy(col_name, field->field_name);
            col_name += strlen(col_name) + 1;
        }
        else
        {
			// do a special judge for GEOMETRY TYPE
            ut_ad(tmp_table->field[idx]->is_equal(cfield) || cfield->sql_type == MYSQL_TYPE_GEOMETRY);

            if (!tmp_table->field[idx]->is_equal(cfield) && !(cfield->sql_type == MYSQL_TYPE_GEOMETRY) )
            {
                //for safe
                goto err_exit;
            }

            memcpy(&col_arr[idx], dict_table_get_nth_col(table, idx), sizeof(dict_col_t));

            strcpy(col_name, dict_table_get_col_name(table, idx));
            col_name += strlen(col_name) + 1;
            
        }
        idx ++;

        ut_a((ulint)(col_name - col_names) < 200 * tmp_table->s->fields);
    }

    ut_ad(dict_table_get_n_cols(table) - DATA_N_SYS_COLS + n_add == tmp_table->s->fields);

    error_no = innobase_update_systable_n_cols_for_gcs(table, tmp_table->s->fields, trx);
    if (error_no != DB_SUCCESS)
        goto err_exit;

    while (lock_retry++ < 10)
    {
        if (!rw_lock_x_lock_nowait(&btr_search_latch))
        {
            /* Sleep for 10ms before trying again. */
            os_thread_sleep(10000);
            continue;
        }

        locked = TRUE;
        break;
    }

    if (!locked)
    {
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "rw_lock_x_lock_nowait(&btr_search_latch) failed %s %d", __FILE__, __LINE__);

        error_no = DB_ERROR;

        goto err_exit;
    }

    dict_mem_table_add_col_simple(table, col_arr, tmp_table->s->fields, col_names, col_name - col_names);

    rw_lock_x_unlock(&btr_search_latch);

err_exit:
    DBUG_RETURN(error_no);
    
}


/*
fast alter row_format 
here we support two kinds of fast row-format alter:
1.gcs->Compact(no fast alter be done before)
2.Compact->GCS 

return:
false  success
true   errno

*/
ulint
innobase_alter_row_format_simple(
	 /*===================*/
	mem_heap_t*         heap,
	trx_t*			    trx,
	dict_table_t*       table,	
	TABLE*              tmp_table,
	Alter_inplace_info* inplace_info,
    enum innodb_row_format_change   change_flag
)
{
	//lock retyr times	
	pars_info_t *	info;
	ulint   		error_no = DB_SUCCESS;
	ulint			lock_retry = 0;
	uint			ncols;    
	//for modify the memory
	ibool			locked = FALSE;

    
	DBUG_ENTER("innobase_alter_row_format_simple");
	DBUG_ASSERT(trx_get_dict_operation(trx) == TRX_DICT_OP_INDEX);
	ut_ad(trx->dict_operation_lock_mode == RW_X_LATCH);
	ut_ad(mutex_own(&dict_sys->mutex));
	ut_ad((tmp_table->s->row_type == ROW_TYPE_COMPACT && dict_table_is_gcs(table))||
        (tmp_table->s->row_type == ROW_TYPE_GCS && !dict_table_is_gcs(table)));

    ut_a(change_flag != INNODB_ROW_FORMAT_CHANGE_NO);

	//todo: check if the old table is Compact or not!

#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

	info = pars_info_create();

    ut_ad(tmp_table->s->fields == table->n_cols - DATA_N_SYS_COLS);

    //set the row format flag
    if(change_flag == INNODB_ROW_FORMAT_COMACT_TO_GCS){
	    //compact -> gcs
        pars_info_add_int4_literal(info,"n_col",tmp_table->s->fields | (1<<31)|(1<<30));
    }else if(change_flag == INNODB_ROW_FORMAT_GCS_TO_COMPACT){
        //gcs -> compact
        ut_a(!dict_table_is_gcs_after_alter_table(table));
        pars_info_add_int4_literal(info,"n_col",tmp_table->s->fields | (1<<31));
    }else{
        //should never come to here!
        ut_a(0);
    }

	pars_info_add_str_literal(info, "table_name", table->name);

	//just for test
	ncols = dict_table_get_n_cols(table);

    error_no = que_eval_sql(
        info,
        "PROCEDURE UPDATE_SYS_TABLES_FAST_ALTER_ROWFOMAT () IS\n"
        "BEGIN\n"
        "UPDATE SYS_TABLES SET N_COLS = :n_col \n"
        "WHERE NAME = :table_name;\n"
        "END;\n",
        FALSE, trx);	

    if(error_no != DB_SUCCESS){
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
             "UPDATE_SYS_TABLES_FAST_ALTER_ROWFOMAT error %s %d table_name(%s)", __FILE__, __LINE__, table->name);
        error_no = DB_ERROR;
       
        goto err_exit;
    }

    // to modify the memory
    //get rw_x lock on select
    while(lock_retry++ <10){
        if(!rw_lock_x_lock_nowait(&btr_search_latch)){

            /* Sleep for 10ms before trying again. */
            os_thread_sleep(10000);
            continue;
        }

        locked = TRUE;
        break;
    }

    if (!locked)
    {
        push_warning_printf(
            (THD*) trx->mysql_thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "rw_lock_x_lock_nowait(&btr_search_latch) failed %s %d", __FILE__, __LINE__);

        error_no = DB_ERROR;

        goto err_exit;
    }

    //todo: modify the memory for fast alter row format

     if(change_flag == INNODB_ROW_FORMAT_COMACT_TO_GCS){
         ut_ad(!table->is_gcs);
         table->is_gcs = TRUE;
     }else if(change_flag == INNODB_ROW_FORMAT_GCS_TO_COMPACT){
         ut_ad(table->is_gcs);
         table->is_gcs = FALSE;
     }else{
         //never come to here!
         ut_a(0);
     }
    //unlock
    rw_lock_x_unlock(&btr_search_latch);

err_exit:
	DBUG_RETURN(error_no);
}

/** Rename a column.
@param table_share	the TABLE_SHARE
@param prebuilt		the prebuilt struct
@param trx		data dictionary transaction
@param nth_col		0-based index of the column
@param from		old column name
@param to		new column name
@retval true		Failure
@retval false		Success */
static __attribute__((nonnull, warn_unused_result))
bool
innobase_rename_column(
/*===================*/
	const TABLE_SHARE*	table_share,
	row_prebuilt_t*		prebuilt,
	trx_t*			trx,
	ulint			nth_col,
	const char*		from,
	const char*		to)
{
	pars_info_t*	info;
	ulint   		error;

	DBUG_ENTER("innobase_rename_column");

	DBUG_ASSERT(trx_get_dict_operation(trx) == TRX_DICT_OP_INDEX);
	ut_ad(trx->dict_operation_lock_mode == RW_X_LATCH);
	ut_ad(mutex_own(&dict_sys->mutex));
#ifdef UNIV_SYNC_DEBUG
	ut_ad(rw_lock_own(&dict_operation_lock, RW_LOCK_EX));
#endif /* UNIV_SYNC_DEBUG */

	info = pars_info_create();

	pars_info_add_ull_literal(info, "tableid", prebuilt->table->id);
	pars_info_add_int4_literal(info, "nth", nth_col);
	pars_info_add_str_literal(info, "old", from);
	pars_info_add_str_literal(info, "new", to);

	trx->op_info = "renaming column in SYS_COLUMNS";

	error = que_eval_sql(
		info,
		"PROCEDURE RENAME_SYS_COLUMNS_PROC () IS\n"
		"BEGIN\n"
		"UPDATE SYS_COLUMNS SET NAME=:new\n"
		"WHERE TABLE_ID=:tableid AND NAME=:old\n"
		"AND POS=:nth;\n"
		"END;\n",
		FALSE, trx);

	DBUG_EXECUTE_IF("ib_rename_column_error",
			error = DB_OUT_OF_FILE_SPACE;);

	if (error != DB_SUCCESS) {
err_exit:
		//my_error_innodb(error, table_share->table_name.str, 0);
		trx->error_state = DB_SUCCESS;
		trx->op_info = "";
		DBUG_RETURN(true);
	}

	trx->op_info = "renaming column in SYS_FIELDS";

	for (dict_index_t* index = dict_table_get_first_index(prebuilt->table);
	     index != NULL;
	     index = dict_table_get_next_index(index)) {

		for (ulint i = 0; i < dict_index_get_n_fields(index); i++) {
			if (strcmp(dict_index_get_nth_field(index, i)->name,
				   from)) {
				continue;
			}

			info = pars_info_create();

			pars_info_add_ull_literal(info, "indexid", index->id);
			pars_info_add_int4_literal(info, "nth", i);
			pars_info_add_str_literal(info, "old", from);
			pars_info_add_str_literal(info, "new", to);

			error = que_eval_sql(
				info,
				"PROCEDURE RENAME_SYS_FIELDS_PROC () IS\n"
				"BEGIN\n"

				"UPDATE SYS_FIELDS SET COL_NAME=:new\n"
				"WHERE INDEX_ID=:indexid AND COL_NAME=:old\n"
				"AND POS=:nth;\n"

				/* Try again, in case there is a prefix_len
				encoded in SYS_FIELDS.POS */

				"UPDATE SYS_FIELDS SET COL_NAME=:new\n"
				"WHERE INDEX_ID=:indexid AND COL_NAME=:old\n"
				"AND POS>=65536*:nth AND POS<65536*(:nth+1);\n"

				"END;\n",
				FALSE, trx);

			if (error != DB_SUCCESS) {
				goto err_exit;
			}
		}
	}

	trx->op_info = "renaming column in SYS_FOREIGN_COLS";

	for (dict_foreign_t* foreign = UT_LIST_GET_FIRST(
		     prebuilt->table->foreign_list);
	     foreign != NULL;
	     foreign = UT_LIST_GET_NEXT(foreign_list, foreign)) {
		for (unsigned i = 0; i < foreign->n_fields; i++) {
			if (strcmp(foreign->foreign_col_names[i], from)) {
				continue;
			}

			info = pars_info_create();

			pars_info_add_str_literal(info, "id", foreign->id);
			pars_info_add_int4_literal(info, "nth", i);
			pars_info_add_str_literal(info, "old", from);
			pars_info_add_str_literal(info, "new", to);

			error = que_eval_sql(
				info,
				"PROCEDURE RENAME_SYS_FOREIGN_F_PROC () IS\n"
				"BEGIN\n"
				"UPDATE SYS_FOREIGN_COLS\n"
				"SET FOR_COL_NAME=:new\n"
				"WHERE ID=:id AND POS=:nth\n"
				"AND FOR_COL_NAME=:old;\n"
				"END;\n",
				FALSE, trx);

			if (error != DB_SUCCESS) {
				goto err_exit;
			}
		}
	}

	for (dict_foreign_t* foreign = UT_LIST_GET_FIRST(
		     prebuilt->table->referenced_list);
	     foreign != NULL;
	     foreign = UT_LIST_GET_NEXT(referenced_list, foreign)) {
		for (unsigned i = 0; i < foreign->n_fields; i++) {
			if (strcmp(foreign->referenced_col_names[i], from)) {
				continue;
			}

			info = pars_info_create();

			pars_info_add_str_literal(info, "id", foreign->id);
			pars_info_add_int4_literal(info, "nth", i);
			pars_info_add_str_literal(info, "old", from);
			pars_info_add_str_literal(info, "new", to);

			error = que_eval_sql(
				info,
				"PROCEDURE RENAME_SYS_FOREIGN_R_PROC () IS\n"
				"BEGIN\n"
				"UPDATE SYS_FOREIGN_COLS\n"
				"SET REF_COL_NAME=:new\n"
				"WHERE ID=:id AND POS=:nth\n"
				"AND REF_COL_NAME=:old;\n"
				"END;\n",
				FALSE, trx);

			if (error != DB_SUCCESS) {
				goto err_exit;
			}
		}
	}

	trx->op_info = "";
	/* Rename the column in the data dictionary cache. */
	//dict_mem_table_col_rename(prebuilt->table, nth_col, from, to);
	DBUG_RETURN(false);
}

/*
false: not suport
true: support
*/
UNIV_INTERN
bool
ha_innobase::check_if_supported_inplace_alter(
    /*==========================================*/
    THD                     *thd,
    TABLE                   *table,
    Alter_inplace_info      *inplace_info
)
{
    DBUG_ENTER("check_if_supported_inplace_alter");

	ut_ad(inplace_info);

	HA_CREATE_INFO * create_info = inplace_info->create_info;
	enum row_type tb_innodb_row_type = get_row_type();

	/*
	check for fast alter row_format:
	if there are alter row_format in the alter statement,
	and the NEW/OLD row_format are in GCS or COMPACT,we can fast alter it,
	or we cannot fast alter.
	
	we returned directly if row_format modified,so the judgement below wouldn't be affected.
	*/

	if(inplace_info->handler_flags == Alter_inplace_info::CHANGE_CREATE_OPTION_FLAG){

	  if(create_info->used_fields ==HA_CREATE_USED_ROW_FORMAT 
		&& this->is_support_fast_rowformat_change(create_info->row_type, tb_innodb_row_type) != INNODB_ROW_FORMAT_CHANGE_NO){
		  DBUG_RETURN(true);
	  }		

	  DBUG_RETURN(false);
	}

    //check gcs fast add column   

    /* 
	如果行格式不是GCS,或者创建的行格式与底层的行格式不一样,则不能直接快速alter 
	*/
    if( ROW_TYPE_GCS != tb_innodb_row_type || 
		( (create_info->used_fields & HA_CREATE_USED_ROW_FORMAT) &&
		  create_info->row_type != tb_innodb_row_type )){
            DBUG_RETURN(false);
    }

    
    /* 只有增加字段的语句 */
    if(inplace_info->handler_flags & ~(Alter_inplace_info::ADD_COLUMN_FLAG)){
        DBUG_RETURN(true);
    }
    
    
    DBUG_RETURN(true);
}

/*
  check if the fast row format alter can  excute
*/

UNIV_INTERN
enum innodb_row_format_change
ha_innobase::is_support_fast_rowformat_change(
 /*=============================*/
 enum row_type		  new_type,
 enum row_type		  old_type){

     DBUG_ENTER("is_support_fast_rowformat_change");
     if(new_type == ROW_TYPE_GCS && old_type == ROW_TYPE_COMPACT)
         DBUG_RETURN(INNODB_ROW_FORMAT_COMACT_TO_GCS);

     if(new_type == ROW_TYPE_COMPACT && old_type == ROW_TYPE_GCS){
         /*
         here to check the dict if support change from gcs to compact

         if the table have been fast add column,the row_format have changed to 'real' GCS row_format,
         you cannot fast alter the table's row_format(check the total column in the dict).
         */

         //the table should be gcs!    
         ut_ad(dict_table_is_gcs(this->prebuilt->table));

         //check if the table have been fast altered
         if (!dict_table_is_gcs_after_alter_table(this->prebuilt->table)) {
            DBUG_RETURN(INNODB_ROW_FORMAT_GCS_TO_COMPACT);
         }
     }

     DBUG_RETURN(INNODB_ROW_FORMAT_CHANGE_NO);
}


UNIV_INTERN
int
ha_innobase::inplace_alter_table(
/*=============================*/
	TABLE*			        table,
    TABLE*                  tmp_table,
	Alter_inplace_info*	    ha_alter_info)
{
    trx_t*                  user_trx;
    THD*			        thd;
    trx_t*                  trx;
    ulint                   err = DB_ERROR;
    char                    norm_name[1000];
    dict_table_t            *dict_table;
    mem_heap_t*             heap;

    DBUG_ENTER("inplace_alter_table");

    thd = ha_thd();

    DBUG_ASSERT(thd == this->user_thd);

    DBUG_ASSERT(check_if_supported_inplace_alter(thd, table, ha_alter_info));

    /* inplace alter table 开始日志 */
    ut_print_timestamp(stderr);
    fprintf(stderr, "  [InnoDB inplace alter table]  start, query: %s; db_name:%s; tmp_name: %s \n", ha_query(), table->s->db.str, tmp_table->alias);

    /* Get the transaction associated with the current thd, or create one
	if not yet created */

    normalize_table_name(norm_name, table->s->normalized_path.str);

    dict_table = dict_table_get(norm_name, FALSE);
    if (dict_table == NULL)
    {
        push_warning_printf(
            (THD*) thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "table %s doesn't exist %s %d", norm_name, __FILE__, __LINE__);

        goto error;
    }

    heap = mem_heap_create(1024);
    if (heap == NULL)
    {
        push_warning_printf(
            (THD*) thd,
            MYSQL_ERROR::WARN_LEVEL_WARN,
            ER_CANT_CREATE_TABLE,
            "mem_heap_create error %s %d", __FILE__, __LINE__);

        goto error;
    }

	user_trx = check_trx_exists(thd);

	/* In case MySQL calls this in the middle of a SELECT query, release
	possible adaptive hash latch to avoid deadlocks of threads */

	trx_search_latch_release_if_reserved(user_trx);

    trx_start_if_not_started(user_trx);

	/* 创建一个后台事务，用于字典操作. */
	trx = innobase_trx_allocate(thd);
	trx_start_if_not_started(trx);
    
    /* Flag this transaction as a dictionary operation, so that
	the data dictionary will be locked in crash recovery. */
    trx_set_dict_operation(trx, TRX_DICT_OP_INDEX);

    /* 全局字段锁 */
    row_mysql_lock_data_dictionary(trx);

    if (ha_alter_info->handler_flags == Alter_inplace_info::ADD_COLUMN_FLAG)  /* 仅加简单列操作 */
    {
        err = innobase_add_columns_simple(heap, trx, dict_table, tmp_table, ha_alter_info);
    }
	else if(ha_alter_info->handler_flags == Alter_inplace_info::CHANGE_CREATE_OPTION_FLAG)
	{
		//fast alter table row format! 
        err = innobase_alter_row_format_simple(heap,trx,dict_table,tmp_table,ha_alter_info,
                is_support_fast_rowformat_change(tmp_table->s->row_type,get_row_type()));
		
	}else{
        ut_ad(FALSE);
        //to do 暂时断言
    }
    
    /* 成功就提交，否则回滚 */
    if (err == DB_SUCCESS)
    {
        trx_commit_for_mysql(trx);

        /* inplace alter table commit日志 */
        ut_print_timestamp(stderr);
        fprintf(stderr, "  [InnoDB inplace alter table]  commit, query: %s; db_name:%s; tmp_name: %s \n", ha_query(), table->s->db.str, tmp_table->alias);
    }
    else
    {
        trx_rollback_for_mysql(trx);

        /* inplace alter table rollback日志 */
        ut_print_timestamp(stderr);
        fprintf(stderr, "  [InnoDB inplace alter table]  rollback, error no : %ul,  query: %s; db_name:%s; tmp_name: %s \n", err, ha_query(), table->s->db.str, tmp_table->alias);
    }

    /* 锁什么时候释放 */


    //dict_table_remove_from_cache(dict_table);

    row_mysql_unlock_data_dictionary(trx);

    trx_free_for_mysql(trx);

    trx_commit_for_mysql(user_trx);

	/* Flush the log to reduce probability that the .frm files and
	the InnoDB data dictionary get out-of-sync if the user runs
	with innodb_flush_log_at_trx_commit = 0 */

	log_buffer_flush_to_disk();

	/* Tell the InnoDB server that there might be work for
	utility threads: */

	srv_active_wake_master_thread();

    mem_heap_free(heap);

// func_exit:
// 	if (err == 0 && (ha_alter_info->create_info->used_fields
// 			 & HA_CREATE_USED_AUTO)) {
// 		dict_table_autoinc_lock(prebuilt->table);
// 		dict_table_autoinc_initialize(
// 			prebuilt->table,
// 			ha_alter_info->create_info->auto_increment_value);
// 		dict_table_autoinc_unlock(prebuilt->table);
// 	}
// 
// ret:
// 	if (err == 0) {
// 		MONITOR_ATOMIC_DEC(MONITOR_PENDING_ALTER_TABLE);
// 	}
// 	DBUG_RETURN(err != 0);

error:
    err = convert_error_code_to_mysql(err, 0, NULL);

    DBUG_RETURN(err);
}

