/*****************************************************************************

Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/******************************************************************//**
@file dict/dict0mem.c
Data dictionary memory object creation

Created 1/8/1996 Heikki Tuuri
***********************************************************************/

#include "dict0mem.h"

#ifdef UNIV_NONINL
#include "dict0mem.ic"
#endif

#include "rem0rec.h"
#include "data0type.h"
#include "mach0data.h"
#include "dict0dict.h"
#include "ha_prototypes.h" /* innobase_casedn_str()*/
#ifndef UNIV_HOTBACKUP
# include "lock0lock.h"
#endif /* !UNIV_HOTBACKUP */
#ifdef UNIV_BLOB_DEBUG
# include "ut0rbt.h"
#endif /* UNIV_BLOB_DEBUG */

#define	DICT_HEAP_SIZE		100	/*!< initial memory heap size when
					creating a table or index object */

#ifdef UNIV_PFS_MUTEX
/* Key to register autoinc_mutex with performance schema */
UNIV_INTERN mysql_pfs_key_t	autoinc_mutex_key;
#endif /* UNIV_PFS_MUTEX */

/**********************************************************************//**
Creates a table memory object.
@return	own: table object */
UNIV_INTERN
dict_table_t*
dict_mem_table_create(
/*==================*/
	const char*	name,	/*!< in: table name */
	ulint		space,	/*!< in: space where the clustered index of
				the table is placed; this parameter is
				ignored if the table is made a member of
				a cluster */
	ulint		n_cols,	/*!< in: number of columns */
	ulint		flags,	/*!< in: table flags */
    ibool       is_gcs, /*!< in: gcs table flag */
    ulint       n_cols_before_alter     /*!< in: number of columns before gcs table alter table */
)
{
	dict_table_t*	table;
	mem_heap_t*	heap;

	ut_ad(name);
	ut_a(!(flags & (~0 << DICT_TF2_BITS)));
    ut_ad(!is_gcs || flags & DICT_TF_COMPACT);              /* GCS必须是compact格式 */

	heap = mem_heap_create(DICT_HEAP_SIZE);

	table = mem_heap_zalloc(heap, sizeof(dict_table_t));

	table->heap = heap;

	table->flags = (unsigned int) flags;
	table->name = ut_malloc(strlen(name) + 1);
	memcpy(table->name, name, strlen(name) + 1);
	table->space = (unsigned int) space;
	table->n_cols = (unsigned int) (n_cols + DATA_N_SYS_COLS);

	table->cols = mem_heap_alloc(heap, (n_cols + DATA_N_SYS_COLS)
				     * sizeof(dict_col_t));

#ifndef UNIV_HOTBACKUP
	table->autoinc_lock = mem_heap_alloc(heap, lock_get_size());

	mutex_create(autoinc_mutex_key,
		     &table->autoinc_mutex, SYNC_DICT_AUTOINC_MUTEX);

	table->autoinc = 0;
    table->is_gcs  = is_gcs;
    ut_ad(!n_cols_before_alter || is_gcs);      /* 仅对gcs表有效，其他表必为0 */
    if (n_cols_before_alter > 0)
        table->n_cols_before_alter_table = (ulint)(n_cols_before_alter +  DATA_N_SYS_COLS);
    else
        table->n_cols_before_alter_table = 0;

	/* The number of transactions that are either waiting on the
	AUTOINC lock or have been granted the lock. */
	table->n_waiting_or_granted_auto_inc_locks = 0;
#endif /* !UNIV_HOTBACKUP */

	ut_d(table->magic_n = DICT_TABLE_MAGIC_N);
	return(table);
}

/****************************************************************//**
Free a table memory object. */
UNIV_INTERN
void
dict_mem_table_free(
/*================*/
	dict_table_t*	table)		/*!< in: table */
{
	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_d(table->cached = FALSE);

#ifndef UNIV_HOTBACKUP
	mutex_free(&(table->autoinc_mutex));
#endif /* UNIV_HOTBACKUP */
	ut_free(table->name);
	mem_heap_free(table->heap);
}

/****************************************************************//**
Append 'name' to 'col_names'.  @see dict_table_t::col_names
@return	new column names array */
static
const char*
dict_add_col_name(
/*==============*/
	const char*	col_names,	/*!< in: existing column names, or
					NULL */
	ulint		cols,		/*!< in: number of existing columns */
	const char*	name,		/*!< in: new column name */
	mem_heap_t*	heap)		/*!< in: heap */
{
	ulint	old_len;
	ulint	new_len;
	ulint	total_len;
	char*	res;

	ut_ad(!cols == !col_names);

	/* Find out length of existing array. */
	if (col_names) {
		const char*	s = col_names;
		ulint		i;

		for (i = 0; i < cols; i++) {
			s += strlen(s) + 1;
		}

		old_len = s - col_names;
	} else {
		old_len = 0;
	}

	new_len = strlen(name) + 1;
	total_len = old_len + new_len;

	res = mem_heap_alloc(heap, total_len);

	if (old_len > 0) {
		memcpy(res, col_names, old_len);
	}

	memcpy(res + old_len, name, new_len);

	return(res);
}

/**********************************************************************//**
Adds a column definition to a table. */
UNIV_INTERN
void
dict_mem_table_add_col(
/*===================*/
	dict_table_t*	table,	/*!< in: table */
	mem_heap_t*	heap,	/*!< in: temporary memory heap, or NULL */
	const char*	name,	/*!< in: column name, or NULL */
	ulint		mtype,	/*!< in: main datatype */
	ulint		prtype,	/*!< in: precise type */
	ulint		len)	/*!< in: precision */
{
	dict_col_t*	col;
	ulint		i;

	ut_ad(table);
	ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);
	ut_ad(!heap == !name);

	i = table->n_def++;

	if (name) {
		if (UNIV_UNLIKELY(table->n_def == table->n_cols)) {
			heap = table->heap;
		}
		if (UNIV_LIKELY(i) && UNIV_UNLIKELY(!table->col_names)) {
			/* All preceding column names are empty. */
			char* s = mem_heap_zalloc(heap, table->n_def);
			table->col_names = s;
		}

		table->col_names = dict_add_col_name(table->col_names,
						     i, name, heap);
	}

	col = dict_table_get_nth_col(table, i);

	dict_mem_fill_column_struct(col, i, mtype, prtype, len);
}

UNIV_INTERN
void
dict_mem_table_add_col_default(
    dict_table_t*           table,
    dict_col_t*             col,
    mem_heap_t*             heap,
    const char*             def_val,
    ulint                   def_val_len
)
{
    ut_ad(table && col && heap && def_val && def_val_len > 0);
    ut_ad(!dict_col_is_nullable(col));

    col->def_val = mem_heap_alloc(heap, sizeof(*col->def_val));
    col->def_val->col = col;
    col->def_val->def_val_len = def_val_len;
    col->def_val->def_val = mem_heap_strdupl(heap, def_val, def_val_len);

    switch (col->mtype)
    {
    case DATA_VARCHAR:
    case DATA_CHAR:
    case DATA_BINARY:
    case DATA_FIXBINARY:
    case DATA_BLOB:
    case DATA_MYSQL:
    case DATA_DECIMAL:
    case DATA_VARMYSQL:
        col->def_val->real_val.var_val = col->def_val->def_val;
        break;

    case DATA_INT:
        ut_ad(def_val_len == 4);
        col->def_val->real_val.int_val = mach_read_from_4(col->def_val->def_val);
        break;

    case DATA_FLOAT:
        ut_ad(def_val_len == sizeof(float));
        col->def_val->real_val.float_val = mach_float_read(col->def_val->def_val);
        break;

    case DATA_DOUBLE:
        ut_ad(def_val_len == sizeof(double));
        col->def_val->real_val.double_val = mach_double_read(col->def_val->def_val);
        break;

    case DATA_SYS_CHILD:	/* address of the child page in node pointer */
    case DATA_SYS:          /* system column */
    default:
        ut_a(0);
        break;
    }

}

/**********************************************************************//**
This function populates a dict_col_t memory structure with
supplied information. */
UNIV_INTERN
void
dict_mem_fill_column_struct(
/*========================*/
	dict_col_t*	column,		/*!< out: column struct to be
					filled */
	ulint		col_pos,	/*!< in: column position */
	ulint		mtype,		/*!< in: main data type */
	ulint		prtype,		/*!< in: precise type */
	ulint		col_len)	/*!< in: column length */
{
#ifndef UNIV_HOTBACKUP
	ulint	mbminlen;
	ulint	mbmaxlen;
#endif /* !UNIV_HOTBACKUP */

	column->ind = (unsigned int) col_pos;
	column->ord_part = 0;
	column->max_prefix = 0;
	column->mtype = (unsigned int) mtype;
	column->prtype = (unsigned int) prtype;
	column->len = (unsigned int) col_len;
    column->def_val = NULL;
#ifndef UNIV_HOTBACKUP
        dtype_get_mblen(mtype, prtype, &mbminlen, &mbmaxlen);
	dict_col_set_mbminmaxlen(column, mbminlen, mbmaxlen);
#endif /* !UNIV_HOTBACKUP */
}

/**********************************************************************//**
Creates an index memory object.
@return	own: index object */
UNIV_INTERN
dict_index_t*
dict_mem_index_create(
/*==================*/
	const char*	table_name,	/*!< in: table name */
	const char*	index_name,	/*!< in: index name */
	ulint		space,		/*!< in: space where the index tree is
					placed, ignored if the index is of
					the clustered type */
	ulint		type,		/*!< in: DICT_UNIQUE,
					DICT_CLUSTERED, ... ORed */
	ulint		n_fields)	/*!< in: number of fields */
{
	dict_index_t*	index;
	mem_heap_t*	heap;

	ut_ad(table_name && index_name);

	heap = mem_heap_create(DICT_HEAP_SIZE);
	index = mem_heap_zalloc(heap, sizeof(dict_index_t));

	dict_mem_fill_index_struct(index, heap, table_name, index_name,
				   space, type, n_fields);

	return(index);
}

/**********************************************************************//**
Creates and initializes a foreign constraint memory object.
@return	own: foreign constraint struct */
UNIV_INTERN
dict_foreign_t*
dict_mem_foreign_create(void)
/*=========================*/
{
	dict_foreign_t*	foreign;
	mem_heap_t*	heap;

	heap = mem_heap_create(100);

	foreign = mem_heap_zalloc(heap, sizeof(dict_foreign_t));

	foreign->heap = heap;

	return(foreign);
}

/**********************************************************************//**
Sets the foreign_table_name_lookup pointer based on the value of
lower_case_table_names.  If that is 0 or 1, foreign_table_name_lookup
will point to foreign_table_name.  If 2, then another string is
allocated from foreign->heap and set to lower case. */
UNIV_INTERN
void
dict_mem_foreign_table_name_lookup_set(
/*===================================*/
	dict_foreign_t*	foreign,	/*!< in/out: foreign struct */
	ibool		do_alloc)	/*!< in: is an alloc needed */
{
	if (innobase_get_lower_case_table_names() == 2) {
		if (do_alloc) {
			foreign->foreign_table_name_lookup = mem_heap_alloc(
				foreign->heap,
				strlen(foreign->foreign_table_name) + 1);
		}
		strcpy(foreign->foreign_table_name_lookup,
		       foreign->foreign_table_name);
		innobase_casedn_str(foreign->foreign_table_name_lookup);
	} else {
		foreign->foreign_table_name_lookup
			= foreign->foreign_table_name;
	}
}

/**********************************************************************//**
Sets the referenced_table_name_lookup pointer based on the value of
lower_case_table_names.  If that is 0 or 1, referenced_table_name_lookup
will point to referenced_table_name.  If 2, then another string is
allocated from foreign->heap and set to lower case. */
UNIV_INTERN
void
dict_mem_referenced_table_name_lookup_set(
/*======================================*/
	dict_foreign_t*	foreign,	/*!< in/out: foreign struct */
	ibool		do_alloc)	/*!< in: is an alloc needed */
{
	if (innobase_get_lower_case_table_names() == 2) {
		if (do_alloc) {
			foreign->referenced_table_name_lookup = mem_heap_alloc(
				foreign->heap,
				strlen(foreign->referenced_table_name) + 1);
		}
		strcpy(foreign->referenced_table_name_lookup,
		       foreign->referenced_table_name);
		innobase_casedn_str(foreign->referenced_table_name_lookup);
	} else {
		foreign->referenced_table_name_lookup
			= foreign->referenced_table_name;
	}
}

/**********************************************************************//**
Adds a field definition to an index. NOTE: does not take a copy
of the column name if the field is a column. The memory occupied
by the column name may be released only after publishing the index. */
UNIV_INTERN
void
dict_mem_index_add_field(
/*=====================*/
	dict_index_t*	index,		/*!< in: index */
	const char*	name,		/*!< in: column name */
	ulint		prefix_len)	/*!< in: 0 or the column prefix length
					in a MySQL index like
					INDEX (textcol(25)) */
{
	dict_field_t*	field;

	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);

	index->n_def++;

	field = dict_index_get_nth_field(index, index->n_def - 1);

	field->name = name;
	field->prefix_len = (unsigned int) prefix_len;
}

/**********************************************************************//**
Frees an index memory object. */
UNIV_INTERN
void
dict_mem_index_free(
/*================*/
	dict_index_t*	index)	/*!< in: index */
{
	ut_ad(index);
	ut_ad(index->magic_n == DICT_INDEX_MAGIC_N);
#ifdef UNIV_BLOB_DEBUG
	if (index->blobs) {
		mutex_free(&index->blobs_mutex);
		rbt_free(index->blobs);
	}
#endif /* UNIV_BLOB_DEBUG */

	mem_heap_free(index->heap);
}


/**********************************************************************//**
直接对字典对象内存增加若干列 
*/
void
dict_mem_table_add_col_simple(
    dict_table_t*           table,              /*!< in: 原表字典对象 */
    dict_col_t*             col_arr,            /*!< in: 增加列后的用户列字典对象 */
    ulint                   n_col,              /*!< in: col_arr的个数 */
    char*                   col_names,          /*!< in: 用户列所有列名 */
    ulint                   col_names_len       /*!< in: col_names的长度 */
)
{
    ulint                   org_heap_size = 0;
    dict_col_t*             org_cols;
    ulint                   org_n_cols = table->n_def - DATA_N_SYS_COLS;
    ulint                   add_n_cols = n_col - org_n_cols;
    dict_index_t*           index = NULL;
    dict_index_t*           clu_index = NULL;
    char*                   new_col_names;
    dict_field_t*           fields;
    ulint                   i;
	ibool					first_alter = FALSE;
    
    ut_ad(dict_table_is_gcs(table) && table->cached);
    ut_ad(table->n_def < n_col + DATA_N_SYS_COLS && table->n_def  == table->n_cols);

    org_heap_size = mem_heap_get_size(table->heap);

    org_cols = table->cols;

    if (table->n_cols_before_alter_table == 0)
    {
		first_alter = TRUE;
        /* 若第一次alter table add column，修改该值 */
        table->n_cols_before_alter_table = table->n_cols;
    }

    /* 拷贝前N列（用户列）及列名 */
    table->cols = mem_heap_zalloc(table->heap, sizeof(dict_col_t) * (DATA_N_SYS_COLS + n_col));
    memcpy(table->cols, col_arr, n_col * sizeof(dict_col_t));

    /* 还原默认值信息，使用table->heap分配内存 */
    for (i = table->n_cols_before_alter_table - DATA_N_SYS_COLS; i < n_col; ++i)
    {
        if (table->cols[i].def_val)
        {
            dict_mem_table_add_col_default(table, &table->cols[i], table->heap, table->cols[i].def_val->def_val, table->cols[i].def_val->def_val_len);
        }
    }

    table->n_def = n_col;
    table->n_cols = n_col + DATA_N_SYS_COLS;

    new_col_names = mem_heap_zalloc(table->heap, col_names_len);
    memcpy(new_col_names, col_names, col_names_len);
    table->col_names = new_col_names;

    /* 增加系统列 */
    table->cached = FALSE;          /* 避免断言 */
    dict_table_add_system_columns(table, table->heap);
    table->cached = TRUE;

    dict_table_set_big_row(table);

    dict_sys->size -= org_heap_size;
    dict_sys->size += mem_heap_get_size(table->heap);

    /* 更新索引及索引列信息 */
    index = UT_LIST_GET_FIRST(table->indexes);
    while (index)
    {
        ulint               n_fields;
        dict_field_t*       org_fields;
        ibool               is_gen_clust_index = FALSE;

        ut_ad(index->n_def == index->n_fields);

        org_heap_size = mem_heap_get_size(index->heap);

        org_fields = index->fields;

        if (dict_index_is_clust(index))
        {
            clu_index = index;

            /* 修改主键为rowid的ord_part值 */
            if (!strcmp(clu_index->name, "GEN_CLUST_INDEX"))
            {
                is_gen_clust_index = TRUE;
            }
            

            ut_ad(index->n_fields <= index->n_user_defined_cols + table->n_cols);

            fields = (dict_field_t*) mem_heap_zalloc(
                index->heap, 1 + (index->n_user_defined_cols + table->n_cols) * sizeof(dict_field_t));  /* 分配足够多的空间 */

            memcpy(fields, org_fields, index->n_fields * sizeof(dict_field_t));

            index->fields = fields;     /* dict_index_add_col必须保证已经赋值 */

            /* 聚集索引需要增加最后几列的信息 */
            for (i = 0; i < add_n_cols; ++i)
            {
                dict_index_add_col(index, table, dict_table_get_nth_col(table, org_n_cols + i), 0);  /* n_nullable已在dict_index_add_col处理 */
            }

            /* 聚集索引只处理前n_field列，因新增的若干列已经在上面处理了 */
            n_fields = index->n_fields;
            index->n_fields     += add_n_cols;

            ut_ad(index->n_fields == index->n_def);

            if (index->n_fields_before_alter == 0)
            {
				ut_ad(first_alter);
                index->n_fields_before_alter = index->n_fields - (table->n_cols - table->n_cols_before_alter_table);
                index->n_nullable_before_alter = dict_index_get_first_n_field_n_nullable(index, index->n_fields_before_alter);
            }
        }
        else
        {
            ut_ad(index->n_user_defined_cols + clu_index->n_uniq + 1 >= index->n_fields);
            ut_ad(clu_index != NULL);

            fields = (dict_field_t*) mem_heap_zalloc(
                index->heap, 1 + (index->n_user_defined_cols + clu_index->n_uniq + 1) * sizeof(dict_field_t));  /* 分配足够多的空间 */

            memcpy(fields, org_fields, index->n_fields * sizeof(dict_field_t));

            n_fields = index->n_fields;

            index->fields = fields;
        }
        
        /* 修改索引列col和name指针地址 */
        for (i = 0; i < n_fields; ++i)
        {
            ulint       col_ind = org_fields[i].col->ind;

            /* 因为表中org_n_cols后插入了若干列，原列索引大于等于org_n_cols(系统列)都需要增大增加的列数 */
            if (col_ind >= org_n_cols)
                col_ind += add_n_cols;

            fields[i].col = dict_table_get_nth_col(table, col_ind);
            fields[i].name = dict_table_get_col_name(table, col_ind);

            if (is_gen_clust_index && !strcmp(fields[i].name, "DB_ROW_ID"))
            {
                ut_ad(!fields[i].col->ord_part);
                fields[i].col->ord_part = 1;
            }
            
        }

        dict_sys->size -= org_heap_size;
        dict_sys->size += mem_heap_get_size(index->heap);

        index = UT_LIST_GET_NEXT(indexes, index);
    }

    /* 外键 */
}

