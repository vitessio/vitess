/* Copyright (c) 2023, The Vitess Authors

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   Without limiting anything contained in the foregoing, this file,
   which is part of C Driver for MySQL (Connector/C), is also subject to the
   Universal FOSS Exception, version 1.0, a copy of which can be found at
   http://oss.oracle.com/licenses/universal-foss-exception.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "m_ctype.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "my_sys.h"
#include "my_config.h"
#include "my_compiler.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_loglevel.h"
#include "my_macros.h"
#include "str_uca_type.h"

#include "rapidjson/rapidjson.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/writer.h"

template <typename J>
static void print_contractions_1(J &json, my_wc_t *path, size_t depth, bool contextual, const MY_CONTRACTION &contraction)
{
    path[depth] = contraction.ch;

    if (contraction.is_contraction_tail)
    {
        json.StartObject();

        json.Key("Path");
        json.StartArray();
        for (size_t i = 0; i <= depth; i++)
        {
            json.Uint((unsigned int)path[i]);
        }
        json.EndArray();

        json.Key("Weights");
        json.StartArray();
        for (size_t i = 0; i < MY_UCA_MAX_WEIGHT_SIZE; i++)
        {
            json.Uint(contraction.weight[i]);
        }
        json.EndArray();

        if (contextual)
        {
            json.Key("Contextual");
            json.Bool(true);
        }

        json.EndObject();
    }

    for (const MY_CONTRACTION &ctr : contraction.child_nodes)
    {
        print_contractions_1(json, path, depth + 1, false, ctr);
    }
    for (const MY_CONTRACTION &ctr : contraction.child_nodes_context)
    {
        print_contractions_1(json, path, depth + 1, true, ctr);
    }
}

template <typename J>
static void print_contractions(J &json, std::vector<MY_CONTRACTION> *contractions)
{
    my_wc_t path[256];
    json.StartArray();
    for (const MY_CONTRACTION &ctr : *contractions)
    {
        print_contractions_1(json, path, 0, false, ctr);
    }
    json.EndArray();
}

template <typename J>
static void print_reorder_params(J &json, struct Reorder_param *reorder)
{
    json.StartArray();
    for (int i = 0; i < reorder->wt_rec_num; i++)
    {
        struct Reorder_wt_rec &r = reorder->wt_rec[i];
        json.StartArray();
        json.Uint(r.old_wt_bdy.begin);
        json.Uint(r.old_wt_bdy.end);
        json.Uint(r.new_wt_bdy.begin);
        json.Uint(r.new_wt_bdy.end);
        json.EndArray();
    }
    json.EndArray();
}

template <typename J>
static void print_unipages(J &json, const MY_UNI_IDX *unicodeidx)
{
    json.StartArray();
    for (const MY_UNI_IDX *idx = unicodeidx; idx->tab != NULL; idx++)
    {
        json.StartObject();
        json.Key("From");
        json.Uint(idx->from);
        json.Key("To");
        json.Uint(idx->to);
        json.Key("Tab");
        json.StartArray();
        const size_t entries = idx->to - idx->from;
        for (size_t i = 0; i <= entries; i++)
        {
            json.Uint(idx->tab[i]);
        }
        json.EndArray();
        json.EndObject();
    }
    json.EndArray();
}

template <typename J>
static void print_uca_weights_900(J &json, int codepoint, uint16 **weights)
{
    uint16 *page = weights[codepoint >> 8];
    if (page == NULL)
        return;

    int offset = codepoint & 0xFF;
    int cecount = page[offset];
    char key[32];
    snprintf(key, sizeof(key), "U+%04X", codepoint);

    json.Key(key);
    json.StartArray();
    for (int ce = 0; ce < cecount; ce++)
    {
        json.Uint(page[256 + (ce * 3 + 0) * 256 + offset]);
        json.Uint(page[256 + (ce * 3 + 1) * 256 + offset]);
        json.Uint(page[256 + (ce * 3 + 2) * 256 + offset]);
    }
    json.EndArray();
}

template <typename J>
static void print_uca_weights_legacy(J &json, int codepoint, uint16 **weights, uchar *lengths)
{
    uint16 *page = weights[codepoint >> 8];
    if (page == NULL)
        return;

    int offset = codepoint & 0xFF;
    uint16 *w = page + offset * lengths[codepoint >> 8];
    if (!w[0])
        return;

    char key[32];
    snprintf(key, sizeof(key), "U+%04X", codepoint);

    json.Key(key);
    json.StartArray();
    for (; w[0]; w++)
    {
        json.Uint(w[0]);
    }
    json.EndArray();
}

template <typename J>
static void print_array_uchar(J &json, const uchar *arr, size_t len)
{
    json.StartArray();
    for (size_t i = 0; i < len; ++i)
    {
        json.Uint(arr[i]);
    }
    json.EndArray();
}

template <typename J>
static void print_array_uint16(J &json, const uint16 *arr, size_t len)
{
    json.StartArray();
    for (size_t i = 0; i < len; ++i)
    {
        json.Uint(arr[i]);
    }
    json.EndArray();
}

static CHARSET_INFO *init_collation(const char *name)
{
    MY_CHARSET_LOADER loader;
    return my_collation_get_by_name(&loader, name, MYF(0));
}

#define MY_UCA_MAXCHAR (0x10FFFF + 1)
#define MY_UCA_CHARS_PER_PAGE 256

extern MY_COLLATION_HANDLER my_collation_uca_900_handler;
extern MY_COLLATION_HANDLER my_collation_any_uca_handler;
extern MY_COLLATION_HANDLER my_collation_utf16_uca_handler;
extern MY_COLLATION_HANDLER my_collation_utf32_uca_handler;
extern MY_COLLATION_HANDLER my_collation_ucs2_uca_handler;

struct KNOWN_HANDLER
{
    const char *name;
    const MY_COLLATION_HANDLER *h;
};

static KNOWN_HANDLER known_handlers[] = {
    {"8bit_bin", &my_collation_8bit_bin_handler},
    {"8bit_simple_ci", &my_collation_8bit_simple_ci_handler},
    {"any_uca", &my_collation_any_uca_handler},
    {"uca_900", &my_collation_uca_900_handler},
    {"utf16_uca", &my_collation_utf16_uca_handler},
    {"utf32_uca", &my_collation_utf32_uca_handler},
    {"ucs2_uca", &my_collation_ucs2_uca_handler},
};

static int dumpall(const char *dumppath)
{
    char pathbuf[4096];
    char jsonbuf[4096 * 4];

    // bootstrap the `all_charsets` collation array
    init_collation("utf8mb4_0900_ai_ci");

    for (const CHARSET_INFO *charset : all_charsets)
    {
        if (!charset || (charset->state & MY_CS_AVAILABLE) == 0)
            continue;

        charset = init_collation(charset->m_coll_name);
        snprintf(pathbuf, sizeof(pathbuf), "%s/%s.json", dumppath, charset->m_coll_name);

        FILE *jsonfile = fopen(pathbuf, "w");
        if (jsonfile == NULL)
        {
            fprintf(stderr, "failed to create '%s'\n", pathbuf);
            return 1;
        }

        rapidjson::FileWriteStream os(jsonfile, jsonbuf, sizeof(jsonbuf));
        rapidjson::Writer<rapidjson::FileWriteStream, rapidjson::UTF8<>, rapidjson::ASCII<>> json(os);

        json.StartObject();
        json.Key("Name");
        json.String(charset->m_coll_name);
        json.Key("Charset");
        json.String(charset->csname);
        json.Key("Number");
        json.Uint(charset->number);

        json.Key("Flags");
        json.StartObject();

        json.Key("Binary");
        json.Bool((charset->state & MY_CS_BINSORT) != 0);
        json.Key("ASCII");
        json.Bool((charset->state & MY_CS_PUREASCII) != 0);
        json.Key("Default");
        json.Bool((charset->state & MY_CS_PRIMARY) != 0);

        json.EndObject();

        for (const KNOWN_HANDLER &handler : known_handlers)
        {
            if (charset->coll == handler.h)
            {
                json.Key("CollationImpl");
                json.String(handler.name);
                break;
            }
        }

        if (charset->ctype != NULL)
        {
            json.Key("CType");
            print_array_uchar(json, charset->ctype, 256);
        }

        if (charset->to_lower != NULL)
        {
            json.Key("ToLower");
            print_array_uchar(json, charset->to_lower, 256);
        }

        if (charset->to_upper != NULL)
        {
            json.Key("ToUpper");
            print_array_uchar(json, charset->to_upper, 256);
        }

        if (charset->tab_to_uni != NULL)
        {
            json.Key("TabToUni");
            print_array_uint16(json, charset->tab_to_uni, 256);
        }

        if (charset->tab_from_uni != NULL)
        {
            json.Key("TabFromUni");
            print_unipages(json, charset->tab_from_uni);
        }

        if (charset->sort_order != NULL)
        {
            json.Key("SortOrder");
            print_array_uchar(json, charset->sort_order, 256);
        }

        if (charset->uca != NULL)
        {
            MY_UCA_INFO *uca = charset->uca;

            json.Key("UCAVersion");

            switch (uca->version)
            {
            case UCA_V400:
                json.Uint(400);
                break;
            case UCA_V520:
                json.Uint(520);
                break;
            case UCA_V900:
                json.Uint(900);
                break;
            default:
                json.Uint(0);
                break;
            }

            json.Key("Weights");
            json.StartObject();
            if (uca->version == UCA_V900)
            {
                for (my_wc_t cp = 0; cp < MY_UCA_MAXCHAR; cp++)
                {
                    print_uca_weights_900(json, cp, uca->weights);
                }
            }
            else
            {
                for (my_wc_t cp = 0; cp < uca->maxchar; cp++)
                {
                    print_uca_weights_legacy(json, cp, uca->weights, uca->lengths);
                }
            }
            json.EndObject();

            if (uca->have_contractions)
            {
                json.Key("Contractions");
                print_contractions(json, uca->contraction_nodes);
            }
        }

        if (charset->coll_param != NULL)
        {
            json.Key("UppercaseFirst");
            json.Bool(charset->coll_param->case_first == CASE_FIRST_UPPER);

            if (charset->coll_param->reorder_param != NULL)
            {
                json.Key("Reorder");
                print_reorder_params(json, charset->coll_param->reorder_param);
            }
        }

        json.EndObject();
        os.Flush();
        fclose(jsonfile);
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        fprintf(stderr, "usage: %s <destination_path>\n", argv[0]);
        return 1;
    }

    return dumpall(argv[1]);
}